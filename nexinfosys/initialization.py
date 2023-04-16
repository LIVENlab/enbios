import io
import json
import logging
import sys

import pandas as pd
import sqlalchemy
from sqlalchemy.pool import StaticPool

import nexinfosys
from nexinfosys.command_definitions import commands
from nexinfosys.command_descriptions import c_descriptions
from nexinfosys.command_field_definitions import command_fields
from nexinfosys.command_field_descriptions import cf_descriptions
from nexinfosys.command_generators.parser_field_examples import generic_field_examples, generic_field_syntax
from nexinfosys.command_generators.parser_field_parsers import string_to_ast, arith_boolean_expression
from nexinfosys.common.create_database import create_pg_database_engine, create_monet_database_engine
from nexinfosys.common.helper import strcmp, add_label_columns_to_dataframe, generate_json
from nexinfosys.ie_exports.back_to_nis_format import nis_format_spreadsheet
from nexinfosys.ie_exports.flows_graph import construct_flow_graph_2
from nexinfosys.ie_exports.geolayer import generate_geojson
from nexinfosys.ie_exports.json_export import export_model_to_json
from nexinfosys.ie_exports.processors_graph import construct_processors_graph_2
from nexinfosys.ie_exports.sdmx import get_dataset_metadata
from nexinfosys.ie_exports.xml_export import export_model_to_xml
from nexinfosys.ie_imports.data_sources.eurostat_bulk import Eurostat
from nexinfosys.ie_imports.data_sources.eurostats_comext import COMEXT
from nexinfosys.ie_imports.data_sources.fadn import FADN
from nexinfosys.ie_imports.data_sources.faostat import FAOSTAT
from nexinfosys.ie_imports.data_sources.oecd import OECD
from nexinfosys.model_services import State, get_case_study_registry_objects
from nexinfosys.models.musiasem_concepts import Parameter, Hierarchy, ProblemStatement
from nexinfosys.models.musiasem_methodology_support import load_table, DBSession, User, Authenticator, CaseStudyStatus, \
    ObjectType, PermissionType, ORMBase
from nexinfosys.solving import BasicQuery

tm_permissions = {  # PermissionType
    "f19ad19f-0a74-44e8-bd4e-4762404a35aa": "read",
    "04cac7ca-a90b-4d12-a966-d8b0c77fca70": "annotate",
    "d0924822-32fa-4456-8143-0fd48da33fd7": "contribute",
    "83d837ab-01b2-4260-821b-8c4a3c52e9ab": "share",
    "d3137471-84a0-4bcf-8dd8-16387ea46a30": "delete"
}

tm_default_users = {  # Users
    "f3848599-4aa3-4964-b7e1-415d478560be": "admin",
    "2a512320-cef7-41c6-a141-8380d900761b": "_anonymous"
}

tm_object_types = {  # ObjectType
    "d6649a54-3538-4ee6-a4fc-8a67b74ed21f": "processor",
    "7eca7cfb-3eea-475f-9950-2d1093099ccb": "flow/fund",
    "3cc2582b-5142-4c8f-b484-a4693ba267cf": "flow",
    "c18ca5ab-471d-4ab4-9c0d-9563078a1c18": "fund",
    "5c9ef31b-399f-4007-824f-f2251e29bfdc": "ff_in_processor",
    "6e187787-adf2-4bdf-a0af-ab2801a6be42": "hierarchy",
    "91a7e3c2-115d-4166-a893-db6a19224154": "pedigree_matrix",
    "659f95f3-bc48-47e5-8584-64bd4477f3f2": "case_study"
}

tm_authenticators = {  # Authenticator
    "b33193c3-63b9-49f7-b888-ceba619d2812": "google",
    "c09fa36b-62a3-4904-9600-e3bb5028d809": "facebook",
    "f510cb30-7a44-4cb1-86f5-1b112e43293a": "firebase",
    "5f32a593-306f-4b69-983c-0a5680556fae": "local",
}

tm_case_study_version_statuses = {  # CaseStudyStatus
    "ee436cb7-0237-4b40-bd77-25acfade0f9b": "in_elaboration",
    "eef5c756-56ac-47a0-96cf-b7bebd73d392": "finished",
    "7d2cd2ae-0f4e-4962-b73c-4e40c553f533": "finished_and_published",
    "e5f3d8e6-accf-4175-9e74-5ad1b9a6faf5": "stopped"
}
# "22bc2577-883a-4408-bba9-fcace20c0fc8":
# "e80a7d27-3ec8-4aa1-b49c-5498e0f85bee":
# "d30120f0-28df-4bca-90e4-4f0676d1c874":
# "83084df6-7ad0-45d7-b3f1-6de594c78611":
# "7e23991b-24a0-4da1-8251-c3c3434dfb87":
# "bfc0c9fe-631f-44d0-8e96-e22d01ffb1ed":
# "dec7e901-b3f4-4343-b3d1-4fa5fbf3804e":
# "013b2f3b-4b2f-4b6c-8f5f-425132aea74b":
# "3eef41be-fde1-4ad4-92d0-fe795158b41d":
# "0fba3591-4ffc-4a88-977a-6e1d922f0735":
# "a61fc587-1272-4d46-bdd0-027cde1b8a78":
# "600397ef-0102-486e-a6f7-d43b0f8ce4b9":
# "763e57b7-2636-4c04-9861-d865fe0bb5ab":
# "788065a7-d9f5-46fa-b8ba-8bc223d09331":
# "38fb34f7-a952-4036-9b0b-4d6c59e8f8d4":
# "0292821a-dd33-450a-bdd8-813b2b95c456":


def initialize_database_data():
    # Load base tables
    load_table(DBSession, User, tm_default_users)
    load_table(DBSession, Authenticator, tm_authenticators)
    load_table(DBSession, CaseStudyStatus, tm_case_study_version_statuses)
    load_table(DBSession, ObjectType, tm_object_types)
    load_table(DBSession, PermissionType, tm_permissions)
    # Create and insert a user
    session = DBSession()
    # Create test User, if it does not exist
    u = session.query(User).filter(User.name == 'test_user').first()
    if not u:
        u = User()
        u.name = "test_user"
        u.uuid = "27c6a285-dd80-44d3-9493-3e390092d301"
        session.add(u)
        session.commit()
    DBSession.remove()


def initialize_databases():
    recreate_db = False
    if nexinfosys.get_global_configuration_variable("DB_CONNECTION_STRING"):
        db_connection_string = nexinfosys.get_global_configuration_variable("DB_CONNECTION_STRING")
        echo_sql = nexinfosys.get_global_configuration_variable("SHOW_SQL", False)
        if isinstance(echo_sql, str):
            echo_sql = True if echo_sql.lower() == "true" else False
        logging.debug("Connecting to metadata server")
        logging.debug(db_connection_string)
        logging.debug("-----------------------------")
        if db_connection_string.startswith("sqlite://"):
            nexinfosys.engine = sqlalchemy.create_engine(db_connection_string,
                                                         echo=echo_sql,
                                                         connect_args={'check_same_thread': False},
                                                         poolclass=StaticPool)
        else:
            nexinfosys.engine = create_pg_database_engine(db_connection_string, "magic_nis", recreate_db=recreate_db)

        # global DBSession # global DBSession registry to get the scoped_session
        DBSession.configure(bind=nexinfosys.engine)  # reconfigure the sessionmaker used by this scoped_session
        tables = ORMBase.metadata.tables
        connection = nexinfosys.engine.connect()
        table_existence = [nexinfosys.engine.dialect.has_table(connection, tables[t].name) for t in tables]
        connection.close()
        if False in table_existence:
            ORMBase.metadata.bind = nexinfosys.engine
            ORMBase.metadata.create_all()
        # connection = nexinfosys.engine.connect()
        # table_existence = [nexinfosys.engine.dialect.has_table(connection, tables[t].name) for t in tables]
        # connection.close()
        # Load base tables
        initialize_database_data()
    else:
        logging.error("No database connection defined (DB_CONNECTION_STRING), exiting now!")
        sys.exit(1)

    if nexinfosys.get_global_configuration_variable("DATA_CONNECTION_STRING"):
        data_connection_string = nexinfosys.get_global_configuration_variable("DATA_CONNECTION_STRING")
        logging.debug("Connecting to data server")
        if data_connection_string.startswith("monetdb"):
            # TODO Install monet packages (see commented packages in "requirements.txt")
            nexinfosys.data_engine = create_monet_database_engine(data_connection_string, "magic_data")
        elif data_connection_string.startswith("sqlite://"):
            nexinfosys.data_engine = sqlalchemy.create_engine(data_connection_string,
                                                              echo=echo_sql,
                                                              connect_args={'check_same_thread': False},
                                                              poolclass=StaticPool)
        else:
            nexinfosys.data_engine = create_pg_database_engine(data_connection_string, "magic_data", recreate_db=recreate_db)
    else:
        logging.error("No data connection defined (DATA_CONNECTION_STRING), exiting now!")
        sys.exit(1)


def get_parameters_in_state(state: State):
    res = []
    registry, _, _, _, _ = get_case_study_registry_objects(state)
    for p in registry.get(Parameter.partial_key()):
        p_name = p.name
        p_type = p.type
        if p.range:
            if strcmp(p_type, "Number"):
                p_range = p.range
            else:
                glb_idx, _, _, _, _ = get_case_study_registry_objects(state)
                h = glb_idx.get(Hierarchy.partial_key(p.range))
                h = h[0]
                p_range = ', '.join(h.codes.keys())
        else:
            p_range = ""
        res.append(dict(name=p_name, type=p_type, range=p_range))
    return res


def get_scenarios_in_state(state: State):
    glb_idx, _, _, _, _ = get_case_study_registry_objects(state)

    ps = glb_idx.get(ProblemStatement.partial_key())
    if len(ps) == 0:
        ps = [ProblemStatement()]

    scenarios = []
    for scenario, params in ps[0].scenarios.items():
        scenarios.append(dict(name=scenario, parameters=params.items()))

    return scenarios


def register_external_datasources():
    from nexinfosys.ie_imports.data_source_manager import DataSourceManager
    dsm2 = DataSourceManager(session_factory=DBSession)

    # Eurostat
    try:
        dsm2.register_datasource_manager(Eurostat())
    except:
        print("Eurostat not registered")

    # COMEXT
    try:
        dsm2.register_datasource_manager(COMEXT())
    except:
        print("COMEXT not registered")

    # FAO
    try:
        fao_dir = nexinfosys.get_global_configuration_variable("FAO_DATASETS_DIR", "/home/rnebot/DATOS/FAOSTAT/")
        dsm2.register_datasource_manager(FAOSTAT(datasets_directory=fao_dir,
                                                 metadata_session_factory=DBSession,
                                                 data_engine=nexinfosys.data_engine))
    except:
        print("FAO not registered")

    # OECD
    try:
        dsm2.register_datasource_manager(OECD())
    except:
        print("OECD not registered")

    # FADN
    try:
        dsm2.register_datasource_manager(FADN(metadata_session_factory=DBSession,
                                              data_engine=nexinfosys.data_engine))
    except:
        print("FADN not registered")

    # sources = dsm2.get_supported_sources()
    return dsm2


def get_graph_from_state(state: State, name: str):
    def html_template():
        return """
        <!doctype html>
        <html>
        <head>
          <title>WDS - VisJS Network</title>
          <script type="text/javascript" src="https://unpkg.com/vis-network/standalone/umd/vis-network.min.js"></script>

          <style type="text/css">
            #mynetwork {
              width: 1200px;
              height: 800px;
              border: 1px solid lightgray;
            }
          </style>
        </head>
        <body>

        <p>
          %s
        </p>

        <div id="mynetwork"></div>

        <script type="text/javascript">
          // create an array with nodes
          var nodes = new vis.DataSet(%s);
        // create an array with edges
          var edges = new vis.DataSet(%s);

          // create a network
          var container = document.getElementById('mynetwork');
          var data = {
            nodes: nodes,
            edges: edges
          };
          var options = {};
          var network = new vis.Network(container, data, options);
        </script>

        </body>
        </html>
    """

    if "." in name:
        pos = name.find(".")
        extension = name[pos + 1:]
        name = name[:pos]

    output = None
    mimetype = None
    if name == "interfaces_graph":
        if extension in ("visjs", "html"):
            query = BasicQuery(state)
            output = construct_flow_graph_2(state, query, None, "visjs")  # Version 2 !!!

            if extension == "html":
                output = html_template() % ("", output["nodes"], output["edges"])
                mimetype = "application/html"
            else:
                output = generate_json(output)
                mimetype = "application/json"
        elif extension == "gml":
            # Prepare GML file
            query = BasicQuery(state)
            output = construct_flow_graph_2(state, query, None, extension)  # Version 2 !!!
            mimetype = "application/text"  # TODO
    elif name == "processors_graph":
        if extension in ("visjs", "html"):
            query = BasicQuery(state)
            output = construct_processors_graph_2(state, query, None, True, True, False, "visjs")
            if extension == "html":
                output = html_template() % ("", output["nodes"], output["edges"])
                mimetype = "application/html"
            else:
                output = generate_json(output)
                mimetype = "application/json"
        elif extension == "gml":
            query = BasicQuery(state)
            output = construct_processors_graph_2(state, query, None, True, True, False, extension)
            mimetype = "application/text"

    return output, mimetype, output is not None


def get_dataset_from_state(state: State, name: str, extension: str, labels_enabled: bool):
    glb_idx, p_sets, hh, datasets, mappings = get_case_study_registry_objects(state)
    if "." in name:
        pos = name.find(".")
        extension = name[pos + 1:]
        name = name[:pos]
    if name in datasets:
        # Obtain the data and the metadata
        ds = datasets[name]  # type: Dataset
        ds2 = ds.data
        # Labels
        if labels_enabled:
            logging.debug("Preparing Dataset labels")
            ds2 = add_label_columns_to_dataframe(name, ds2, glb_idx)

        if isinstance(ds2.index, (pd.Int64Index, pd.core.indexes.range.RangeIndex)):
            export_index = False
        else:
            export_index = True

        # TODO Elaborate "meta-workbook" (NIS workbook capable of reproducing the dataset)
        if extension == "json":
            tmp = json.loads(
                '{"data": ' + ds2.to_json(orient='split', date_format='iso', date_unit='s') + ', "metadata": {}}')
            del tmp["data"]["index"]
            return tmp, "text/json", True
        elif extension == "json2":
            tmp = ds2.to_json(orient='records', date_format='iso', date_unit='s')
            return tmp, "text/json", True
        elif extension == "csv":
            tmp = ds2.to_csv(date_format="%Y-%m-%d %H:%M:%S", index=export_index, na_rep="")
            return io.StringIO(tmp), "text/csv", True
        elif extension == "kendopivotgrid":
            # Prepare Data and Schema in Kendo PivotGrid format
            data = json.loads(ds2.to_json(orient='records', date_format='iso', date_unit='s'))
            fields = {}
            dimensions = {}
            measures = {}
            for i, c in enumerate(ds2.columns):
                is_dimension = i < len(ds2.columns) - 1
                fields[c] = dict(type="string" if is_dimension else "number")
                if is_dimension:
                    dimensions[c] = dict(caption="All " + c)
                else:
                    measures["Sum " + c] = dict(field=c, format="{0:c}", aggregate="sum")
                    measures["Avg " + c] = dict(field=c, format="{0:c}", aggregate="average")
            schema = dict(model=dict(fields=fields), cube=dict(dimensions=dimensions, measures=measures))
            return dict(data=data, schema=schema), "text/json", True
        elif extension == "sdmx.json":
            md = get_dataset_metadata(name, ds)
            return md, "text/json", True
        elif extension == "xlsx":
            # Generate XLSX from data & return it
            output = io.BytesIO()
            # from pyexcelerate import Workbook, Writer
            # wb = Workbook()
            # data = [ds2.columns] + [ds2.values]
            # wb.new_sheet(name, data=data)
            # wr = Writer.Writer(wb)
            # wr.save(output)
            logging.debug("Generating Excel")
            ds2.to_excel(output, sheet_name=name, index=False)  # , engine="xlsxwriter")
            return output.getvalue(), "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", True
        else:
            return ds2, "application/pandas.dataframe", True
    else:
        return {"error": f"Could not find a Dataset with name '{name}' in the current state"}, "text/json", False


def get_model(state: State, format: str):
    output = None
    # Generate graph from State
    if state:
        if format == "json":
            output = str(export_model_to_json(state))
            mimetype = "text/json"
        elif format == "xlsx":
            # A reproducible session must be open, signal about it if not
            output = nis_format_spreadsheet(state)
            mimetype = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        elif format == "xml":
            # A reproducible session must be open, signal about it if not
            # Prepare a XML string
            glb_idx, _, _, _, _ = get_case_study_registry_objects(state)
            output, _ = export_model_to_xml(glb_idx)
            mimetype = "text/xml"

    if output:
        return output, mimetype, True
    else:
        return {"error": F"Cannot return model, format '{format}' not recognized"}, "text/json", False


def get_geolayer(state: State, format):
    output = None
    # Generate graph from State
    if state:
        # TODO Obtain a list of Geolocated processors
        #      Obtain attributes attached to the processor: Name, Full name, system, subsystem type, ...
        #      Go to general matrix and obtain values for interfaces, different scenarios, times and observers
        # TODO Extract geometry of processors into a new layer
        # TODO  Elaborate a new layer where each processor
        if format == "geojson":
            # TODO Prepare GeoJSON file
            output = generate_geojson(state)
            output = io.StringIO(generate_json(output))
            mimetype = "application/geo+json"
        elif format == "kmz" or format == "kml":
            # TODO Prepare KML file
            output = io.BytesIO()
            if format == "kmz":
                mimetype = "application/vnd.google-earth.kmz"
            else:
                mimetype = "application/vnd.google-earth.kml+xml"

    if output:
        return output.getvalue(), mimetype, True
    else:
        return {"error": F"Cannot return geolayer, format '{format}' not recognized"}, "text/json", False


def get_ontology(state: State, format):
    # TODO OWLREADY2 installation on the Docker image issues a problem
    # Recover InteractiveSession
    output = None
    # Generate graph from State
    if state:
        if format == "owl":
            # TODO Prepare OWL file
            output = io.StringIO()
            mimetype = "application/rdf+xml"  # TODO

    if output:
        return output.getvalue(), mimetype, True
    else:
        return {"error": F"Cannot return ontology, format '{format}' not recognized"}, "text/json", False


def validate_command(command_content_to_validate):
    """
    The input comes in a JSON field "content":
    {"command": "<command name",
     "fields": {"<field name>": "<value", ...}
    }
    :param command_content_to_validate:
    :return: A dictionary with the same fields of the input dictionary, whose values are the diagnosis, None being
            everything-ok, and a string being a message describing the problem.
    """
    def split_expansion_expressions(f, content):
        # Dataset expansion. Isolate each expression
        pieces = []
        offset = 0
        look_for = "{"
        open_brace = False
        s = None
        while offset < len(content):
            pos = content[offset:].find(look_for)
            if pos >= 0:
                if look_for == "{":
                    if pos > 0:
                        pieces.append((content[offset:offset + pos], False))  # Literal
                    look_for = "}"
                    open_brace = True
                else:
                    if pos > 0:
                        pieces.append((content[offset:offset + pos], True))  # Expansion
                    else:
                        s = f"Invalid syntax in field '{f}' with value: " + content + ". No expression between curly braces."
                        valid = False
                        break
                    look_for = "{"
                    open_brace = False
                offset += pos + 1
            else:  # Character not found
                if open_brace:
                    s = f"Invalid syntax in field '{f}' with value: " + content + ". Curly brace not closed."
                    valid = False
                    break
                else:  # Add the rest
                    pieces.append((content[offset:], False))
                    offset = len(content)

        return pieces, s

    if "command" in command_content_to_validate:
        command = command_content_to_validate["command"]
    else:
        raise Exception("Must specify 'command'")

    if "fields" in command_content_to_validate:
        fields = command_content_to_validate["fields"]
    else:
        raise Exception("Must specify 'fields'")

    alternative_command_names = command_content_to_validate.get("alternative_command_names", {})

    result = {}
    # Find command from the worksheet name ("command")
    match = None
    for cmd in commands:
        for cmd_name in cmd.allowed_names:
            if cmd_name.lower() in command.lower():
                if match:
                    if match[1] < len(cmd_name):
                        match = (cmd.name, len(cmd_name))
                else:
                    match = (cmd.name, len(cmd_name))
    if not match:
        for k, v in alternative_command_names:
            if k.lower() in command.lower():
                for cmd in commands:
                    for cmd_name in cmd.allowed_names:
                        if cmd_name.lower() in v.lower():
                            match = (cmd.name, 0)
                            break
                    if match:
                        break
                if match:
                    break

    # Fields in the command
    status = True
    if match:
        for f in fields:  # Validate field by field
            for f2 in command_fields[match[0]]:  # Find corresponding field in the command
                if f.lower() in [f3.lower() for f3 in f2.allowed_names]:
                    fld = f2
                    break
            else:
                fld = None
            if fld:  # If found, can validate syntax
                # Validate Syntax
                content = fields[f]
                content_msg = content  # Original "content", to show in case of error
                if isinstance(content, (int, float)):
                    content = str(content)

                # Check if it is an expansion expression
                valid = True
                if "{" in content or "}" in content:
                    # Is expansion allowed in this command?
                    expansion_allowed = True
                    if expansion_allowed:
                        pieces, s = split_expansion_expressions(f, content)
                        if s is None:
                            c = ""
                            for p in pieces:
                                if p[1]:  # Expansion expression
                                    try:
                                        string_to_ast(arith_boolean_expression, p[0])
                                        c += "expand"
                                    except:
                                        s = f"Invalid syntax in field '{f}' with value: {content}, expansion expression '{p[0]}' invalid"
                                        result[f] = s
                                        valid = False
                                        break
                                else:
                                    c += p[0]
                            if valid:
                                content = c
                        else:
                            valid = False

                if not valid:
                    result[f] = s
                    status = False
                else:
                    if fld.allowed_values:
                        if content != content_msg:  # It was an expansion expression, cannot check it now, assume it is good
                            result[f] = None
                        else:
                            # Case insensitive comparison
                            if content.lower().strip() in [f.lower().strip() for f in fld.allowed_values]:
                                result[f] = None
                            else:
                                result[f] = "'"+content+"' in field '"+f+"' must be one of: "+", ".join(fld.allowed_values)
                                status = False
                    else:
                        try:
                            string_to_ast(fld.parser, content)
                            result[f] = None
                        except:
                            s = f"Invalid syntax in field '{f}' with value: '{content_msg}'"
                            if fld.examples:
                                s += ". Examples: "+", ".join(fld.examples)
                            result[f] = s
                            status = False

            else:
                result[f] = "Field '"+f+"' not found in command '"+command+"'. Possible field names: "+", ".join([item for f2 in command_fields[command] for item in f2.allowed_names])
                status = False
    else:
        for f in fields:  # Validate field by field
            result[f] = "Command '" + command +"' not found in the list of command names: " +", ".join([n for c in commands for n in c.allowed_names])
        status = False

    return result, status


def command_field_help(command_content_to_validate):
    """
    The input comes in a JSON field "content":
    {"command": "<command name",
     "fields": ["<field name>", "<field_name>"]
    }

    :return: A dictionary with the same fields passed in the input dictionary, whose values are the help divided in
        sections: explanation, allowed_values, formal syntax and examples
    """
    if "command" in command_content_to_validate:
        command = command_content_to_validate["command"]
    else:
        raise Exception("Must specify 'command'")

    if "fields" in command_content_to_validate:
        fields = command_content_to_validate["fields"]
    else:
        raise Exception("Must specify 'fields'")

    alternative_command_names = command_content_to_validate.get("alternative_command_names", {})

    result = {}
    # Find command from the worksheet name ("command")
    match = None
    for cmd in commands:
        for cmd_name in cmd.allowed_names:
            if cmd_name.lower() in command.lower():
                if match:
                    if match[1] < len(cmd_name):
                        match = (cmd.name, len(cmd_name))
                else:
                    match = (cmd.name, len(cmd_name))
    if not match:
        for k, v in alternative_command_names:
            if k.lower() in command.lower():
                for cmd in commands:
                    for cmd_name in cmd.allowed_names:
                        if cmd_name.lower() in v.lower():
                            match = (cmd.name, 0)
                            break
                    if match:
                        break
                if match:
                    break

    # Fields in the command
    status = True
    if match:
        for f in fields:  # Validate field by field
            for f2 in command_fields.get(match[0], []):  # Find corresponding field in the command
                if f.lower() in [f3.lower() for f3 in f2.allowed_names]:
                    fld = f2
                    break
            else:
                fld = None
            if fld:  # If found, can elaborate help
                mandatoriness = "Mandatory" if fld.mandatory else "Optional"
                description = cf_descriptions.get((match[0], fld.name), f"Text for field {f} in command {match[0]}")
                if isinstance(description, list):
                    description = description[0]  # Short version for online. Index 1 contains the long version
                examples = ("<br><b>Examples:</b><br>&nbsp;&nbsp;"+"<br>&nbsp;&nbsp;".join(generic_field_examples[fld.parser])) if generic_field_examples.get(fld.parser) else ""
                syntax = ", ".join(fld.allowed_values) if fld.allowed_values else generic_field_syntax.get(fld.parser, "<>")
                if fld.allowed_values:
                    result[f] = f"<small><b>({mandatoriness})</b></small><br>{description}<br><b>Syntax. One of:</b> {syntax}"
                else:
                    result[f] = f"<small><b>({mandatoriness})</b></small><br>{description}<br><b>Syntax:</b> {syntax}{examples}"
            else:
                if match[0] == "datasetqry" and f:  # Must be a dimension name
                    mandatoriness = "Optional"
                    description = "A dimension name. Use for filtering purposes (leave the values that MUST be at the output)."
                    result[f] = f"<small><b>({mandatoriness})</b></small><br>{description}"
                else:
                    result[f] = "Field '"+f+"' not found in command '"+command+"'. Possible field names: "+", ".join([item for f2 in command_fields.get(match[0], []) for item in f2.allowed_names])
                    status = False
    else:
        for f in fields:  # Validate field by field
            result[f] = "Command '" + command +"' not found in the list of command names: " +", ".join([n for c in commands for n in c.allowed_names])
        status = False

    return result, status


def comm_help(command_content_to_validate):
    if "command" in command_content_to_validate:
        command = command_content_to_validate["command"]
    else:
        raise Exception("Must specify 'command'")

    alternative_command_names = command_content_to_validate.get("alternative_command_names", {})

    # Find command from the worksheet name ("command")
    match = None
    for cmd in commands:
        for cmd_name in cmd.allowed_names:
            if cmd_name.lower() in command.lower():
                if match:
                    if match[1] < len(cmd_name):
                        match = (cmd.name, len(cmd_name))
                else:
                    match = (cmd.name, len(cmd_name))
    if not match:
        for k, v in alternative_command_names:
            if k.lower() in command.lower():
                for cmd in commands:
                    for cmd_name in cmd.allowed_names:
                        if cmd_name.lower() in v.lower():
                            match = (cmd.name, 0)
                            break
                    if match:
                        break
                if match:
                    break

    # Fields in the command
    status = True
    if match:
        if (match[0], "title") in c_descriptions:
            title = c_descriptions[(match[0], "title")]
            description = c_descriptions[(match[0], "description")]
            semantics = c_descriptions[(match[0], "semantics")]
            examples = c_descriptions[(match[0], "examples")]
            result = dict(title=title, description=description, semantics=semantics, examples=examples)
        else:
            result = dict(title=f"Title {command}", description=f"Description {command}", semantics=f"Semantics {command}")
    else:
        status = False
        result = dict()

    return result, status


def prepare_and_reset_database_for_tests(prepare=False, metadata_string="sqlite://", data_string="sqlite://"):
    if prepare:
        nexinfosys.engine = sqlalchemy.create_engine(metadata_string, echo=True)
        nexinfosys.data_engine = sqlalchemy.create_engine(data_string, echo=True)

        # global DBSession # global DBSession registry to get the scoped_session
        DBSession.configure(bind=nexinfosys.engine)  # reconfigure the sessionmaker used by this scoped_session
        tables = ORMBase.metadata.tables
        connection = nexinfosys.engine.connect()
        table_existence = [nexinfosys.engine.dialect.has_table(connection, tables[t].name) for t in tables]
        connection.close()
        if False in table_existence:
            ORMBase.metadata.bind = nexinfosys.engine
            ORMBase.metadata.create_all()

    # Load base tables
    load_table(DBSession, User, tm_default_users)
    load_table(DBSession, Authenticator, tm_authenticators)
    load_table(DBSession, CaseStudyStatus, tm_case_study_version_statuses)
    load_table(DBSession, ObjectType, tm_object_types)
    load_table(DBSession, PermissionType, tm_permissions)
    # Create and insert a user
    session = DBSession()
    # Create test User, if it does not exist
    u = session.query(User).filter(User.name == 'test_user').first()
    if not u:
        u = User()
        u.name = "test_user"
        u.uuid = "27c6a285-dd80-44d3-9493-3e390092d301"
        session.add(u)
        session.commit()
    DBSession.remove()