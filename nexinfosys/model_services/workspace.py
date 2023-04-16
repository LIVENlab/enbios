# -*- coding: utf-8 -*-
"""
* Registry of objects. Add, remove, search
* Support for high level operations: directly create and/or modify objects, calling the specification API. Create connections
"""
import copy
import datetime
import json
import logging
import uuid
from enum import Enum
from typing import List, Union, Dict, NoReturn

import pandas as pd

import nexinfosys
from nexinfosys.command_generators import Issue, IssueLocation, IType
from nexinfosys.command_generators.parsers_factory import commands_container_parser_factory
from nexinfosys.common.helper import create_dictionary, strcmp
from nexinfosys.initialization import prepare_and_reset_database_for_tests
from nexinfosys.model_services import IExecutableCommand, get_case_study_registry_objects
from nexinfosys.model_services import State
from nexinfosys.models.musiasem_concepts import ProblemStatement, FactorsRelationDirectedFlowObservation, Processor, \
    Factor, Parameter, FactorInProcessorType
from nexinfosys.models.musiasem_methodology_support import (User,
                                                            CaseStudy,
                                                            CaseStudyVersion,
                                                            CaseStudyVersionSession,
                                                            CommandsContainer,
                                                            force_load,
                                                            DBSession)
from nexinfosys.serialization import serialize_state, deserialize_state
from nexinfosys.solving import BasicQuery
from nexinfosys.solving.flow_graph_outputs import get_dataset
from nexinfosys.solving.flow_graph_solver import flow_graph_solver, evaluate_parameters_for_scenario

logger = logging.getLogger(__name__)


class Identity:
    pass

# class IdentityCredentials:
#     pass
#
# class NISystem:
#     def __init__(self):
#         self._authentication_service = None
#         self._authorization_service = None
#         self._base_url = None
#         self._configuration_manager = None
#         self._plugin_manager = None
#
#
#     def gather_credentials(self) -> IdentityCredentials:
#         return IdentityCredentials()
#
#     def login(self, ic: IdentityCredentials):
#         # Check credentials using authentication service
#         return Identity()
#
#     def logout(self, id: Identity):
#         pass


class CommandResult:
    pass

# class SessionCreationAction(Enum):  # Used in FlowFund
#     """
#
#         +--------+--------+---------+---------------------------------+
#         | branch | clone  | restart |        Behavior                 |
#         +--------+--------+---------+---------------------------------+
#         | True   | True   | True    | Branch & New ReproducibleSession|
#         | True   | True   | False   | Branch & Clone                  |
#         | True   | False  | True    | Branch & New ReproducibleSession|
#         | True   | False  | False   | Branch & New ReproducibleSession|
#         | False  | True   | True    | New CS (not CS version)         |
#         | False  | True   | False   | New CS & Clone                  |
#         | False  | False  | True    | Restart                         |
#         | False  | False  | False   | Continue                        |
#         +--------+--------+---------+---------------------------------+
#     """
#     BranchAndNewWS = 7
#     BranchAndCloneWS = 6
#     NewCaseStudy = 3
#     NewCaseStudyCopyFrom = 2
#     Restart = 1
#     Continue = 0
# ---------------------------------------------------------------------------------------------------------------------

# #####################################################################################################################
# >>>> PRIMITIVE COMMAND & COMMAND GENERATOR PROCESSING FUNCTIONS <<<<
# #####################################################################################################################


def executable_command_to_commands_container(e_cmd: IExecutableCommand):
    """
    IExecutableCommand -> CommandsContainer

    The resulting CommandsContainer will be always in native (JSON) format, because the specification
    to construct an IExecutableCommand has been translated to this native format.

    :param command:
    :return:
    """
    d = {"command": e_cmd._serialization_type,
         "label": e_cmd._serialization_label,
         "content": e_cmd.json_serialize()}

    return CommandsContainer.create("native", "application/json", json.dumps(d).encode("utf-8"))


def persistable_to_executable_command(p_cmd: CommandsContainer, limit=1000):
    """
    A persistable command can be either a single command or a sequence of commands (like a spreadsheet). In the future
    it could even be a full script.

    Because the executable command is DIRECTLY executable, it is not possible to convert from persistable to a single
    executable command. But it is possible to obtain a list of executable commands, and this is the aim of this function

    The size of the list can be limited by the parameter "limit". "0" is for unlimited

    :param p_cmd:
    :return: A list of IExecutableCommand
    """
    # Create commands generator from factory (from generator_type and file_type)
    state = State()
    commands_generator = commands_container_parser_factory(p_cmd.generator_type, p_cmd.file_type, p_cmd.file, state)

    # Loop over the IExecutableCommand instances
    issues_aggreg = []
    outputs = []
    count = 0
    for cmd, issues in commands_generator:
        # If there are syntax ERRORS, STOP!!!
        stop = False
        if issues and len(issues) > 0:
            for t in issues:
                if t[0] == 3:  # Error
                    stop = True
        if stop:
            break

        issues_aggreg.extend(issues)

        count += 1
        if count >= limit:
            break


def execute_command(state, e_cmd: "IExecutableCommand") -> nexinfosys.IssuesOutputPairType:
    if e_cmd:
        return e_cmd.execute(state)
    else:
        return [], None  # issues, output


def execute_command_container(state, p_cmd: CommandsContainer, ignore_imports=False):
    return execute_command_container_file(state, p_cmd.generator_type, p_cmd.content_type, p_cmd.content, ignore_imports)


def execute_command_container_file(state, generator_type, file_type: str, file, ignore_imports):
    """
    This could be considered the MAIN method of the processing.
    1) Assuming an initial "state" (that can be clean or not),
    2) loops over the commands represented in a "file" of some of the supported types (JSON, Spreadsheet)
       2.1) Parses each command, returning an IExecutableCommand instance (containing the JSON definition inside)
       2.2) Executes each command, if there are no error issues. Command execution can reads and modifies "state"

    :param generator_type: Which commands generator (parser + constructor of IExecutableCommand instances) is used
    :param file_type: The file format
    :param file: The file contents
    :return: Issues and outputs (no outputs still required, probably won't be needed)
    """
    # Create commands generator from factory (from generator_type and file_type)
    commands_generator = commands_container_parser_factory(generator_type, file_type, file, state, None, None, ignore_imports)

    # Loop over the IExecutableCommand instances
    issues_aggreg = []
    outputs = []
    cmd_number = 0
    for cmd, issues in commands_generator:
        if len(issues) > 0:
            new_issues, errors_exist = transform_issues(issues, cmd, cmd_number)
            c = "\n"
            logging.debug(f"Issues:\n{c.join([i.description for i in new_issues])}")
        else:
            logging.debug(f"{type(cmd)} {cmd._source_block_name if hasattr(cmd, '_source_block_name') else ''}; # syntax issues: {len(issues)}")
        cmd_number += 1  # Command counter

        errors_exist = False

        if issues and len(issues) > 0:
            new_issues, errors_exist = transform_issues(issues, cmd, cmd_number)
            issues_aggreg.extend(new_issues)

        if errors_exist:
            break

        # ## COMMAND EXECUTION ## #
        issues, output = execute_command(state, cmd)

        if issues and len(issues) > 0:
            new_issues, errors_exist = transform_issues(issues, cmd, cmd_number)
            issues_aggreg.extend(new_issues)

        if output:
            outputs.append(output)

        if errors_exist:
            break

    return issues_aggreg, outputs


def transform_issues(issues: List[Union[dict, nexinfosys.Issue, tuple, Issue]], cmd, sheet_number: int) -> (List[Issue], bool):

    errors_exist = False
    new_issues: List[Issue] = []

    for i in issues:
        if isinstance(i, dict):
            issue = Issue(itype=IType(i["type"]), description=i["message"], ctype=i["c_type"],
                          location=IssueLocation(sheet_name=i["sheet_name"], sheet_number=i["sheet_number"]))
        elif isinstance(i, nexinfosys.Issue):  # namedtuple
            issue = Issue(itype=i.type, description=i.message, ctype=i.c_type,
                          location=IssueLocation(sheet_name=i.sheet_name, sheet_number=i.sheet_number))
        elif isinstance(i, tuple):
            issue = Issue(itype=IType(i[0]), description=i[1],
                          location=IssueLocation(sheet_name=""))
        else:  # isinstance(i, Issue):
            issue = i

        if issue.itype == IType.ERROR:
            errors_exist = True

        if not issue.ctype and cmd:  # "cmd" may be "None", in case the Issue is produced by the commands container loop
            issue.ctype = cmd._serialization_type

        if not issue.location.sheet_name or issue.location.sheet_name == "":
            issue.location.sheet_name = cmd._source_block_name if hasattr(cmd, "_source_block_name") else ""

        if not issue.location.sheet_number:
            issue.location.sheet_number = sheet_number

        new_issues.append(issue)

    return new_issues, errors_exist


def convert_generator_to_native(generator_type, file_type: str, file):
    """
    Converts a generator
    Creates a generator parser, then it feeds the file type and the file
    The generator parser has to parse the file and to elaborate a native generator (JSON)

    :param generator_type:
    :param file_type:
    :param file:
    :return: Issues and output file
    """

    output = []
    if generator_type.lower() not in ["json", "native", "primitive"]:
        # Create commands generator from factory (from generator_type and file_type)
        state = State()
        commands_generator = commands_container_parser_factory(generator_type, file_type, file, state)
        # Loop over the IExecutableCommand instances
        for cmd, issues in commands_generator:
            # If there are syntax ERRORS, STOP!!!
            stop = False
            if issues and len(issues) > 0:
                for t in issues:
                    if t["type"] == 3:  # Error
                        stop = True
                        break

            output.append({"command": cmd._serialization_type,
                           "label": cmd._serialization_label,
                           "content": cmd.json_serialize(),
                           "issues": issues
                           }
                          )
            if stop:
                break

    return output


# ######################################################################################################################
# SOLVING (PREPARATION AND CALL SOLVER)
# ######################################################################################################################

def prepare_and_solve_model(state: State, dynamic_scenario_parameters: Dict = None) -> List[Issue]:
    """
    Modify the state so that:
    * Implicit references of Interfaces to subcontexts are materialized
      * Creating processors
      * Creating interfaces in these processors
      * Creating relationships in these processors

    * The ProblemStatement class is considered for solving
    q* State is modified to contain the scalar and matrix indicators

    :param state:
    :param dynamic_scenario_parameters:
    :return:
    """
    prepare_model(state)
    issues = call_solver(state, dynamic_scenario_parameters)

    return issues


def call_solver(state: State, dynamic_scenario_parameters: Dict) -> List[Issue]:
    """
    Solve the problem

    :param state: MuSIASEM object model
    :param systems:
    :param dynamic_scenario_parameters: A dictionary containing a dynamic scenario, for interactive exploration
    """

    def obtain_problem_statement(dynamic_scenario_parameters: Dict = None) -> ProblemStatement:
        """
        Obtain a ProblemStatement instance
        Obtain the solver parameters plus a list of scenarios
        :param dynamic_scenario_parameters:
        :return:
        """
        if dynamic_scenario_parameters is not None:
            scenarios = create_dictionary()
            scenarios["dynamic"] = create_dictionary(dynamic_scenario_parameters)
            return ProblemStatement(scenarios=scenarios)
        else:
            ps_list: List[ProblemStatement] = glb_idx.get(ProblemStatement.partial_key())
            if len(ps_list) == 0:
                # No scenarios (dummy), and use the default solver
                scenarios = create_dictionary()
                scenarios["default"] = create_dictionary()
                return ProblemStatement(scenarios=scenarios)
            else:
                # TODO Combine all ProblemStatements into a single ProblemStatement
                return ps_list[-1]  # Take last ProblemStatement

    # Registry and the other objects also
    glb_idx, _, _, datasets, _ = get_case_study_registry_objects(state)

    global_parameters: List[Parameter] = glb_idx.get(Parameter.partial_key())

    dynamic_scenario = dynamic_scenario_parameters is not None
    if not dynamic_scenario:
        problem_statement = obtain_problem_statement()
    else:
        problem_statement = obtain_problem_statement(dynamic_scenario_parameters)

    # Obtain "parameters" Dataset
    datasets["params"] = obtain_parameters_dataset(global_parameters, problem_statement)

    solver_type_param = glb_idx.get(Parameter.partial_key("NISSolverType"))
    solver_type_param = solver_type_param[0]
    solver_type = solver_type_param.current_value

    issues: List[Issue] = []
    if solver_type == "FlowGraph":
        issues = flow_graph_solver(global_parameters, problem_statement, state, dynamic_scenario)

    return issues


def prepare_model(state) -> NoReturn:
    """
    Modify the state so that:
    * Implicit references of Interfaces to subcontexts are materialized
      * Creating processors
      * Creating interfaces in these processors
      * Creating relationships in these processors

    :param state:
    """

    # TODO: currently when an interface is defined as a Scale from two or more interfaces, the computed values are
    #  added while the intuition tells us that only one scale should be defined. We have to give a warning message
    #  if this situation happens.

    # Registry and the other objects also
    glb_idx, _, _, _, _ = get_case_study_registry_objects(state)
    # Prepare a Query to obtain ALL interfaces
    query = BasicQuery(state)
    filt = {}
    objs = query.execute([Factor], filt)
    for iface in objs[Factor]:  # type: Factor
        if strcmp(iface.processor.instance_or_archetype, 'Archetype') or strcmp(iface.processor.instance_or_archetype, 'No'):
            continue

        # If the Interface is connected to a "Subcontext" different than the owning Processor
        if iface.opposite_processor_type:
            if iface.opposite_processor_type.lower() != iface.processor.subsystem_type.lower():
                # Check if the interface has flow relationships
                # TODO An alternative is to search "observations" of type FactorsRelationDirectedFlowObservation
                #      in the same "iface"

                if iface.orientation.lower() == "input":
                    parameter = {"target": iface}
                else:
                    parameter = {"source": iface}

                relations = glb_idx.get(FactorsRelationDirectedFlowObservation.partial_key(**parameter))

                # If it does not have flow relationships:
                #  * define default Processor name and retrieve it (or if it does not exist, create it)
                #  * create an Interface into that Processor and a Flow Relationship
                if len(relations) == 0:
                    # Define the name of a Processor in the same context but in different subcontext
                    p_name = iface.opposite_processor_type  # + "_" + iface.processor.processor_system
                    p = glb_idx.get(Processor.partial_key(p_name, system=iface.processor.processor_system))
                    if len(p) == 0:
                        attributes = {
                            'subsystem_type': iface.opposite_processor_type,
                            'processor_system': iface.processor.processor_system,
                            'functional_or_structural': 'Functional',
                            'instance_or_archetype': 'Instance'
                            # 'stock': None
                        }

                        p = Processor(p_name, attributes=attributes)
                        glb_idx.put(p.key(), p)
                    else:
                        p = p[0]

                    attributes = {
                        'sphere': 'Technosphere' if iface.opposite_processor_type.lower() in ["local", "external"] else 'Biosphere',
                        'roegen_type': iface.roegen_type,
                        'orientation': "Input" if iface.orientation.lower() == "output" else "Output",
                        'opposite_processor_type': iface.processor.subsystem_type
                    }

                    # Create Interface (if it does not exist)
                    if not p.factors_find(iface.taxon.name):
                        f = Factor.create_and_append(name=iface.taxon.name,
                                                     processor=p,
                                                     in_processor_type=
                                                     FactorInProcessorType(external=False,
                                                                           incoming=iface.orientation.lower() == "output"),
                                                     attributes=attributes,
                                                     taxon=iface.taxon)

                        glb_idx.put(f.key(), f)

                    # Create Flow Relationship
                    if iface.orientation.lower() == "output":
                        source = iface
                        target = f
                    else:
                        source = f
                        target = iface

                    fr = FactorsRelationDirectedFlowObservation.create_and_append(
                        source=source,
                        target=target,
                        observer=None)
                    glb_idx.put(fr.key(), fr)


def obtain_parameters_dataset(global_parameters: List[Parameter], problem_statement: ProblemStatement):
    params_keys = []
    params_data = []
    for scenario_name, scenario_exp_params in problem_statement.scenarios.items():  # type: str, dict
        p = evaluate_parameters_for_scenario(global_parameters, scenario_exp_params)
        for k, v in p.items():
            params_keys.append((scenario_name, k))
            params_data.append(v)

    df = pd.DataFrame(params_data,
                      index=pd.MultiIndex.from_tuples(params_keys, names=["Scenario", "Parameter"]),
                      columns=["Value"])
    return get_dataset(df, "params", "Parameter values per Scenario")

# #####################################################################################################################
# >>>> INTERACTIVE SESSION <<<<
# #####################################################################################################################


class CreateNew(Enum):
    CASE_STUDY = 1
    VERSION = 2
    NO = 3


class InteractiveSession:
    """ 
    Main class for interaction with NIS
    The first thing would be to identify the user and create a GUID for the session which can be used by the web server
    to store and retrieve the interactive session state.
    
    It receives command_executors, modifying state accordingly
    If a reproducible session is opened, 
    """
    def __init__(self, session_factory):
        # Session factory with access to business logic database
        self._session_factory = session_factory

        # Interactive session ID
        self._guid = str(uuid.uuid4())

        # User identity, if given (can be an anonymous session)
        self._identity = None  # type: Identity
        self._state = State()  # To keep the state
        self._reproducible_session = None  # type: ReproducibleSession

    def reset_state(self):
        """ Restart state """
        self._state = State()
        # TODO self._recordable_session = None ??

    @property
    def state(self):
        return self._state

    @state.setter
    def state(self, state: State):
        self._state = state

    def get_sf(self):
        return self._session_factory

    def set_sf(self, session_factory):
        self._session_factory = session_factory
        if self._reproducible_session:
            self._reproducible_session.set_sf(session_factory)

    def open_db_session(self):
        return self._session_factory()

    def close_db_session(self):
        self._session_factory.remove()

    def quit(self):
        """
        End interactive session
        :return: 
        """
        self.close_reproducible_session()
        self.close_db_session()

    # --------------------------------------------------------------------------------------------

    def identify(self, identity_information, testing=False):
        """
        Given credentials of some type -identity_information-, link an interactive session to an identity.
        The credentials can vary from an OAuth2 Token to user+password.
        Depending on the type of credentials, invoke a type of "identificator" or other
        An interactive session without identification is allowed to perform a subset of available operations
        
        :param identity_information: 
        :return: True if the identification was successful, False if not 
        """
        # TODO Check the credentials
        if isinstance(identity_information, dict):
            if "user" in identity_information and testing:
                # Check if the user is in the User's table
                session = self._session_factory()
                src = session.query(User).filter(User.name == identity_information["user"]).first()
                # Check if the dataset exists. "ETL" it if not
                # ds = session.query(Dataset).\
                #     filter(Dataset.code == dataset).\
                #     join(Dataset.database).join(Database.data_source).\
                #     filter(DataSource.name == src_name).first()
                force_load(src)
                session.close()
                self._session_factory.remove()
                if src:
                    self._identity = src.name
                    if self._state:
                        self._state.set("_identity", self._identity)

                return src is not None
            elif "token" in identity_information:
                # TODO Validate against some Authentication service
                pass

    def get_identity_id(self):
        return self._identity

    def unidentify(self):
        # TODO The un-identification cannot be done in the following circumstances: any?
        self._identity = None
        if self._state:
            self._state.set("_identity", self._identity)

    # --------------------------------------------------------------------------------------------
    # Reproducible sessions and commands INSIDE them
    # --------------------------------------------------------------------------------------------
    def open_reproducible_session(self,
                                  case_study_version_uuid: str,
                                  recover_previous_state=True,
                                  cr_new: CreateNew = CreateNew.NO,
                                  allow_saving=True):
        self._reproducible_session = ReproducibleSession(self)
        self._reproducible_session.open(self._session_factory, case_study_version_uuid, recover_previous_state, cr_new, allow_saving)

    def close_reproducible_session(self, issues=None, output=None, save=False, from_web_service=False, cs_uuid=None, cs_name=None):
        if self._reproducible_session:
            if save:
                # TODO Save issues AND (maybe) output
                self._reproducible_session.save(from_web_service, cs_uuid=cs_uuid, cs_name=cs_name)
            uuid_, v_uuid, cs_uuid = self._reproducible_session.close()
            self._reproducible_session = None
            return uuid_, v_uuid, cs_uuid
        else:
            return None, None, None

    def reproducible_session_opened(self):
        return self._reproducible_session is not None

    @property
    def reproducible_session(self):
        return self._reproducible_session

    # --------------------------------------------------------------

    def execute_executable_command(self, cmd: IExecutableCommand):
        return execute_command(self._state, cmd)

    def register_executable_command(self, cmd: IExecutableCommand):
        self._reproducible_session.register_executable_command(cmd)

    def register_andor_execute_command_generator1(self, c: CommandsContainer, register=True, execute=False, ignore_imports=False):
        """
        Creates a generator parser, then it feeds the file type and the file
        The generator parser has to parse the file and to generate command_executors as a Python generator

        :param generator_type:
        :param file_type:
        :param file:
        :param register: If True, register the command in the ReproducibleSession
        :param execute: If True, execute the command in the ReproducibleSession
        :return:
        """
        if not self._reproducible_session:
            raise Exception("In order to execute a command generator, a work session is needed")
        if not register and not execute:
            raise Exception("More than zero of the parameters 'register' and 'execute' must be True")

        if register:
            self._reproducible_session.register_persistable_command(c)

        if execute:
            c.execution_start = datetime.datetime.now()
            pass_case_study = self._reproducible_session._session.version.case_study is not None
            ret = self._reproducible_session.execute_command_generator(c, pass_case_study, ignore_imports)
            c.execution_end = datetime.datetime.now()
            return ret
            # Or
            # return execute_command_container(self._state, c)
        else:
            return None

    def register_andor_execute_command_generator(self, generator_type, file_type: str, file, register=True, execute=False, ignore_imports=False):
        """
        Creates a generator parser, then it feeds the file type and the file
        The generator parser has to parse the file and to generate command_executors as a Python generator

        :param generator_type: 
        :param file_type: 
        :param file: 
        :param register: If True, register the command in the ReproducibleSession
        :param execute: If True, execute the command in the ReproducibleSession
        :return: 
        """

        return self.register_andor_execute_command_generator1(
            CommandsContainer.create(generator_type, file_type, file),
            register,
            execute,
            ignore_imports
        )

    # --------------------------------------------------------------------------------------------

    def get_case_studies(self):
        """ Get a list of case studies READABLE by current identity (or public if anonymous) """
        pass

    def get_case_study_versions(self, case_study: str):
        # TODO Check that the current user has READ access to the case study
        pass

    def get_case_study_version(self, case_study_version: str):
        # TODO Check that the current user has READ access to the case study
        pass

    def get_case_study_version_variables(self, case_study_version: str):
        """ A tree of variables, by type: processors, flows """
        pass

    def get_case_study_version_variable(self, case_study_version: str, variable: str):
        pass

    def remove_case_study_version(self, case_study_version: str):
        pass

    def share_case_study(self, case_study: str, identities: List[str], permission: str):
        pass

    def remove_case_study_share(self, case_study: str, identities: List[str], permission: str):
        pass

    def get_case_study_permissions(self, case_study: str):
        pass

    def export_case_study_version(self, case_study_version: str):
        pass

    def import_case_study(self, file):
        pass

# #####################################################################################################################
# >>>> REPRODUCIBLE SESSION <<<<
# #####################################################################################################################


class ReproducibleSession:
    def __init__(self, isess):
        # Containing InteractiveSession. Used to set State when a ReproducibleSession is opened and it overwrites State
        self._isess = isess  # type: InteractiveSession
        self._identity = isess._identity
        self._sess_factory = None
        self._allow_saving = None
        self._session = None  # type: CaseStudyVersionSession

    @property
    def ws_commands(self):
        return self._session.commands if self._session else None

    def open(self, session_factory, uuid_: str=None, recover_previous_state=True, cr_new:CreateNew=CreateNew.NO, allow_saving=True):
        """
        Open a work session

    +--------+--------+---------+-----------------------------------------------------------------------------------+
    | UUID   | cr_new | recover |        Behavior                                                                   |
    +--------+--------+---------+-----------------------------------------------------------------------------------+ 
    | !=None | True   | True    | Create new CS or version (branch) from "UUID", clone WS, recover State, append WS |
    | !=None | True   | False   | Create new CS or version (branch) from "UUID", Zero State, first WS               |
    | !=None | False  | True    | Recover State, append WS                                                          |
    | !=None | False  | False   | Zero State, append WS (overwrite type)                                            |
    | ==None | -      | -       | New CS and version, Zero State, first WS                                          |
    +--------+--------+---------+-----------------------------------------------------------------------------------+
    Use cases:
    * A new case study, from scratch. uuid_=None
    * A new case study, copied from another case study. uuid_=<something>, cr_new=CreateNew.CASE_STUDY, recover_previous_state=True
    * A new version of a case study
      - Copying previous version
      - Starting from scratch
    * Continue a case study version
      - But restart (from scratch)
    * Can be a Transient session

        :param uuid_: UUID of the case study or case study version. Can be None, for new case studies or for testing purposes.
        :param recover_previous_state: If an existing version is specified, it will recover its state after execution of all command_executors
        :param cr_new: If != CreateNew.NO, create either a case study or a new version. If == CreateNew.NO, append session to "uuid"
        :param allow_saving: If True, it will allow saving at the end (it will be optional). If False, trying to save will generate an Exception
        :return UUID of the case study version in use. If it is a new case study and it has not been saved, the value will be "None"
        """

        # TODO Just register for now. But in the future it should control that there is no other "allow_saving" ReproducibleSession opened
        # TODO for the same Case Study Version. So it implies modifying some state in CaseStudyVersion to have the UUID
        # TODO of the active ReproducibleSession, even if it is not in the database. Register also the date of "lock", so the
        # TODO lock can be removed in case of "hang" of the locker ReproducibleSession
        self._allow_saving = allow_saving
        self._sess_factory = session_factory
        session = self._sess_factory()
        if uuid_:
            uuid_ = str(uuid_)
            # Find UUID. Is it a Case Study or a Case Study version?
            # If it is the former, look for the active version.
            cs = session.query(CaseStudy).filter(CaseStudy.uuid == uuid_).first()
            if not cs:
                vs = session.query(CaseStudyVersion).filter(CaseStudyVersion.uuid == uuid_).first()
                if not vs:
                    ss = session.query(CaseStudyVersionSession).filter(CaseStudyVersionSession.uuid == uuid_).first()
                    if not ss:
                        raise Exception("Object '"+uuid_+"' not found, when opening a ReproducibleSession")
                    else:
                        vs = ss.version
                        cs = vs.case_study
                else:
                    cs = vs.case_study
            else:  # A case study, find the latest version (the version modified latest -by activity, newest ReproducibleSession-)
                max_date = None
                max_version = None
                for v in cs.versions:
                    for s in v.sessions:
                        if not max_date or s.open_instant > max_date:
                            max_date = s.open_instant
                            max_version = v
                vs = max_version
                cs = vs.case_study

            # List of active sessions
            # NOTE: instead of time ordering, the ID is used, assuming sessions with greater ID were created later
            lst = session.query(CaseStudyVersionSession). \
                filter(CaseStudyVersionSession.version_id == vs.id). \
                order_by(CaseStudyVersionSession.id). \
                all()
            idx = 0
            for i, ws in enumerate(lst):
                if ws.restarts:
                    idx = i
            lst = lst[idx:]  # Cut the list, keep only active sessions

            if cr_new != CreateNew.NO:  # Create either a case study or a case study version
                if cr_new == CreateNew.CASE_STUDY:
                    cs = copy.copy(cs)  # New Case Study: COPY CaseStudy
                else:
                    force_load(cs)  # New Case Study Version: LOAD CaseStudy (then version it)
                vs2 = copy.copy(vs)  # COPY CaseStudyVersion
                vs2.case_study = cs  # Assign case study to the new version
                if recover_previous_state:  # If the new version keeps previous state, copy it also
                    vs2.state = vs.state  # Copy state
                    vs2.state_version = vs.state_version
                    for ws in lst:  # COPY active ReproducibleSessions
                        ws2 = copy.copy(ws)
                        ws2.version = vs2
                        for c in ws.commands:  # COPY commands
                            c2 = copy.copy(c)
                            c2.session = ws2
                vs = vs2
            else:
                # Load into memory
                if len(lst) == 1:
                    ws = lst[0]
                    force_load(ws)
                force_load(vs)
                force_load(cs)

            if recover_previous_state:
                # Load state if it is persisted (if not EXECUTE, POTENTIALLY VERY SLOW)
                if vs.state:
                    # Deserialize
                    self._isess._state = deserialize_state(vs.state, vs.state_version)
                else:
                    self._isess._state = State()  # Zero State, execute all commands in sequence
                    for ws in lst:
                        for c in ws.commands:
                            execute_command_container(self._isess._state, c)
                if cr_new == CreateNew.VERSION:  # TODO Check if this works in all possible circumstances (combine the parameters of the function)
                    recover_previous_state = False
            else:
                self._isess._state = State()

        else:  # New Case Study AND new Case Study Version
            cs = CaseStudy()
            vs = CaseStudyVersion()
            vs.creation_instant = datetime.datetime.utcnow()
            vs.case_study = cs

        # Detach Case Study and Case Study Version
        if cs in session:
            session.expunge(cs)
        if vs in session:
            session.expunge(vs)
        # Create the Case Study Version Session
        usr = session.query(User).filter(User.name == self._identity).first()
        if usr:
            force_load(usr)
        else:
            if allow_saving:
                raise Exception("A user is required to register which user is authoring a case study")
        # TODO !!!!NEW CODE, ADDED TO SUPPORT NEEDED FUNCTIONALITY. NEEDS BETTER CODING!!!!
        restart = not recover_previous_state if uuid_ else True
        if not restart:
            self._session = ws
        else:
            self._session = CaseStudyVersionSession()
            self._session.version = vs
            self._session.who = usr
            self._session.restarts = True
        # If the Version existed, define "restarts" according to parameter "recover_previous_state"
        # ElseIf it is the first Session -> RESTARTS=True

        session.close()
        # session.expunge_all()
        self._sess_factory.remove()

    def update_current_version_state(self, lst_cmds):
        """ Designed to work using the REST interface. TEST in direct use. """
        # Version
        # v = self._session.version
        # Serialize state
        st = serialize_state(self._isess._state)
        # v.state = st
        # Open DB session
        session = self._sess_factory()
        # Load version and change its state
        v = session.query(CaseStudyVersion).get(self._session.version_id)
        v.state = st
        session.add(v)
        for c in lst_cmds:
            c2 = session.query(CommandsContainer).get(c.id)
            c2.execution_start = c.execution_start
            c2.execution_end = c.execution_end
            session.add(c2)
        session.commit()
        self._sess_factory.remove()

    def save(self, from_web_service=False, cs_uuid=None, cs_name=None):
        if not self._allow_saving:
            raise Exception("The ReproducibleSession was opened disallowing saving. Please close it and reopen it with the proper value")
        # Serialize state
        st = serialize_state(self._isess._state)
        self._session.version.state = st
        self._session.state = st
        ws = self._session

        # Open DB session
        session = self._sess_factory()
        # Change the case study
        if cs_uuid:
            # Load case study
            cs = session.query(CaseStudy).filter(CaseStudy.uuid == cs_uuid).first()
            if cs:
                ws.version.case_study = cs
            else:
                raise Exception("The case study UUID '"+cs_uuid+"' was not found")
        # Append commands, self._session, the version and the case_study
        if not from_web_service:
            for c in self._session.commands:
                session.add(c)
            session.add(ws)
            session.add(ws.version)
            session.add(ws.version.case_study)
        else:
            ws.who = session.merge(ws.who)
            cs_id = ws.version.case_study.id
            vs_id = ws.version.id
            if cs_id and not vs_id:
                ws.version.case_study = None
            if vs_id:
                ws.version = None

            if cs_id:
                cs = session.query(CaseStudy).get(cs_id)
            else:
                cs = ws.version.case_study
                session.add(cs)

            if vs_id:
                vs = session.query(CaseStudyVersion).get(vs_id)
                ws.version = vs
            else:
                ws.version.case_study = cs
                vs = ws.version
                session.add(vs)

            ws.close_instant = datetime.datetime.utcnow()
            session.add(ws)
            for c in self._session.commands:
                session.add(c)
        if cs_name:
            ws.version.name = cs_name

        # If it was called from the REST API, assure that the version has a creation date (it should not happen)
        if from_web_service and not vs.creation_instant:
            logging.debug("Late setup of version creation date")
            vs.creation_instant = datetime.datetime.utcnow()

        # Commit DB session
        session.commit()
        force_load(self._session)
        self._sess_factory.remove()

    def register_persistable_command(self, cmd: CommandsContainer):
        cmd.session = self._session

    def create_and_register_persistable_command(self, generator_type, file_type, file):
        """
        Generates command_executors from an input stream (string or file)
        There must be a factory to parse stream 
        :param generator_type: 
        :param file_type: 
        :param file: It can be a stream or a URL or a file name
        """
        c = CommandsContainer.create(generator_type, file_type, file)
        self.register_persistable_command(c)
        return c

    def execute_command_generator(self, cmd: CommandsContainer, pass_case_study=False, ignore_imports=False):
        if pass_case_study:  # CaseStudy can be modified by Metadata command, pass a reference to it
            self._isess._state.set("_case_study", self._session.version.case_study)
            self._isess._state.set("_case_study_version", self._session.version)

        ret = execute_command_container(self._isess._state, cmd, ignore_imports)

        if pass_case_study:
            self._isess._state.set("_case_study", None)
            self._isess._state.set("_case_study_version", None)

        return ret

    def register_executable_command(self, command: IExecutableCommand):
        c = executable_command_to_commands_container(command)
        c.session = self._session

    def set_sf(self, session_factory):
        self._sess_factory = session_factory

    @property
    def commands(self):
        return self._session.commands

    @property
    def case_study(self):
        return self._session.version.case_study

    def close(self) -> tuple:
        if not self._session:
            raise Exception("The CaseStudyVersionSession is not opened")
        id3 = self._session.uuid, self._session.version.uuid, self._session.version.case_study.uuid
        self._session = None
        self._allow_saving = None
        return id3


def execute_file_return_issues(file_name, generator_type):
    """
    Execution of files in the context of TESTS

    :param file_name:
    :param generator_type:
    :return:
    """
    if generator_type == "spreadsheet":
        content_type = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        read_type = "rb"
    elif generator_type == "native":
        content_type = "application/json"
        read_type = "r"

    prepare_and_reset_database_for_tests()
    isess = InteractiveSession(DBSession)
    isess.identify({"user": "test_user"}, testing=True)  # Pass just user name.
    isess.open_reproducible_session(case_study_version_uuid=None,
                                    recover_previous_state=None,
                                    cr_new=CreateNew.CASE_STUDY,
                                    allow_saving=False)

    # Add system-level entities from JSON definition in "default_cmds"
    ret = isess.register_andor_execute_command_generator("json", "application/json", nexinfosys.default_cmds, False, True)

    # Execute current file
    with open(file_name, read_type) as f1:
        buffer = f1.read()

    issues, output = isess.register_andor_execute_command_generator(generator_type, content_type, buffer, False, True)

    for idx, issue in enumerate(issues):
        logging.debug(f"Issue {idx+1}/{len(issues)} = {issue}")

    logging.debug(f"Output = {output}")

    isess.close_reproducible_session()
    isess.close_db_session()
    return isess, issues


def execute_file(file_name, generator_type):
    """
    Execution of files in the context of TESTS

    :param file_name:
    :param generator_type:
    :return:
    """
    return execute_file_return_issues(file_name, generator_type)[0]  # Return just "isession"


if __name__ == '__main__':
    import jsonpickle
    with open("/home/rnebot/pickled_state", "r") as f:
        s = f.read()
    o = jsonpickle.decode(s)
    # Submit Worksheet as New Case Study
    isess = InteractiveSession()
    isess.quit()

