import copy
import json
import pandas as pd
from pathlib import Path
import pandera as pa
import ast
from Applications.Sparks_functions.generic.basefile_schema import methods_schema, hierarchy_schema
from Applications.Sparks_functions.generic.generic_dataclass import *


logger = logging.getLogger("sparks")
bd.projects.set_current(bw_project)            


class SoftLinkCalEnb():
    """
    Transform table-like input into a hierarchy (ENBIOS) like format
    """

    def __init__(self,
                 calliope: pd.DataFrame,
                 mother_data: list[BaseFileActivity],
                 sublocations: list[str],
                 motherfile: Path,
                 smaller_vers: bool =None):

        self.calliope=calliope.copy()
        self.motherfile=Path(motherfile)
        self.sublocations=sublocations
        self.smaller_vers=smaller_vers
        self.mother_data=mother_data

        logger.info("===Soflink class initiated===")
        self._validate_inputs()

    def _validate_inputs(self)-> None:
        """
        Validate input data using pandera schemas
        :return:
        """
        logger.info("Validating input data")
        #try:
         #   calliope_cleaning_schema.validate(self.calliope, lazy=True)
        #except pa.errors.SchemaErrors as e:
         #   logger.error(f"Input data validation error: {e.failure_cases}")
          #  raise
        #logger.info("Input data validated successfully")
        
        # check unique entries
        act_names = [getattr(a, "full_name", None) for a in self.mother_data]
        if len(act_names) != len(set(act_names)):
            logger.error("Duplicate full_name detected among mother_data activities. Consider deduplicating.")
            raise ValueError("Duplicate full_name detected among mother_data activities. Consider deduplicating.")


    def _get_scenarios(self):

        cal_dat = self.calliope
        cal_dat['scenarios'] = cal_dat['scenarios'].astype(str)

        if self.smaller_vers:  # get a small version of the data (only the first)
            try:
                scenario = cal_dat['scenarios'].unique().tolist()[0]
                cal_dat['scenarios'] = cal_dat['scenarios'].astype(str)
                cal_dat = cal_dat[cal_dat['scenarios'] == str(scenario)]
                logger.info(f"Using only scenario {scenario}")
            except:
                raise ValueError('Scenarios out of bonds')

        scenarios_check = [str(x) for x in
                           cal_dat['scenarios'].unique()]  # Convert to string, just in case the scenario is a number
        logger.debug(f"Found scenarios in the dataset: {scenarios_check}")

        scenarios = []
        for scenario, group in cal_dat.groupby('scenarios'):
            logger.debug(f"[Scenarios] Processing scenario: {scenario} with {len(group)} activities")
            activities = [
                Activity_scenario(
                    alias=row['full_name_energy'],
                    amount=row['energy_value'],
                    unit=row['unit']
                )
                for _, row in group.iterrows() # the issue might come from the data
            ]
            if not activities:
                logger.warning(f"[Scenarios] Scenario '{scenario}' has no activities!")

            scenarios.append(
                Scenario(name=str(scenario), activities=activities).to_dict()
            )

        assert (len(scenarios) == len(scenarios_check))
        return scenarios


    def _get_methods(self):
        """ Get methods from the motherfile"""
        methods_data = pd.read_excel(self.motherfile, sheet_name='Methods')
        logger.info("Validating methods sheet")

        try:
           methods_schema.validate(methods_data, lazy=True)
        except pa.errors.SchemaErrors as e:
            logger.error(f"Input data validation error: {e.failure_cases}")
            raise

        methods = []

        for meth in methods_data['Formula']:
            try:
                parsed = ast.literal_eval(meth)
                methods.append(
                    Method(parsed).to_dict(*parsed)
                )
            except (ValueError, SyntaxError) as e:
                logger.error(f"Failed to parse method entry '{meth}': {e}")
                continue

        return  {key: value for d in methods for key, value in d.items()}


    def run(self, path: Optional[Union[str, Path]] = None) -> dict:
        """public function """
        logger.info("Starting ENBIOS generation")

        hierarchy = Hierarchy(base_path=self.motherfile, motherdata=self.mother_data,
                              sublocations=self.sublocations, data=self.calliope).generate_hierarchy()
        logger.info("Hierarchy generated (top-level name: %s)",
                    hierarchy.get("name") if isinstance(hierarchy, dict) else "N/A")

        methods = self._get_methods()
        logger.info("Methods extracted: %d", len(methods))

        scenarios = self._get_scenarios()
        logger.info("Scenarios prepared: %d", len(scenarios))

        enbios2_data = {
            "adapters": [
                {
                    "adapter_name": "brightway-adapter",
                    "config": {"bw_project": bw_project},
                    "methods": methods,
                }
            ],
            "hierarchy": hierarchy,
            "scenarios": scenarios,
        }

        if path is not None:
            out_path = Path(path)
            out_path.parent.mkdir(parents=True, exist_ok=True)
            try:
                with open(out_path, "w", encoding="utf8") as fh:
                    json.dump(enbios2_data, fh, indent=4)
                logger.info("Wrote ENBIOS JSON to %s", out_path.resolve())
            except Exception:
                logger.exception("Failed to write ENBIOS JSON to %s", out_path)

        self.enbios2_data = enbios2_data
        logger.info("ENBIOS data generation finished")
        return enbios2_data


class Hierarchy:
    def __init__(self,
                 base_path: str,
                 motherdata,
                 sublocations:list,
                 data: pd.DataFrame):

        self.parents = pd.read_excel(base_path,
                                     sheet_name='Dendrogram_top',
                                     keep_default_na=False,
                                     na_values=[])
        self._validate_hierarchy_input()
        self.hierarchy_data = self._extract_data(data)

        self.motherdata=motherdata
        self.subloc = sublocations
        logger.debug("Hierarchy class initiated")

        self.motherdata = self.hierarchy_data
        self.data=self._transform_motherdata()


    def _validate_hierarchy_input(self):
        """Validate the input Motherfile schema"""
        logger.info("Validating hierarchy input")
        try:
            hierarchy_schema.validate(self.parents, lazy=True)
        except pa.errors.SchemaErrors as e:
            logger.error(f"Input hierarchy data validation error: {e.failure_cases}")
            logger.debug(self.parents)
            raise ValueError("Hierarchy input validation failed, see logs for details") from err



    def _create_copies(self,
                       existing_act: BaseFileActivity,
                       new_names: List[str])->List[BaseFileActivity]:
            """ Pass a the name of an existing BasefileAct,
             a list of new names, and return a list of copies"""

            copies=[]
            for new_name in new_names:
                new_act=BaseFileActivity(
                    name=new_name,
                    region=existing_act.region,
                    carrier=existing_act.carrier,
                    parent=existing_act.parent,
                    code=existing_act.code,
                    factor=existing_act.factor,
                    full_alias = existing_act.full_alias,
                    alias_filename_loc=existing_act.alias_filename_loc,
                    init_post=False
                )
                new_act = copy.deepcopy(existing_act)
                new_act.name = new_name
                new_act.alias_carrier_region = new_name
                copies.append(new_act)

            return copies





    def _extract_data(self,df) -> List['HierarchyActivity']:
        """
        extract activities from the basefile and create a list HierarchyActivity instances
        """
        logger.info("Extracting LCA activities...")

        def _create_activity(row):
            # move the activities from the basefile into a DataBase dataclass
            try:
                kwargs = {
                    'name': row['techs'],
                    "full_name": row['full_name_energy'],
                    "parent": row['parent'],
                    "code": row['code']
                }
                return HierarchyActivity(**kwargs)

            except KeyError as e:
                logger.warning(f"Warning during data extraction {e}")
                return None

        logger.info("Extracting final hiearchy information")
        logger.debug("Columns present: %s", df.columns.tolist())
        logger.debug("dtypes:\n%s", df.dtypes.to_dict())
        logger.debug("Sample head:\n%s", df.head(3).to_dict(orient='records'))

        base_activities = df.progress_apply(_create_activity, axis=1).dropna().tolist()

        return base_activities

    def _transform_motherdata(self):
        """ Transform mother data into a config dictionary
        This should be equal to the last level of the hierarchy"""

        unique_dict_2 ={}
        for x in self.hierarchy_data:
            if x.full_name not in unique_dict_2:
                unique_dict_2[x.full_name] = {'name': x.full_name, 'adapter': 'bw',
                                              'config': {'code': x.code}}

        unique_items2 = list(unique_dict_2.values())

        return unique_items2


    def generate_hierarchy(self):

        last_level=None
        last_level_branches=[]
        last_level_hierachy=[] # official list
        
        for level in reversed(self.parents['Level'].unique().tolist()):
            data=self.parents.loc[self.parents['Level']==level]
            if last_level is None:
                last_level_branches = [
                    Last_Branch(
                        name=row['Processor'],
                        level=level,
                        parent=row['ParentProcessor'],
                        origin=[x for x in self.motherdata if x.parent == row['Processor']],
                    )
                    for _, row in data.iterrows()
                ]
                last_level_hierachy=[x.leafs for x in last_level_branches]
                last_level=level
                continue

            if last_level is not None and level!=self.parents['Level'].unique().tolist()[0]:
                last_level_branches = [
                    Branch(
                        name = row['Processor'],
                        level=level,
                        parent= row['ParentProcessor'],
                        origin=[x for x in last_level_branches if x.parent == row['Processor']]
                    )
                    for _, row in data.iterrows()]
                
                last_level_hierachy=[x.leafs for x in last_level_branches]

            else:
                last_level_branches = [
                    Branch(
                        name=row['Processor'],
                        level=level,
                        parent=row['ParentProcessor'],
                        origin=[x for x in last_level_branches if x.parent == row['Processor']]
                    )
                    for _, row in data.iterrows()]
        
        
        return {'name': last_level_branches[0].name, 'aggregator': 'sum', 'children': last_level_branches[0].leafs}