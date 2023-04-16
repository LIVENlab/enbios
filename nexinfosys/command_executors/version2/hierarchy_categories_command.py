import json

from nexinfosys.command_generators import Issue, IssueLocation, IType
from nexinfosys.common.helper import strcmp
from nexinfosys.model_services import IExecutableCommand, get_case_study_registry_objects
from nexinfosys.models.musiasem_concepts import HierarchySource, HierarchyGroup, Hierarchy, HierarchyLevel, HierarchyNode, \
    Taxon


class HierarchyCategoriesCommand(IExecutableCommand):
    """
    Serves to specify a hierarchy (composition or taxonomy) of Observables
    Observables can be Processors, Factors or external categories
    """
    def __init__(self, name: str):
        self._name = name
        self._content = None

    def execute(self, state: "State"):
        """
        Create a Hierarchy of Taxon. The exact form of this hierarchy is different depending on the concept:
        * FactorTypes and Categories use Hierarchies, which are intrinsic.
            The hierarchy name is passed to the containing Hierarchy object
        * Processors use Part-Of Relations. In this case, the hierarchy name is lost
        Names of Processor and FactorTypes are built both in hierarchical and simple form
        The hierarchical is all the ancestors from root down to the current node, separated by "."
        The simple name is just the current node. If there is already another concept with that name, the simple name
        is not stored (STORE BOTH CONCEPTS by the same name, and design some tie breaking mechanism??)
        """
        issues = []
        glb_idx, p_sets, hh, datasets, mappings = get_case_study_registry_objects(state)
        name = self._content["command_name"]

        # Process parsed information
        for item in self._content["items"]:
            r = item["_row"]
            # HierarchySource (Optional)
            hsource = item.get("source", None)  # Code of entity defining the Hierarchy
            if hsource:
                tmp = hsource
                hsource = glb_idx.get(HierarchySource.partial_key(name=hsource))
                if len(hsource) == 0:
                    hsource = HierarchySource(name=tmp)
                    glb_idx.put(hsource.key(), hsource)
                else:
                    hsource = hsource[0]

            hname = item.get("hierarchy_name", None)
            if not hname:
                issues.append(Issue(itype=IType.ERROR,
                                    description="The name of the Hierarchy has not been defined. Skipped.",
                                    location=IssueLocation(sheet_name=name, row=r, column=None)))
                continue

            # HierarchyGroup (equivalent to Hierarchy of Code Lists, HCL)
            hg = item.get("hierarchy_group", None)
            if hg:
                is_code_list = False  # Hierarchy group
            else:
                is_code_list = True  # Hierarchy group for the Code List, with the same name
                hg = hname

            # Check if the HierarchyGroup is previously defined. YES, use it; NO, create new HierarchyGroup
            tmp = hg
            hg = glb_idx.get(HierarchyGroup.partial_key(name=hg))
            if len(hg) == 0:
                hg = HierarchyGroup(name=tmp, source=hsource)
                glb_idx.put(hg.key(), hg)
            else:
                hg = hg[0]

            # Check if the Hierarchy is defined. YES, get it; NO, create it
            tmp = hname
            h = glb_idx.get(Hierarchy.partial_key(name=hname))
            if len(h) == 0:
                h = Hierarchy(name=tmp)
                glb_idx.put(h.key(), h)
                glb_idx.put(h.key(hg.name+"."+h.name), h)  # Register with alternative (full) name
            else:
                h = h[0]

            # Add the Hierarchy to the HierarchyGroup (if not)
            if h not in hg.hierarchies:
                hg.hierarchies.append(h)

            # Level
            level = item.get("level", None)
            if level:
                # Check if the level is defined. YES, get it; NO, create it
                for l in h.levels:
                    if strcmp(l.name, level):
                        level = l
                        break
                else:
                    level = HierarchyLevel(name=level, hierarchy=h)
                    h.levels.append(level)

            code = item.get("code", None)
            label = item.get("label", None)
            description = item.get("description", None)
            attributes = item.get("attributes", None)
            expression = item.get("expression", None)

            # Parent property (what really defines Hierarchies)
            parent_code = item.get("parent_code", None)
            if parent_code:
                ph = h  # Parent Hierarchy is the same as current hierarchy
                pcode = ph.codes.get(parent_code, None)
                if not pcode:
                    issues.append(Issue(itype=IType.ERROR,
                                        description="Could not find code '"+parent_code+"' in hierarchy '"+ph.name+"'. Skipped.",
                                        location=IssueLocation(sheet_name=name, row=r, column=None)))
                    continue
            else:
                pcode = None

            # ReferredHierarchy. If we are not defining a Code List, the base hierarchy has to be mentioned
            if not is_code_list:
                ref_hierarchy = item.get("referred_hierarchy", None)
                if not ref_hierarchy:
                    issues.append(Issue(itype=IType.ERROR,
                                        description="For HCLs, defining ReferredHierarchy is mandatory",
                                        location=IssueLocation(sheet_name=name, row=r, column=None)))
                    continue

                tmp = ref_hierarchy
                ref_hierarchy = glb_idx.get(Hierarchy.partial_key(name=ref_hierarchy))
                if len(ref_hierarchy) == 0:
                    issues.append(Issue(itype=IType.ERROR,
                                        description="ReferredHierarchy '"+tmp+"' not defined previously",
                                        location=IssueLocation(sheet_name=name, row=r, column=None)))
                    continue
                else:
                    ref_hierarchy = ref_hierarchy[0]

                ref_code = ref_hierarchy.codes.get(code, None)
                if not ref_code:
                    issues.append(Issue(itype=IType.ERROR,
                                        description="Code '"+code+"' not found in referred hierarchy '"+ref_hierarchy.name+"'",
                                        location=IssueLocation(sheet_name=name, row=r, column=None)))
                    continue

                # Ignore: LABEL, DESCRIPTION. Copy them from referred code
                label = ref_code.label
                description = ref_code.description
            else:
                ref_code = None

            c = h.codes.get(code, None)
            if c:
                issues.append(Issue(itype=IType.ERROR,
                                    description="Code '" + code + "' in hierarchy '" + h.name + "' redefined.",
                                    location=IssueLocation(sheet_name=name, row=r, column=None)))
                continue

            # Finally, create the HierarchyCode with all the gathered attributes, then weave it to other
            # (name, label=None, description=None, referred_node=None, parent=None, parent_weight=1.0, hierarchy=None)
            c = Taxon(name=code, hierarchy=h, level=level,
                      referred_taxon=ref_code, parent=pcode,
                      label=label, description=description,
                      attributes=attributes, expression=expression)
            # Add code to hierarchy
            h.codes[code] = c
            if not c.parent:
                h.roots_append(c)
            # Add code to level
            if level:
                level.codes.add(c)
            # Add child to parent code
            # (DONE BY THE CONSTRUCTOR!!)
            # if pcode:
            #     pcode.children_codes.append(c)

        return issues, None  # Issues, Output

    def estimate_execution_time(self):
        return 0

    def json_serialize(self):
        # Directly return the content
        return self._content

    def json_deserialize(self, json_input):
        # TODO Check validity
        issues = []
        if isinstance(json_input, dict):
            self._content = json_input
        else:
            self._content = json.loads(json_input)
        return issues
