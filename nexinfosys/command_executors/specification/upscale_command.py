import json
import math

from nexinfosys.common.helper import strcmp
from nexinfosys.model_services import IExecutableCommand, State, get_case_study_registry_objects
from nexinfosys.models.musiasem_concepts import Observer, Processor, \
    FactorsRelationScaleObservation, \
    ProcessorsRelationPartOfObservation, \
    ProcessorsRelationUpscaleObservation, \
    ProcessorsSet


class UpscaleCommand(IExecutableCommand):
    """
    Serves to instantiate processor templates inside another processor
    See Almeria case study for the base example of this
    """
    def __init__(self, name: str):
        self._name = name
        self._content = None

    def execute(self, state: "State"):
        """
        For each parent processor clone all the child processors.
        The cloning process may pass some factor observation, that may result in
        """
        some_error = False
        issues = []

        parent_processor_type = self._content["parent_processor_type"]
        child_processor_type = self._content["child_processor_type"]
        scaled_factor = self._content["scaled_factor"]
        source = self._content["source"]
        # column_headers = self._content["column_headers"]
        # row_headers = self._content["row_headers"]
        scales = self._content["scales"]

        # Find processor sets, for parent and child
        glb_idx, p_sets, hh, datasets, mappings = get_case_study_registry_objects(state)
        if parent_processor_type not in p_sets:
            some_error = True
            issues.append((3, "The processor type '"+parent_processor_type +
                           "' (appointed for parent) has not been found in the commands execute so far"))

        if child_processor_type not in p_sets:
            some_error = True
            issues.append((3, "The processor type '"+child_processor_type +
                           "' (should be child processor) has not been found in the commands execute so far"))

        if some_error:
            return issues, None

        # CREATE the Observer of the Upscaling
        oer = glb_idx.get(Observer.partial_key(source))
        if not oer:
            oer = Observer(source)
            glb_idx.put(oer.key(), oer)
        else:
            oer = oer[0]

        # Processor Sets have associated attributes, and each of them has a code list
        parent = p_sets[parent_processor_type]  # type: ProcessorsSet
        child = p_sets[child_processor_type]  # type: ProcessorsSet

        # Form code lists from the command specification
        code_lists = None
        for sc_dict in scales:
            codes = sc_dict["codes"]
            if not code_lists:
                code_lists = [set() for _ in codes]

            for i, c in enumerate(codes):
                code_lists[i].add(c)

        # Match existing code lists (from Processor attributes) with the ones gathered in the specification of
        # the two (parent and child) processors sets.
        # Form lists of attributes of processors used in the code lists
        parent_attrs = []
        child_attrs = []
        matched = []
        for i, cl in enumerate(code_lists):
            found = False
            for attr, attr_values in parent.attributes.items():
                if set(attr_values).issuperset(cl):
                    parent_attrs.append((attr, i))  # (Attribute, code list index)
                    found = True
                    break
            for attr, attr_values in child.attributes.items():
                if set(attr_values).issuperset(cl):
                    child_attrs.append((attr, i))  # (Attribute, code list index)
                    found = True
                    break
            matched.append(found)
        for i, found in enumerate(matched):
            if not found:
                cl = code_lists[i]
                # TODO Try cl as a list of names of parent or child processors
                if not found:
                    issues.append((2, "The code list: " + ", ".join(cl) + " is not contained in the attributes of the parent processors set '" + parent_processor_type + "' nor in the attributes of the child processors set '" + child_processor_type + "'"))

        # Execute the upscale for each
        cached_processors = {}
        for sc_dict in scales:
            try:
                non_zero_weight = math.fabs(float(sc_dict["weight"])) > 1e-6
            except:
                non_zero_weight = True
            if not non_zero_weight:
                continue

            codes = sc_dict["codes"]
            # Find parent processor
            parent_dict = {attr: codes[i] for attr, i in parent_attrs}
            d2s = str(parent_dict)
            if d2s in cached_processors:
                parent = cached_processors[d2s]
                if not parent:
                    issues.append((3, "Either the tuple (" + d2s + ") did not match any Processor or matched more than one."))
            else:
                parent_dict.update(Processor.partial_key())

                # Obtain Processor matching the attributes <<<<<<<<<<
                # Query the PartialRetrievalDictionary by attributes
                parents = glb_idx.get(parent_dict)

                if len(parents) > 1:
                    issues.append((3, "The tuple ("+str(parent_dict)+") matches "+str(len(parents))+" Processors: "+(", ".join([p.name for p in parents]))))
                    parent = None
                elif len(parents) == 0:
                    issues.append((3, "The tuple (" + str(parent_dict) + ") did not match any Processor"))
                    parent = None
                else:
                    parent = parents[0]

                cached_processors[d2s] = parent

            # Find child processor
            child_dict = {attr: codes[i] for attr, i in child_attrs}
            d2s = str(child_dict)
            if d2s in cached_processors:
                child = cached_processors[d2s]
                if not child:
                    issues.append((3, "Either the tuple (" + d2s + ") did not match any Processor or matched more than one."))
            else:
                child_dict.update(Processor.partial_key())

                # Obtain Processors matching the attributes
                # Query the PartialRetrievalDictionary by attributes
                children = glb_idx.get(child_dict)

                if len(children) > 1:
                    issues.append((3, "The tuple ("+str(child_dict)+") matches "+str(len(parents))+" Processors: "+(", ".join([p.name for p in children]))))
                    child = None
                elif len(children) == 0:
                    issues.append((3, "The tuple (" + str(child_dict) + ") did not match any Processor"))
                    child = None
                else:
                    child = children[0]  # type: Processor

                cached_processors[d2s] = child

            # Clone child processor (and its descendants) and add an upscale relation between "parent" and the clone
            if parent and child:
                if non_zero_weight:
                    # Clone the child processor
                    # TODO
                    cloned_child, cloned_children = child.clone(state=glb_idx)
                    Processor.register([cloned_child] + list(cloned_children), glb_idx)

                    # Create the new Relation Observations
                    # - Part-of Relation
                    o1 = ProcessorsRelationPartOfObservation.create_and_append(parent, cloned_child, oer)  # Part-of
                    glb_idx.put(o1.key(), o1)
                    # - Upscale Relation
                    quantity = str(sc_dict["weight"])
                    if True:
                        # Find Interface named "scaled_factor"
                        for f in parent.factors:
                            if strcmp(f.name, scaled_factor):
                                origin = f
                                break
                        else:
                            origin = None
                        for f in cloned_child.factors:
                            if strcmp(f.name, scaled_factor):
                                destination = f
                                break
                        else:
                            destination = None

                        if origin and destination:
                            o3 = FactorsRelationScaleObservation.create_and_append(origin, destination,
                                                                                   observer=None,
                                                                                   quantity=quantity)
                            glb_idx.put(o3.key(), o3)
                        else:
                            raise Exception("Could not find Interfaces to define a Scale relation. Processors: " +
                                            parent.name+", "+cloned_child.name+"; Interface name: "+scaled_factor)
                    else:
                        o3 = ProcessorsRelationUpscaleObservation.create_and_append(parent, cloned_child,
                                                                                    observer=None,
                                                                                    factor_name=scaled_factor,
                                                                                    quantity=quantity)
                        glb_idx.put(o3.key(), o3)
            else:
                # TODO
                parent_dict = str({attr: codes[i] for attr, i in parent_attrs})
                child_dict = str({attr: codes[i] for attr, i in child_attrs})
                if not parent and child:
                    issues.append((2, "Could not find parent Processor matching attributes: "+parent_dict))
                elif not child and parent:
                    issues.append((2, "Could not find child Processor matching attributes: "+child_dict))
                else:
                    issues.append((2, "Could not find parent Processor matching attributes: "+parent_dict+", nor child Processor matching attributes: " + child_dict))

        return issues, None

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
