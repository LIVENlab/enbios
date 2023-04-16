"""
Evaluation of ASTs

Ideas copied/adapted from:
https://gist.github.com/cynici/5865326

"""
import importlib
import math
import re
import traceback
from typing import Dict, Tuple, Union, List

import lxml
import numpy as np
import pandas as pd
from lxml import etree

from nexinfosys import case_sensitive
from nexinfosys.command_generators import global_functions, IType, Issue, IssueLocation, parser_field_parsers
from nexinfosys.command_generators.parser_field_parsers import string_to_ast, arith_boolean_expression, key_value_list, \
    simple_ident, expression_with_parameters, number_interval, arith_boolean_expression_with_less_tokens, \
    indicator_expression
from nexinfosys.common.helper import create_dictionary, PartialRetrievalDictionary, strcmp, is_float
from nexinfosys.model_services import State
from nexinfosys.models.musiasem_concepts import ExternalDataset, FactorType, Processor, Hierarchy


# #################################################################################################################### #


# -- FUNCTIONS
def get_interface_type(attribute, value, prd: PartialRetrievalDictionary = None):
    """
    Obtain the name of an InterfaceType given the value of an attribute
    (Obtain the registry of objects)

    :param attribute:
    :param value:
    :param prd: A PartialRetrievalDictionary, passed in State "_glb_idx" to the AST evaluator by
    :return:
    """

    if not prd:
        raise Exception(f"No Global-Index parameter passed to InterfaceType function")
    else:
        # Obtain ALL InterfaceTypes, then ONE having attribute "attribute" with value <value>
        its = prd.get(FactorType.partial_key())
        ret = None
        for it in its:
            v = vars(it).get(attribute)
            if not v:
                v = it.attributes.get(attribute)
            if v and (strcmp(v, str(value)) or (is_float(value) and float(v) == float(value))):
                ret = it.name
                break
        if ret:
            return ret
        else:
            raise Exception(f"No InterfaceType found having attribute '{attribute}' with value '{value}'")


def get_processor(attribute, value, prd: PartialRetrievalDictionary = None):
    """
    Obtain the name of a Processor given the value of an attribute
    (Obtain the registry of objects)

    :param attribute:
    :param value:
    :param prd: A PartialRetrievalDictionary, passed in State "_glb_idx" to the AST evaluator by
    :return:
    """

    if not prd:
        raise Exception(f"No Global-Index parameter passed to Processor function")
    else:
        # Obtain ALL Processors, then ONE having attribute "attribute" with value <value>
        procs = prd.get(Processor.partial_key())
        ret = None
        for proc in procs:
            v = vars(proc).get(attribute)
            if not v:
                v = proc.attributes.get(attribute)
            if v and (strcmp(v, str(value)) or (is_float(value) and float(v) == float(value))):
                ret = proc.name
                break
        if ret:
            return ret
        else:
            raise Exception(f"No Processor found having attribute '{attribute}' with value '{value}'")


def get_nis_name(original_name):
    """
    Convert the original_name to a name valid for NIS

    :param original_name:
    :return:
    """
    if original_name.strip() == "":
        return ""
    else:
        prefix = original_name[0] if original_name[0].isalpha() else "_"
        remainder = original_name[1:] if original_name[0].isalpha() else original_name

        return prefix + re.sub("[^0-9a-zA-Z_]", "_", remainder)


def call_udif_function(function_name, state: State = None):
    mod_name, func_name = function_name.rsplit('.', 1)
    mod = importlib.import_module(mod_name)
    func = getattr(mod, func_name)
    kwargs = dict(state=state)
    # CALL FUNCTION!!
    try:
        obj = func(**kwargs)
    except Exception as e:
        obj = None

    return obj


lcia_methods = None  # type: PartialRetrievalDictionary


def lcia_method(method: str, category: str, indicator: str,
                compartment: str = None,
                subcompartment: str = None,
                horizon: str = None, sum_if_multiple: bool = True,
                trace: int = 0,
                state: State = None, lcia_methods_dict: Dict = None):
    """
    Calculate the LCIA method indicator

    :param indicator: Indicator name
    :param method: LCIA method weighting
    :param horizon: Time horizon
    :param compartment: Compartment to which the indicator applies
    :param subcompartment: Subcompartment to which the indicator applies
    :param category: Category to which the indicator applies
    :param sum_if_multiple: If there are several indicators with the same name, sum them
    :param trace: If True, print the important information
    :param state: Current values of processor plus parameters
    :param lcia_methods_dict: Where LCIA data is collected
    :return: A dictionary with the indicators and calculated values
    """
    global lcia_methods
    if not lcia_methods and lcia_methods_dict:
        lcia_methods = PartialRetrievalDictionary()
        for i, t in enumerate(lcia_methods_dict.items()):
            # [0] m=Method,
            # [1] t=caTegory,
            # [2] d=inDicator,
            # [3] h=Horizon,
            # [4] i=Interface,
            # [5] c=Compartment,
            # [6] s=Subcompartment,
            k, v = t
            _ = dict(m=k[0], t=k[1], d=k[2], h=k[3], i=k[4], c=k[5], s=k[6])
            # NOTE: 'i' is used to assure the tuple is unique
            lcia_methods.put(_, (v[0], v[1], v[2], i))

    if lcia_methods is None or \
            indicator is None or indicator.strip() == "" or \
            method is None or method.strip() == "" or \
            category is None or category.strip() == "":
        return None

    trace = int(trace) == 1

    if trace:
        print(f"Processor: {state.get('__processor_name')}\tLCIA method: {method}\tCategory: {category}\tIndicator: {indicator}\t"
              f"Comp: {compartment}\tSubcomp: {subcompartment}\t"
              f"Horizon: {horizon}\tsum_if_multiple: {sum_if_multiple}")
    qk = dict(d=indicator)
    if method:
        qk["m"] = method
    if category:
        qk["t"] = category
    if horizon:
        qk["h"] = horizon
    if compartment:
        qk["c"] = compartment
    if subcompartment:
        qk["s"] = subcompartment

    ms = lcia_methods.get(key=qk, key_and_value=True)  # Query-Obtain the LCIA CF to be used
    indicators_weights = create_dictionary()
    for k, v in ms:
        method = k["m"]
        indic_name = f'{indicator}_{method}'
        if_name = k["i"]
        if k["h"] != "":
            indic_name += f'_{k["h"]}'
        if k["c"] != "":
            indic_name += f'_{k["c"]}'
            if_name += f'_{k["c"]}'
        if k["s"] != "":
            indic_name += f'_{k["s"]}'
            if_name += f'_{k["s"]}'
        if k["t"] != "":
            indic_name += f'_{k["t"]}'

        indic_name = get_nis_name(indic_name)
        if_name = get_nis_name(if_name)
        if indic_name in indicators_weights:
            lst = indicators_weights[indic_name]
        else:
            lst = []
            indicators_weights[indic_name] = lst

        lst.append((if_name, v[0], float(v[1])))  # Interface, TargetUnit, Weight

    ifaces = create_dictionary()
    for t in state.list_namespace_variables():
        # if not t[0].startswith("_"):
        p = t[1]  # * ureg(iface_unit)
        ifaces[t[0]] = p

    indicators_values = dict()
    for name, lst_weights in indicators_weights.items():
        if trace:
            lst_av = []
        interfaces = []
        weights = []  # From "
        involved_vars = {}
        for t in lst_weights:
            if t[0] in ifaces:
                v = ifaces[t[0]]  # TODO .to(t[1])
                if math.isnan(v):
                    involved_vars[t[0]] = "NAv"  # It is an interface but its value is not available
                else:
                    involved_vars[t[0]] = "Av"
                    interfaces.append(v)
                    weights.append(t[2])
                    if trace:
                        lst_av.append(f"{t[2]*v}\t{t[2]}\t{v}\t{t[0]}")
            else:
                # "NAp" variable (it is not an interface of the processor, so it does not apply)
                involved_vars[t[0]] = "NAp"
        # Calculate the value
        ind = np.sum(np.multiply(interfaces, weights))  # * ureg(indicator_unit)
        indicators_values[name] = ind
        if trace:
            print(f"{ind}\t\t\tIndicator: {name}")
            print("\n".join(lst_av))

    if sum_if_multiple:
        lon = len(indicators_values)
        indicators_values = sum(indicators_values.values())
        if trace:
            print(f"{indicators_values}\t\t\tSum of {lon} sub-indicators")

    return indicators_values


def starts_with(context, *args):
    """

    :return:
    """
    return args[0].startswith(args[1])


def lower_case(context, text):
    """

    :param text:
    :return:
    """
    if isinstance(text, list):
        return [x.lower() for x in text]
    elif isinstance(text, str):
        return text.lower()
    else:
        return text


def obtain_processors(xquery: str = None, processors_dom=None, processors_map=None):
    if xquery:
        try:
            processors = set()
            ns = etree.FunctionNamespace(None)
            ns["starts-with"] = starts_with
            ns["lower-case"] = lower_case
            r = processors_dom.xpath(xquery if case_sensitive else xquery.lower())
            for e in r:
                fname = e.get("fullname")
                if fname:
                    processors.add(processors_map[fname])
                else:
                    pass  # Interfaces...
            return processors
        except lxml.etree.XPathEvalError:
            traceback.print_exc()
            # TODO Try CSSSelector syntax
            # TODO Generate Issue
    else:
        # ALL
        # TODO Probably this should not be used because it will normally imply double accounting
        return set(processors_map.values())


def get_adapted_case_dataframe_filter(df, column, values):
    i_names = df.index.unique(level=column).values
    i_names_case = [_ if case_sensitive else _.lower() for _ in i_names]
    i_names_corr = dict(zip(i_names_case, i_names))
    # https://stackoverflow.com/questions/18453566/python-dictionary-get-list-of-values-for-list-of-keys
    return [i_names_corr[_] for _ in values.intersection(i_names_case)]


def obtain_subset_of_processors(processors_selector: str, serialized_model: lxml.etree._ElementTree,
                                registry: PartialRetrievalDictionary,
                                p_map: Dict[str, Processor], df: Union[List, pd.DataFrame]) -> pd.DataFrame:
    processors = obtain_processors(processors_selector, serialized_model, p_map)

    if len(p_map) == len(processors):
        processors = set()

    if isinstance(df, pd.DataFrame):
        dfs = [df]
    else:
        dfs = df

    results = []

    # Filter Processors
    if len(processors) > 0:
        # Obtain names of processor to KEEP
        processor_names = set([_.full_hierarchy_names(registry)[0] for _ in processors])
        if not case_sensitive:
            processor_names = set([_.lower() for _ in processor_names])

        p_names = get_adapted_case_dataframe_filter(df, "Processor", processor_names)
        # p_names = df.index.unique(level="Processor").values
        # p_names_case = [_ if case_sensitive else _.lower() for _ in p_names]
        # p_names_corr = dict(zip(p_names_case, p_names))
        # # https://stackoverflow.com/questions/18453566/python-dictionary-get-list-of-values-for-list-of-keys
        # p_names = [p_names_corr[_] for _ in processor_names.intersection(p_names_case)]
        # Filter dataframe to only the desired Processors

        for df_ in dfs:
            results.append(df_.query('Processor in [' + ', '.join(['"' + p + '"' for p in p_names]) + ']'))
    else:
        for df_ in dfs:
            results.append(df_)
        processors = p_map

    if isinstance(df, pd.DataFrame):
        results = results[0]

    return results, processors


# Comparison operators
opMap = {
    "<": lambda a, b: a < b,
    "<=": lambda a, b: a <= b,
    ">": lambda a, b: a > b,
    ">=": lambda a, b: a >= b,
    "==": lambda a, b: a == b,
    "=": lambda a, b: a == b,
    "!=": lambda a, b: a != b,
    "<>": lambda a, b: a != b,
}

na_ops = ("+", "-", "u+", "u-")  # Operators admitting evalution at nest level 0


def ast_evaluator(exp: Dict, state: State, obj, issue_lst, atomic_h_names=False,
                  allowed_functions=global_functions, account_nas_name: str = None,
                  nest_level=0) -> Union[Tuple[float, List[str]], Tuple[str, float, List[str]]]:
    """
    Numerically evaluate the result of the parse of "expression" rule (not valid for the other "expression" rules)

    :param exp: Dictionary representing the AST (output of "string_to_ast" function)
    :param state: "State" used to obtain variables/objects
    :param obj: An object used when evaluating hierarchical variables. simple names, functions and datasets are considered members of this object
    :param issue_lst: List in which issues have to be annotated
    :param atomic_h_names: If True, treat variable names as atomic (False processes them part by part, from left to right). Used in dataset expansion
    :param allowed_functions: Set of allowed function names (and their definitions)
    :param account_nas_name: If != "", each evaluation at top level returns 4 results: the evaluation, number of variables,
                        number of not available and number of not applicable (i.e., like zero)
    :param nest_level: Recursive level of the function. Used to allow evaluation of addends when they are NotAvailable
    :return: value (scalar EXCEPT for named parameters, which return a tuple "parameter name - parameter value"), list of unresolved variables
    """
    ret_var = None
    ret_val = None
    # TODO dict()
    #      Resolved vars will be "Av"
    #      Each unresolved var will be marked as "NAv" (not-available) or "NAp" (not-applicable, with counts as zero)
    involved_vars = dict()
    if "type" in exp:
        t = exp["type"]
        if t in ("int", "float", "str", "boolean"):  # Literals
            ret_val = exp["value"]
        elif t == "key_value_list":
            d = create_dictionary()
            for k, v in exp["parts"].items():
                d[k], tmp = ast_evaluator(v, state, obj, issue_lst, atomic_h_names, allowed_functions,
                                          account_nas_name=account_nas_name, nest_level=nest_level + 1)
                involved_vars.update(tmp)
            ret_val = d
        elif t == "named_parameter":  # Named parameters, special!
            # This one returns a tuple (parameter name, parameter value, unresolved variables)
            v, tmp = ast_evaluator(exp["value"], state, obj, issue_lst, atomic_h_names,
                                   account_nas_name=account_nas_name, nest_level=nest_level + 1)
            involved_vars.update(tmp)
            # Name of the parameter, its value, then "unresolved_vars"
            ret_var = exp["param"]
            ret_val = v
        elif t == "dataset":
            # Function parameters and Slice parameters
            func_params = [ast_evaluator(p, state, obj, issue_lst, atomic_h_names, allowed_functions,
                                         account_nas_name=account_nas_name, nest_level=nest_level + 1) for p in
                           exp["func_params"]]
            slice_params = [ast_evaluator(p, state, obj, issue_lst, atomic_h_names, allowed_functions,
                                          account_nas_name=account_nas_name, nest_level=nest_level + 1) for p in
                            exp["slice_params"]]

            # Find dataset named "exp["name"]"
            if obj is None:
                # Global dataset
                ds = state.get(exp["name"], exp["ns"])
                if not ds:
                    issue_lst.append((3, "Global dataset '" + exp["name"] + "' not found"))
            else:
                # Dataset inside "obj"
                try:
                    ds = getattr(obj, exp["name"])
                except:
                    ds = None
                if not ds:
                    issue_lst.append((3, "Dataset '" + exp["name"] + "' local to " + str(obj) + " not found"))

            involved_vars = None
            if ds and isinstance(ds, ExternalDataset):
                ret_val = ds.get_data(None, slice_params, None, None, func_params)
            else:
                ret_val = None
        elif t == "function":  # Call function
            # If it is a function (not a method), is it a valid function, is it an aggregator function?
            if obj is None:
                if exp["name"] in allowed_functions:
                    _f = allowed_functions[exp["name"]]
                    aggregate = _f.get("aggregate", False)
                else:
                    aggregate = False
                    issue_lst.append((3, "Function '" + exp["name"] + "' does not exist"))

            # First, obtain the Parameters
            args = []  # List of unnamed parameters
            kwargs = {}  # List of named parameters
            can_resolve = True
            for i, p in enumerate(exp["params"]):
                # If it is an aggregator function, first parameter not evaluated, evaluated by the aggregator function
                if aggregate and i == 0:
                    # First parameter is not evaluated, the AST is just passed to the aggregator function,
                    # which will evaluate it for every matching processor (per scenario, period)
                    args.append(p)
                else:
                    q = ast_evaluator(p, state, obj, issue_lst, atomic_h_names, allowed_functions,
                                      account_nas_name=account_nas_name, nest_level=nest_level + 1)
                    if len(q) == 3:  # Named parameter
                        kwargs[q[0]] = q[1]
                        tmp = q[2]
                    else:  # Parameter
                        args.append(q[0])
                        tmp = q[1]
                    involved_vars.update(tmp)
                    if len(tmp) > 0:
                        can_resolve = False

            if obj is None:
                # Check if it can be resolved (all variables specified)
                # Check if global function exists, then call it. There are no function namespaces (at least for now)
                if can_resolve and exp["name"] in allowed_functions:
                    _f = allowed_functions[exp["name"]]
                    mod_name, func_name = _f["full_name"].rsplit('.', 1)
                    mod = importlib.import_module(mod_name)
                    func = getattr(mod, func_name)
                    kwargs.update(_f["kwargs"])
                    for sp_kwarg, name in _f.get("special_kwargs", {}).items():
                        if sp_kwarg == "PartialRetrievalDictionary":
                            kwargs[name] = state.get("_glb_idx")
                        elif sp_kwarg == "ProcessorsDOM":
                            kwargs[name] = state.get("_processors_dom")
                        elif sp_kwarg == "ProcessorsMap":
                            kwargs[name] = state.get("_processors_map")
                        elif sp_kwarg == "DataFrameGroup":
                            kwargs[name] = state.get("_df_group")
                        elif sp_kwarg == "IndicatorDictionaries":
                            kwargs[name] = state.get("_indicators_tmp")
                        elif sp_kwarg == "IndicatorState":
                            kwargs[name] = state
                        elif sp_kwarg == "LCIAMethods":
                            kwargs[name] = state.get("_lcia_methods")
                        elif sp_kwarg == "AccountNA":
                            kwargs[name] = account_nas_name
                        elif sp_kwarg == "ProcessorNames":
                            kwargs[name] = state.get("_processor_names")

                    # CALL FUNCTION!!
                    try:
                        obj = func(*args, **kwargs)
                    except Exception as e:
                        obj = None
                        issue_lst.append(str(e))
            else:
                # CALL FUNCTION LOCAL TO THE OBJECT (a "method")
                try:
                    obj = getattr(obj, exp["name"])
                    obj = obj(*args, **kwargs)
                except Exception as e:
                    obj = None
                    issue_lst.append(str(e))
            ret_val = obj
        elif t == "h_var":
            # Evaluate in sequence
            obj = None
            _namespace = exp.get("ns", None)
            if atomic_h_names:
                h_name = '.'.join(exp["parts"])
                exp["parts"] = [h_name]

            for o in exp["parts"]:
                if isinstance(o, str):
                    # Simple name
                    if obj is None:
                        obj = state.get(o, _namespace)
                        if obj is None:
                            issue_lst.append((3, "'" + o + "' is not globally declared in namespace '" + (
                                _namespace if _namespace else "default") + "'"))
                            if _namespace:
                                involved_vars[_namespace + "::" + o] = "NAp"
                            else:
                                involved_vars[o] = "NAp"
                            if account_nas_name:
                                obj = 0  # If not defined, the value is zero (NAs is for Sums; for Products it would probably be 1...)
                        else:  # Value defined, check if it is a NAv (NaN)
                            if isinstance(obj, (float, int)):  # AccountNAs is valid only for float's
                                if math.isnan(obj):
                                    involved_vars[o] = "NAv"
                                else:
                                    if account_nas_name:
                                        involved_vars[o] = "Av"
                    else:
                        if isinstance(obj, ExternalDataset):
                            # Check if "o" is column (measure) or dimension
                            if o in obj.get_columns() or o in obj.get_dimensions():
                                obj = obj.get_data(o, None)
                            else:
                                issue_lst.append((3, "'" + o + "' is not a measure or dimension of the dataset."))
                        else:
                            try:
                                obj = getattr(obj, o)
                            except:
                                issue_lst.append((3, "'" + o + "' is not a ."))
                else:
                    # Dictionary: function call or dataset access
                    if obj is None:
                        o["ns"] = _namespace
                    obj = ast_evaluator(o, state, obj, issue_lst, atomic_h_names, allowed_functions,
                                        account_nas_name=account_nas_name, nest_level=nest_level + 1)
            if obj is None or isinstance(obj, (str, int, float, bool)):
                ret_val = obj
            # TODO elif isinstance(obj, ...) depending on core object types, invoke a default method, or
            #  issue ERROR if it is not possible to cast to something simple
            else:
                ret_val = obj
        elif t == "condition":  # Evaluate IF part to a Boolean. If True, return the evaluation of the THEN part; if False, return None
            if_result, tmp = ast_evaluator(exp["if"], state, obj, issue_lst, atomic_h_names, allowed_functions)
            involved_vars.update(tmp)
            if len(tmp) == 0 and if_result:
                then_result, tmp = ast_evaluator(exp["then"], state, obj, issue_lst,
                                                 atomic_h_names, allowed_functions,
                                                 account_nas_name=account_nas_name, nest_level=nest_level + 1)
                involved_vars.update(tmp)
                if len(tmp) > 0:
                    then_result = None
                ret_val = then_result
        elif t == "conditions":
            ret_val = None
            for c in exp["parts"]:
                cond_result, tmp = ast_evaluator(c, state, obj, issue_lst, atomic_h_names,
                                                 allowed_functions, account_nas_name=account_nas_name,
                                                 nest_level=nest_level + 1)
                involved_vars.update(tmp)
                if len(tmp) == 0 and cond_result:
                    ret_val = cond_result
                    break
        elif t == "reference":
            ret_val = "[" + exp["ref_id"] + "]"  # TODO Return a special type
        elif t in ("u+", "u-", "exponentials", "multipliers", "adders", "comparison", "not", "and",
                   "or"):  # Arithmetic and Boolean
            # Evaluate recursively the left and right operands
            locally_involved_vars = {}
            if t in ("u+", "u-"):
                current = 0
                # NOTE: Unary operators do not have "left" side. So leave "locally_involved_vars" empty
            else:
                current, tmp1 = ast_evaluator(exp["terms"][0], state, obj, issue_lst, atomic_h_names,
                                              allowed_functions, account_nas_name=account_nas_name,
                                              nest_level=nest_level + 1)
                locally_involved_vars.update(tmp1)
                involved_vars.update(tmp1)

            for i, e in enumerate(exp["terms"][1:]):
                following, tmp2 = ast_evaluator(e, state, obj, issue_lst, atomic_h_names,
                                                allowed_functions, account_nas_name=account_nas_name,
                                                nest_level=nest_level + 1)
                locally_involved_vars.update(tmp2)
                involved_vars.update(tmp2)

                # Type casting for primitive types
                # TODO For Object types, apply default conversion. If both sides are Object, assume number
                if (isinstance(current, (int, float)) and isinstance(following, (int, float))) or \
                        (isinstance(current, bool) and isinstance(following, bool)) or \
                        (isinstance(current, str) and isinstance(following, str)):
                    pass  # Do nothing
                else:  # In others cases, CAST to the operand of the left. This may result in an Exception
                    if current is not None and following is not None:
                        following = type(current)(following)

                op = exp["ops"][i].lower()
                # Either evaluation of terms was ok OR
                # NAs accounting is enabled, nest-level==0 and is an NA compatible operator
                admissible_evaluation = (len([k for k, v in locally_involved_vars.items() if v != "Av"]) == 0) or \
                                        (nest_level == 0 and op in na_ops and account_nas_name is not None)
                if admissible_evaluation:
                    if op in ("+", "-", "u+", "u-"):
                        if current is None:
                            current = 0
                        if following is None:
                            following = 0
                        if op in ("-", "u-"):
                            following = -following
                        current += following
                    elif op in ("*", "/", "//", "%", "**", "^"):
                        if following is None:
                            following = 1
                        if current is None:
                            current = 1
                        if op == "*":
                            current *= following
                        elif op == "/":
                            current /= following
                        elif op == "//":
                            current //= following
                        elif op == "%":
                            current %= following
                        elif op in ("**", "^"):
                            current ^= following
                    elif op == "not":
                        current = not bool(following)
                    elif op == "and":
                        current = current and following
                    elif op == "or":
                        current = current or following
                    else:  # Comparators
                        fn = opMap[op]
                        current = fn(current, following)
                else:
                    current = None  # Could not evaluate because there are missing variables

            if account_nas_name and nest_level == 0:
                # Calculate NAv, NAp and N
                nav = set()
                nap = set()
                av = set()
                m = {"NAv": nav, "NAp": nap, "Av": av}
                # Split in sets depending on the category
                [m[v].add(k) for k, v in involved_vars.items()]
                # Prepare return value, a dict
                current = {account_nas_name: current,
                           f"{account_nas_name}_nav": f"{len(nav)}",
                           f"{account_nas_name}_nap": f"{len(nap)}",
                           f"{account_nas_name}_n": f"{len(nav)+len(nap)+len(av)}"}
            elif len([k for k, v in locally_involved_vars.items() if v != "Av"]) > 0:
                current = None

            ret_val = current
        else:
            issue_lst.append((3, "'type' = " + t + " not supported."))
    else:
        issue_lst.append((3, "'type' not present in " + str(exp)))

    # Convert "involved_vars" returned by top level to a set of "unresolved_vars"
    if involved_vars and nest_level == 0:
        involved_vars = {k for k, v in involved_vars.items() if v != "Av"}

    if ret_var is None:
        if involved_vars is not None:
            return ret_val, involved_vars
        else:
            return ret_val  # Dataset
    else:
        return ret_var, ret_val, involved_vars  # Named parameter


def ast_evaluator_static(exp: Dict, state: State, obj, issue_lst, atomic_h_names=False,
                         allowed_functions=global_functions, account_nas_name: str = None,
                         nest_level=0) -> Union[Tuple[float, List[str]], Tuple[str, float, List[str]]]:
    """
    NOT USED
    Statically evaluate the result of the parse of "expression" rule (not valid for the other "expression" rules)
    Return True if the expression can be evaluated (explicitly mentioned variables are defined previously)

    :param exp: Dictionary representing the AST (output of "string_to_ast" function)
    :param state: "State" used to obtain variables/objects
    :param obj: An object used when evaluating hierarchical variables. simple names, functions and datasets are considered members of this object
    :param issue_lst: List in which issues have to be annotated
    :param atomic_h_names: If True, treat variable names as atomic (False processes them part by part, from left to right). Used in dataset expansion
    :param allowed_functions: Set of allowed function names (and their definitions)
    :param account_nas_name: If True, each evaluation at top level returns 4 results: the evaluation, number of variables,
                        number of not available and number of not applicable (i.e., like zero)
    :param nest_level: Recursive level of the function. Used to allow evaluation of addends when they are NotAvailable
    :return: value (scalar EXCEPT for named parameters, which return a tuple "parameter name - parameter value"), list of unresolved variables
    """
    ret_val = None
    # TODO dict()
    #      Each unresolved var will be marked as "NAv" (not-available) or "NAp" (not-applicable, with counts as zero)
    unresolved_vars = set()
    if "type" in exp:
        t = exp["type"]
        if t == "dataset":
            # Function parameters and Slice parameters
            func_params = [ast_evaluator_static(p, state, obj, issue_lst, atomic_h_names, allowed_functions,
                                                account_nas_name=account_nas_name, nest_level=nest_level + 1) for p in
                           exp["func_params"]]
            slice_params = [ast_evaluator_static(p, state, obj, issue_lst, atomic_h_names, allowed_functions,
                                                 account_nas_name=account_nas_name, nest_level=nest_level + 1) for p in
                            exp["slice_params"]]

            # Find dataset named "exp["name"]"
            if obj is None:
                # Global dataset
                ds = state.get(exp["name"], exp["ns"])
                if not ds:
                    issue_lst.append((3, "Global dataset '" + exp["name"] + "' not found"))
                else:
                    ds = True
            else:
                ds = True  # We cannot be sure it will be found, but do not break the evaluation
            # True if the Dataset is True, and the parameters are True
            return ds and all(func_params) and all(slice_params)
        elif t == "function":  # Call function
            # First, obtain the Parameters
            args = []
            kwargs = {}
            can_resolve = True
            for p in [ast_evaluator_static(p, state, obj, issue_lst, atomic_h_names, allowed_functions,
                                           account_nas_name=account_nas_name, nest_level=nest_level + 1) for p in
                      exp["params"]]:
                if len(p) == 3:
                    kwargs[p[0]] = p[1]
                    tmp = p[2]
                else:
                    args.append(p[0])
                    tmp = p[1]
                unresolved_vars.update(tmp)
                if len(tmp) > 0:
                    can_resolve = False

            if obj is None:
                # Check if global function exists, then call it. There are no function namespaces (at least for now)
                if exp["name"] in allowed_functions:
                    _f = allowed_functions[exp["name"]]
                    mod_name, func_name = _f["full_name"].rsplit('.', 1)
                    mod = importlib.import_module(mod_name)
                    func = getattr(mod, func_name)
                    # True if everything is True: function defined and all parameters are True
                    obj = func and all(args) and all(kwargs.values())
            else:
                # Call local function (a "method")
                obj = True
            return obj
        elif t in ("u+", "u-", "exponentials", "multipliers", "adders", "comparison", "not", "and",
                   "or"):  # Arithmetic and Boolean
            # Evaluate recursively the left and right operands
            if t in ("u+", "u-"):
                current = True
                tmp1 = []  # Unary operators do not have "left" side. So empty list for unresolved vars
            else:
                current, tmp1 = ast_evaluator_static(exp["terms"][0], state, obj, issue_lst, atomic_h_names,
                                                     allowed_functions, account_nas_name=account_nas_name,
                                                     nest_level=nest_level + 1)
                unresolved_vars.update(tmp1)

            for i, e in enumerate(exp["terms"][1:]):
                following, tmp2 = ast_evaluator_static(e, state, obj, issue_lst, atomic_h_names,
                                                       allowed_functions, account_nas_name=account_nas_name,
                                                       nest_level=nest_level + 1)
                unresolved_vars.update(tmp2)
                current = current and following

            if len(unresolved_vars) > 0:
                current = None

            ret_val = current
        else:
            issue_lst.append((3, "'type' = " + t + " not supported."))
    else:
        issue_lst.append((3, "'type' not present in " + str(exp)))

    return unresolved_vars


def ast_to_string(exp):
    """
    Elaborate string from expression AST

    :param exp: Input dictionary
    :return: value (scalar EXCEPT for named parameters, which return a tuple "parameter name - parameter value"
    """
    val = None
    if "type" in exp:
        t = exp["type"]
        if t in ("int", "float", "str"):
            val = str(exp["value"])
        elif t == "named_parameter":
            val = str(exp["param"] + "=" + ast_to_string(exp["value"]))
        elif t == "pf_name":
            val = str(exp["processor"] if exp["processor"] else "") + (":" + exp["factor"]) if exp["factor"] else ""
        elif t == "dataset":
            # Function parameters and Slice parameters
            func_params = [ast_to_string(p) for p in exp["func_params"]]
            slice_params = [ast_to_string(p) for p in exp["slice_params"]]

            val = exp["name"]
            if func_params:
                val += "(" + ", ".join(func_params) + ")"
            if slice_params:
                val += "[" + ", ".join(slice_params) + "]"
        elif t == "function":  # Call function
            # First, obtain the Parameters
            val = exp["name"]
            params = []
            for p in [ast_to_string(p) for p in exp["params"]]:
                if isinstance(p, tuple):
                    params.append(p[0] + "=" + p[1])
                else:
                    params.append(p)
            val += "(" + ", ".join(params) + ")"
        elif t == "h_var":
            # Evaluate in sequence
            _namespace = exp["ns"] if "ns" in exp else None
            if _namespace:
                val = _namespace + "::"
            else:
                val = ""

            parts = []
            for o in exp["parts"]:
                if isinstance(o, str):
                    parts.append(o)
                else:
                    # Dictionary: function call or dataset access
                    parts.append(ast_to_string(o))
            val += ".".join(parts)
        elif t in ("u+", "u-", "multipliers", "adders"):  # Arithmetic OPERATIONS
            # Evaluate recursively the left and right operands
            if t in "u+":
                current = ""
            elif t == "u-":
                current = "-"
            else:
                current = ast_to_string(exp["terms"][0])

            for i, e in enumerate(exp["terms"][1:]):
                following = ast_to_string(e)

                op = exp["ops"][i]
                if op in ("+", "-", "u+", "u-"):
                    if current is None:
                        current = 0
                    if following is None:
                        following = 0
                    if op in ("-", "u-"):
                        following = "-(" + following + ")"

                    current = "(" + current + ") + (" + following + ")"
                elif op in ("*", "/", "//", "%"):  # Multipliers
                    if following is None:
                        following = "1"
                    if current is None:
                        current = "1"
                    if op == "*":
                        current = "(" + current + ") * (" + following + ")"
                    elif op == "/":
                        current = "(" + current + ") / (" + following + ")"
                    elif op == "//":
                        current = "(" + current + ") // (" + following + ")"
                    elif op == "%":
                        current = "(" + current + ") % (" + following + ")"
                elif op == "not":
                    if following is None:
                        following = "True"
                    current = "Not (" + following + ")"
                else:  # And, Or, Comparators
                    if following is None:
                        following = "True"
                    if current is None:
                        current = "True"
                    current = "(" + current + ") " + op + "(" + following + ")"

            val = current

    return val


def dictionary_from_key_value_list(kvl, state: State = None):
    """
    From a string containing a list of keys and values, return a dictionary
    Keys must be literals, values can be expressions, to be evaluated at a later moment

    (syntactic validity of expressions is not checked here)

    :param kvl: String containing the list of keys and values
    :except If syntactic problems occur
    :return: A dictionary
    """
    try:
        ast = parser_field_parsers.string_to_ast(key_value_list, kvl)
    except:
        raise Exception(f"Could not parse key-value list: {key_value_list}")

    d = create_dictionary()
    for k, v in ast["parts"].items():
        issues = []
        res, unres = ast_evaluator(v, state, None, issues)
        if len(unres) == 0:
            v = res
            d[k] = v
        else:
            raise Exception(f"Could not evaluate key '{k}' in key-value list. Value: {v}")

    # pairs = kvl.split(",")
    # for p in pairs:
    #     k, v = p.split("=", maxsplit=1)
    #     if not k:
    #         raise Exception(
    #             "Each key-value pair must be separated by '=' and key has to be defined, value can be empty: " + kvl)
    #     else:
    #         try:
    #             k = k.strip()
    #             v = v.strip()
    #             string_to_ast(simple_ident, k)
    #             try:
    #                 # Simplest: string
    #                 string_to_ast(quotedString, v)
    #                 v = v[1:-1]
    #             except:
    #                 issues = []
    #                 ast = string_to_ast(expression_with_parameters, v)
    #                 res, unres = ast_evaluator(ast, state, None, issues)
    #                 if len(unres) == 0:
    #                     v = res
    #
    #             d[k] = v
    #         except:
    #             raise Exception("Key must be a string: " + k + " in key-value list: " + kvl)
    return d


# Check value domain (according to Parameter definition)
def check_parameter_value(glb_idx, p, value, issues, sheet_name, row):
    retval = True
    if p.range:
        try:  # Try "numeric interval"
            ast = string_to_ast(number_interval, p.range)
            # try Convert value to float
            ast2 = string_to_ast(expression_with_parameters, value)
            evaluation_issues: List[Tuple[int, str]] = []
            s = State()
            value, unresolved_vars = ast_evaluator(exp=ast2, state=s, obj=None, issue_lst=evaluation_issues)
            if value is not None:
                try:
                    value = float(value)
                    left = ast["left"]
                    right = ast["right"]
                    left_number = ast["number_left"]
                    right_number = ast["number_right"]
                    if left == "[":
                        value_meets_left = value >= left_number
                    else:
                        value_meets_left = value > left_number
                    if right == "]":
                        value_meets_right = value <= right_number
                    else:
                        value_meets_right = value < right_number
                    if not value_meets_left or not value_meets_right:
                        issues.append(Issue(itype=IType.ERROR,
                                            description=f"The value {value} specified for the parameter '{p.name}' is out of the range {p.range}",
                                            location=IssueLocation(sheet_name=sheet_name, row=row, column=None)))
                        retval = False
                except:
                    issues.append(Issue(itype=IType.ERROR,
                                        description=f"The parameter '{p.name}' has a non numeric value '{value}', and has been constrained with a numeric range. Please, either change the Value or the Range",
                                        location=IssueLocation(sheet_name=sheet_name, row=row, column=None)))
                    retval = False
            else:
                pass  # The parameter depends on other parameters, a valid situation

        except:  # A hierarchy name
            h = glb_idx.get(Hierarchy.partial_key(p.range))
            h = h[0]
            if value not in h.codes.keys():
                issues.append(Issue(itype=IType.ERROR,
                                    description=f"The value '{value}' specified for the parameter '{p.name}' is not in the codes of the hierarchy '{p.range}': {', '.join(h.codes.keys())}",
                                    location=IssueLocation(sheet_name=sheet_name, row=row, column=None)))
                retval = False

    return retval


if __name__ == '__main__':
    from nexinfosys.model_services import State
    from dotted.collection import DottedDict

    issues = []
    s = State()

    ast = string_to_ast(indicator_expression, 'LCIAMethod("ReCiPe Midpoint (H) V1.13", "climate change", "GWP100", trace=1)'.lower())
    val, variables = ast_evaluator(ast, s, None, issues)

    # AST_EVALUATOR APPLICATIONS
    # Dataset expansion: dataset name "." dataset field, operations, functions
    # ProcessorScalings: _get_scale_value
    # check_parameter_value (in this module)
    # back_to_nis_format:get_interfaces, to obtain Interface value
    # global_scalar_indicators
    # local_scalar_indicators
    # evaluate_numeric_expression_with_parameters
    s.set("time", 2016)
    s.set("scenario", "s3")
    c = "?time==2016 -> (?scenario=='s2' -> 4, scenario=='s1' -> 1, -1?), scenario=='s3' -> 2?"
    c = '?scenario=="government_directed" and time==2030 -> 0.9477, scenario=="market_driven" and time==2030 -> 0.9970, 0.5111?'
    c = '? scenario=="government_directed" -> (?time==2030 -> 0.9477, time==2050 -> 0.0000, 1?), scenario=="market_driven" -> (?time==2030 -> 0.9970, time==2050 -> 0.0000, 1?), scenario=="people_powered" -> (?time==2030 -> 0.5111, time==2050 -> 0.0000, 1?), 1?'
    ast = string_to_ast(arith_boolean_expression, c)
    res, unres = ast_evaluator(ast, s, None, issues)

    ast = string_to_ast(arith_boolean_expression_with_less_tokens, "200 < capacity_factor and capacity_factor < 400")
    ast2 = string_to_ast(arith_boolean_expression, "200 < capacity_factor and capacity_factor < 400")
    s = State()
    s.set("capacity_factor", 300)
    issues = []
    res2, unres2 = ast_evaluator(ast2, s, None, issues)
    res, unres = ast_evaluator(ast, s, None, issues)

    issues = []
    s = State()
    ex = "level =”N - 1”, farm_type =”GH”, efficiency = 0.3"
    ast = string_to_ast(key_value_list, ex)
    res, unres = ast_evaluator(ast, s, None, issues)
    s.set("Param1", 2.1)
    # s.set("Param", 0.1)
    s.set("Param2", 0.2)
    s.set("p1", 2.3)

    ej = "level='n+1', r=[Ref2019], a=5*p1, c=?p1>3 -> 'T1', p1<=2 -> 'T2', 'T3'?"
    ast = string_to_ast(key_value_list, ej)
    res, unres = ast_evaluator(ast, s, None, issues)

    examples = ["?Param1 > 3 -> 5, Param1 <=3 -> 2?",
                "(Param1 * 3 >= 0.3) AND (Param2 * 2 <= 0.345)",
                "cos(Param*3.1415)",
                "{Param} * 3 >= 0.3",
                "'Hola'",
                "'Hola' + 'Adios'",
                "5 * {Param1}",
                "True",
                "'Hola' + Param1"
                ]
    for e in examples:
        try:
            ast = string_to_ast(arith_boolean_expression, e)
            issues = []
            res, unres = ast_evaluator(ast, s, None, issues)
            print(e + ":: AST: " + str(ast))
            if len(unres) > 0:
                print("Unresolved variables: " + str(unres))
            else:
                print(str(type(res)) + ": " + str(res))
        except Exception as e2:
            print("Incorrect: " + e + ": " + str(e2))

    s.set("HH", DottedDict({"Power": {"p": 34.5, "Price": 2.3}}))
    s.set("EN", DottedDict({"Power": {"Price": 1.5}}))
    s.set("HH", DottedDict({"Power": 25}), "ns2")
    s.set("param1", 0.93)
    s.set("param2", 0.9)
    s.set("param3", 0.96)
    examples = [
        # "EN(p1=1.5, p2=2.3)[d1='C11', d2='C21'].v2",  # Simply sliced Variable Dataset (function call)
        "a_function(p1=2, p2='cc', p3=1.3*param3)",
        "-5+4*2",  # Simple expression #1
        "HH",  # Simple name
        "HH.Power.p",  # Hierarchic name
        "5",  # Integer
        "1.5",  # Float
        "1e-10",  # Float scientific notation
        "(5+4)*2",  # Simple expression #2 (parenthesis)
        "3*2/6",  # Simple expression #3 (consecutive operators of the same kind)
        "'hello'",  # String
        "ns2::HH.Power",  # Hierarchic name from another Namespace
        "HH.Power.Price + EN.Power.Price * param1",
        "EN[d1='C11', d2='C21'].d1",  # Simple Dataset slice
        "b.a_function(p1=2, p2='cc', p3=1.3*param3)",
        "b.EN[d1='C11', d2='C21'].d1",  # Hierachical Dataset slice
        "tns::EN(p1=1.5+param2, p2=2.3 * 0.3)[d1='C11', d2='C21'].v2",  # Simply sliced Variable Dataset (function call)
    ]
    for example in examples:
        print(example)
        res = string_to_ast(arith_boolean_expression, example)
        print(res)
        issues = []
        value, unres = ast_evaluator(res, s, None, issues)
        print(str(type(value)) + ": " + str(value) + "; unresolved: " + unres)
