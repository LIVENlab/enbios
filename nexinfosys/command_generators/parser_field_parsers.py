"""
Parsing of fields of the spreadsheet using PyParsing

Ideas for expression rules copied/adapted from:
https://gist.github.com/cynici/5865326

"""
import re
import traceback
from functools import partial

import lxml
import pyparsing
import typing
from pyparsing import (ParserElement, Regex,
                       oneOf, srange, infixNotation, ParseResults, opAssoc,
                       Forward, Regex, Suppress, Literal, Word,
                       Optional, OneOrMore, ZeroOrMore, Or, alphas, alphanums, White,
                       Combine, Group, delimitedList, nums, quotedString, NotAny,
                       removeQuotes, CaselessKeyword, OnlyOnce)
from typing import Dict, List

from nexinfosys import ureg
from nexinfosys.command_generators import global_functions, global_functions_extended, extended_dict_of_function_names

# Enable memoizing
# See: https://stackoverflow.com/questions/21370697/pyparsing-performance-and-memory-usage/21371472#21371472
ParserElement.enablePackrat()

# Number
# Variable(key=expression, key=expression)[key=expression, key=expression]
# Hierarchical variable name
# Arithmetic Expression

# FORWARD DECLARATIONS
arith_expression = Forward()
arith_boolean_expression = Forward()
arith_boolean_expression_with_less_tokens = Forward()  # Simpler version of "arith_boolean_expression"
conditions_list = Forward()
expression = Forward()  # Generic expression (boolean, string, numeric)
expression_with_parameters = Forward()  # TODO Parameter value definition. An expression potentially referring to other parameters. Boolean operators. Simulation of IF ELIF ELSE or SWITCH
hierarchy_expression = Forward()
hierarchy_expression_v2 = Forward()
geo_value = Forward()  # TODO Either a Geo or a reference to a Geo
url_parser = Forward()  # TODO
context_query = Forward()  # TODO A way to find a match between pairs of processors. A pair of Processor Selectors
domain_definition = Forward()  # TODO Domain definition. Either a Category Hierarchy name or a numeric interval (with open closed)
parameter_value = Forward()  # TODO Parameter Value. Could be "expression_with_parameters"
indicator_expression = Forward()

# TOKENS

# Separators and operators (arithmetic and boolean)
lparen, rparen, lbracket, rbracket, lcurly, rcurly, dot, equals, hash = map(Literal, "()[]{}.=#")
double_quote = Literal('"')
single_quote = Literal("'")
quote = oneOf('" ''')  # Double quotes, single quotes
signop = oneOf('+ -')
multop = oneOf('* / // %')
plusop = oneOf('+ -')
expop = oneOf('^ **')
comparisonop = oneOf("< <= > >= == != <>")
andop = CaselessKeyword("AND")
orop = CaselessKeyword("OR")
notop = CaselessKeyword("NOT")
processor_factor_separator = Literal(":")
conditions_opening = Literal("?")
conditions_closing = Literal("?")

# Boolean constants
true = CaselessKeyword("True")
false = CaselessKeyword("False")
# Simple identifier
simple_ident = Word(alphas+"_", alphanums+"_")  # Start in letter and "_", then "_" + letters + numbers
external_ds_name = Word(alphas, alphanums+"-"+"_"+".")  # Dataset names can have
list_simple_ident = delimitedList(simple_ident, ",")

interfaces_list_expression = list_simple_ident
indicators_list_expression = list_simple_ident
attributes_list_expression = list_simple_ident

# Basic data types
positive_int = Word(nums).setParseAction(lambda t: {'type': 'int', 'value': int(t[0])})
positive_float = (Combine(Word(nums) + Optional("." + Word(nums)) + Optional(oneOf("E e") + Optional(oneOf('+ -')) + Word(nums)))
                  ).setParseAction(lambda _s, l, t:
                                   {'type': 'float',
                                    'value': float(t[0])
                                    }
                                   )
signed_float = (Optional(Or([Literal("+"), Literal("-")])("sign")) + positive_float).\
    setParseAction(lambda _s, l, t: {'type': 'float',
                                     'value': t[0]['value'] if isinstance(t[0], dict) else (-t[1]['value'] if t[0] == '-' else t[1]['value'])
                                     }
                   )
boolean = Or([true, false]).setParseAction(
    lambda t: {'type': 'boolean', 'value': bool(t[0])}
)

quoted_string = quotedString(r".*")
unquoted_string = Regex(r".*")  # Anything
alphanums_string = Word(alphanums)
code_string = Word(alphanums+"_"+"-")  # For codes in Categories, Code Lists
# Not used
literal_code_string = (Literal('!').suppress()+Optional(simple_ident+dot.suppress())("hierarchy")+code_string("code"))
pair_numbers = signed_float + Literal(",").suppress() + signed_float

# References
code_string_reference = hash.suppress() + code_string + hash.suppress()

# RULES - literal ANY string
string = quotedString.setParseAction(
    lambda t: {'type': 'str', 'value': t[0][1:-1]}
)


# RULES - XQuery/XPath string - used in aggregator expressions potentially involving several processors

# RULES - Processor field string - Name of one Interface, Indicator or Attribute in a Processor
def processor_field_string(s, l, tt):
    """
    :param s:
    :param l:
    :param tt:
    :return:
    """
    # Parse s as simple_ident
    if re.match(r"[a-zA-Z_]\w*", s):
        return {'type': 'ProcessorField', 'value': s}
    else:
        raise Exception("Syntax error validating Processor field string")


processor_field_string = (Literal("p") + quoted_string).setParseAction(processor_field_string)


# RULES - unit name
def parse_action_unit_name(s, l, tt):
    try:
        ureg.parse_expression(s)
        return {"type": "unit_name", "unit": s}
    except:
        raise Exception("Unit name invalid")


unit_name = Regex(r".*").setParseAction(parse_action_unit_name)


# RULES - Simple hierarchical name
#         A literal hierarchical name - A non literal hierarchical name could be "processor_name"
simple_h_name = (simple_ident + ZeroOrMore(dot.suppress() + simple_ident))\
    .setParseAction(lambda _s, l, t: {'type': 'h_var',
                                      'parts': t.asList()
                                      }
                    )

# RULES - simple_hierarchical_name [":" simple_hierarchical_name]
factor_name = (Optional(simple_h_name.setResultsName("processor")) + Optional(Group(processor_factor_separator.suppress() + simple_h_name).setResultsName("factor"))
               ).setParseAction(lambda _s, l, t:
                                {'type': 'pf_name',
                                 'processor': t.processor if t.processor else None,
                                 'factor': t.factor[0] if t.factor else None
                                 }
                                )

# RULES - processor_name
# "h{a.c}ola{b}.sdf{c}",
# "{a.b.c}.h{c}ola{b}.sdf{c}",
# "{a}b",
# "{a}b{c}",
# "aa",
# "aa{b}aa",
# "{a}",
# NOT USED, NOW THERE IS A PREVIOUS MACRO EXPANSION for names like "{a.b}_proc", APPLIED *BEFORE* SYNTAX ANALYSIS
# item = lcurly.suppress() + simple_h_name + rcurly.suppress()
# processor_name_part = Group((item("variable") | Word(alphanums)("literal")) + ZeroOrMore(
#     item("variable") | Word(alphanums + "_")("literal")))


def parse_action_processor_name(s, l, tt, node_type="processor_name"):
    """
    Function to elaborate a node for evaluation of processor name (definition of processor name) or
    selection of processors, with variable names and wildcard (..)
    :param s:
    :param l:
    :param tt:
    :return:
    """
    variables = set()
    parts = []
    expandable = False
    for t in tt:
        _ = repr(t)
        if _.startswith("ParseResults"):
            _ = _[12:]
        st = eval(_)  # Inefficient??

        if st == ".":
            parts.append(("separator", st))
            continue
        elif st == "..":
            parts.append(("separator", st))
            expandable = True
            continue
        classif = st[1]
        for tok in st[0]:
            # Find it in literals or in variables
            if "literal" in classif and tok in classif["literal"]:
                parts.append(("literal", tok))

    expandable = False

    return dict(type=node_type, parts=parts, variables=variables, input=s, expandable=expandable, complex=expandable)


processor_name_part_separator = Literal(".")
processor_name_part = Group(simple_ident("literal"))
# processor_name = simple_h_name TODO Currently this has the syntax of a hierarchical name
processor_name = (processor_name_part("part") + ZeroOrMore(processor_name_part_separator("separator") + processor_name_part("part"))).\
    setParseAction(parse_action_processor_name)

# RULES - processor_names (note the plural)
# "..",
# "..Crop",
# "Farm..Crop",
# "Farm..",
# "..Farm..",
# ".Farm",  # Invalid
# "Farm.",  # Invalid
# "h{c}ola{b}.sdf{c}",
# "{a}.h{c}ola{b}.sdf{c}",
# "{a}b",
# "{a}b{c}",
# "aa",
# "aa{b}aa",
# "{a}",
processor_names_wildcard_separator = Literal("..")
processor_names_pre = (Or([processor_name_part, Literal("*")])("part") +
                       ZeroOrMore((processor_name_part_separator("separator")+processor_name_part("part")) |
                                  (processor_names_wildcard_separator("separator")+Optional(processor_name_part("part")))
                                  )
                       )
processor_names = ((processor_names_wildcard_separator + Optional(processor_names_pre)) | processor_names_pre
                   ).setParseAction(partial(parse_action_processor_name, node_type="processor_names"))


# RULES - context_query
# Right now, context_query would be exactly equal to "processor_names", i.e., a way to specify a set of
# processors (idea proposed by Michele)
context_query << processor_names

# RULES - domain_definition
number_interval = (Or([Literal("["), Literal("(")])("left") + signed_float("number_left") + Literal(",").suppress() + signed_float("number_right") + Or([Literal("]"), Literal(")")])("right"))\
    .setParseAction(lambda _s, l, t: {'type': 'number_interval',
                                      'left': t[0],
                                      'right': t[3],
                                      'number_left': t[1]['value'],
                                      'number_right': t[2]['value'],
                                      })
domain_definition << Or([simple_ident, number_interval])


# RULES - reference
reference = (Optional(lbracket) + simple_ident.setResultsName("ident") + Optional(rbracket)
             ).setParseAction(lambda _s, l, t: {'type': 'reference',
                                                'ref_id': t.ident
                                                }
                              )

bracketed_reference = (lbracket + simple_ident.setResultsName("ident") + rbracket
             ).setParseAction(lambda _s, l, t: {'type': 'reference',
                                                'ref_id': t.ident
                                                }
                              )


# RULES - Processors selector
def processor_selector_validation(s, l, tt):
    """
    Function to elaborate a node for evaluation of processor name (definition of processor name) or
    selection of processors, with variable names and wildcard (..)
    :param s:
    :param l:
    :param tt:
    :return:
    """
    try:
        lxml.etree.XPath(s)
        return s
    except lxml.etree.XPathSyntaxError:
        # TODO Check if it is CSS syntax
        raise Exception("Syntax error validating XPath expression")


processors_selector_expression = Regex(r".*").setParseAction(processor_selector_validation)

# RULES - namespace, parameter list, named parameters list, function call
namespace = simple_ident + Literal("::").suppress()
named_parameter = Group(simple_ident + equals.suppress() + expression).setParseAction(lambda t: {'type': 'named_parameter', 'param': t[0][0], 'value': t[0][1]})
named_parameters_list = delimitedList(named_parameter, ",")
parameters_list = delimitedList(Or([expression, named_parameter]), ",")


def func_call_action(s, l, t):
    if t[0][0] in global_functions:
        return dict(type='function', name=t[0][0], params=t[0][1:])
    else:
        raise Exception(f"Function '{t[0][0]}' not defined")


func_call = Group(simple_ident + lparen.suppress() + parameters_list + rparen.suppress()
                  ).setParseAction(func_call_action)


def func_call_action_extended_list(s, l, t):
    if t[0][0] in extended_dict_of_function_names:
        return dict(type='function', name=t[0][0], params=t[0][1:])
    else:
        raise Exception(f"Function '{t[0][0]}' not defined")


func_call_extended = Group(simple_ident + lparen.suppress() + parameters_list + rparen.suppress()
                           ).setParseAction(func_call_action_extended_list)


# RULES - key-value list
# key "=" value. Key is a simple_ident; "Value" can be an expression referring to parameters
value = Group(arith_boolean_expression).setParseAction(lambda t:
                                                       {'type': 'value',
                                                        'value': t[0]
                                                        }
                                                       )
key_value = Group(simple_ident + equals.suppress() + arith_boolean_expression).setParseAction(lambda t: {'type': 'key_value', 'key': t[0][0], 'value': t[0][1]})
key_value_list = delimitedList(key_value, ",").setParseAction(
    lambda _s, l, t: {'type': 'key_value_list',
                      'parts': {t2["key"]: t2["value"] for t2 in t}
                      }
)


# RULES - dataset variable, hierarchical var name
dataset = (simple_ident.setResultsName("ident") +
           Optional(lparen.suppress() + parameters_list + rparen.suppress()).setResultsName("func_params") +
           lbracket.suppress() + named_parameters_list.setResultsName("slice_params") + rbracket.suppress()
           ).setParseAction(lambda _s, l, t: {'type': 'dataset',
                                              'name': t.ident,
                                              'func_params': t.func_params if t.func_params else None,
                                              'slice_params': t.slice_params,
                                              }
                            )
# [ns::]dataset"."column_name
dataset_with_column = (Optional(namespace).setResultsName("namespace") +
                       Group(Or([simple_ident]) + dot.suppress() + simple_ident).setResultsName("parts")
                       ).setParseAction(lambda _s, l, t:
                                                         {'type': 'h_var',
                                                          'ns': t.namespace[0] if t.namespace else None,
                                                          'parts': t.parts.asList(),
                                                          }
                                        )
# RULES - hierarchical var name
obj_types = Or([simple_ident, func_call, dataset])
# h_name, the most complex NAME, which can be a hierarchical composition of names, function calls and datasets
h_name = (Optional(namespace).setResultsName("namespace") +
          Group(obj_types + ZeroOrMore(dot.suppress() + obj_types)).setResultsName("parts")
          # + Optional(hash + simple_ident).setResultsName("part")
          ).setParseAction(lambda _s, l, t: {'type': 'h_var',
                                             'ns': t.namespace[0] if t.namespace else None,
                                             'parts': t.parts.asList(),
                                             }
                           )

# RULES - Arithmetic expression AND Arithmetic Plus Boolean expression
arith_expression << infixNotation(Or([positive_float, positive_int, string, code_string_reference,
                                       simple_h_name,
                                       func_call]),  # Operand types (REMOVED h_name: no "namespace" and no "datasets")
                                 [(signop, 1, opAssoc.RIGHT, lambda _s, l, t: {
                                     'type': 'u'+t.asList()[0][0],
                                     'terms': [0, t.asList()[0][1]],
                                     'ops': ['u'+t.asList()[0][0]]
                                  }),
                                  (expop, 2, opAssoc.LEFT, lambda _s, l, t: {
                                      'type': 'exponentials',
                                      'terms': t.asList()[0][0::2],
                                      'ops': t.asList()[0][1::2]
                                  }),
                                  (multop, 2, opAssoc.LEFT, lambda _s, l, t: {
                                      'type': 'multipliers',
                                      'terms': t.asList()[0][0::2],
                                      'ops': t.asList()[0][1::2]
                                  }),
                                  (plusop, 2, opAssoc.LEFT, lambda _s, l, t: {
                                      'type': 'adders',
                                      'terms': t.asList()[0][0::2],
                                      'ops': t.asList()[0][1::2]
                                  }),
                                  ],
                                 lpar=lparen.suppress(),
                                 rpar=rparen.suppress())

# "The" expression, the most complex rule of this file
arith_boolean_expression << infixNotation(Or([positive_float, positive_int,
                                                   string, boolean, code_string_reference,
                                                   bracketed_reference,
                                                   conditions_list,
                                                   simple_h_name,
                                                   func_call]),
                                 [(signop, 1, opAssoc.RIGHT, lambda _s, l, t: {
                                     'type': 'u'+t.asList()[0][0],
                                     'terms': [0, t.asList()[0][1]],
                                     'ops': ['u'+t.asList()[0][0]]
                                  }),
                                  (expop, 2, opAssoc.LEFT, lambda _s, l, t: {
                                      'type': 'exponentials',
                                      'terms': t.asList()[0][0::2],
                                      'ops': t.asList()[0][1::2]
                                  }),
                                  (multop, 2, opAssoc.LEFT, lambda _s, l, t: {
                                      'type': 'multipliers',
                                      'terms': t.asList()[0][0::2],
                                      'ops': t.asList()[0][1::2]
                                  }),
                                  (plusop, 2, opAssoc.LEFT, lambda _s, l, t: {
                                      'type': 'adders',
                                      'terms': t.asList()[0][0::2],
                                      'ops': t.asList()[0][1::2]
                                  }),
                                  (comparisonop, 2, opAssoc.LEFT, lambda _s, l, t: {
                                      'type': 'comparison',
                                      'terms': t.asList()[0][0::2],
                                      'ops': t.asList()[0][1::2]
                                  }),
                                  (notop, 1, opAssoc.RIGHT, lambda _s, l, t: {
                                      'type': 'not',
                                      'terms': [0, t.asList()[0][1]],
                                      'ops': ['u'+t.asList()[0][0]]
                                  }),
                                  (andop, 2, opAssoc.LEFT, lambda _s, l, t: {
                                      'type': 'and',
                                      'terms': t.asList()[0][0::2],
                                      'ops': t.asList()[0][1::2]
                                  }),
                                  (orop, 2, opAssoc.LEFT, lambda _s, l, t: {
                                      'type': 'or',
                                      'terms': t.asList()[0][0::2],
                                      'ops': t.asList()[0][1::2]
                                  }),
                                  ],
                                 lpar=lparen.suppress(),
                                 rpar=rparen.suppress())

# RULES - Expression varying value depending on conditions
# If a condition omits the condition itself, "True" is assumed, and the result of the expression is returned. So it is recommended for the last in the list
condition = Or([Group(arith_boolean_expression + Literal("->").suppress() + arith_boolean_expression), arith_boolean_expression])
conditions_list << Group(conditions_opening.suppress() + delimitedList(condition, ",") + conditions_closing.suppress()).setParseAction(lambda _s, l, t:
                                                               {
                                                                   'type': 'conditions',
                                                                   'parts': [{"type": "condition", "if": c[0], "then": c[1]} if isinstance(c, list) else
                                                                             {"type": "condition", "if": {'type': 'boolean', 'value': True}, "then": c}
                                                                             for c in t.asList()[0]
                                                                             ]
                                                               })

# RULES - Expression type 2
expression << infixNotation(Or([positive_float, positive_int, string, h_name]),  # Operand types
                                 [(signop, 1, opAssoc.RIGHT, lambda _s, l, t: {'type': 'u'+t.asList()[0][0], 'terms': [0, t.asList()[0][1]], 'ops': ['u'+t.asList()[0][0]]}),
                                  (expop, 2, opAssoc.LEFT, lambda _s, l, t: { 'type': 'exponentials', 'terms': t.asList()[0][0::2], 'ops': t.asList()[0][1::2]}),
                                  (multop, 2, opAssoc.LEFT, lambda _s, l, t: {'type': 'multipliers', 'terms': t.asList()[0][0::2], 'ops': t.asList()[0][1::2]}),
                                  (plusop, 2, opAssoc.LEFT, lambda _s, l, t: {'type': 'adders', 'terms': t.asList()[0][0::2], 'ops': t.asList()[0][1::2]}),
                                  ],
                                 lpar=lparen.suppress(),
                                 rpar=rparen.suppress())

# RULES - Slightly simpler version of "arith_boolean_expression"
simple_h_name_1 = Group(simple_ident)\
    .setParseAction(lambda _s, l, t: {'type': 'h_var',
                                      'parts': t.asList()[0]
                                      }
                    )

arith_boolean_expression_with_less_tokens << infixNotation(Or([positive_float,
                                                                    positive_int,
                                                                    boolean,
                                                                    string,
                                                                    simple_h_name_1,
                                                                    func_call]),
                                                                [(signop, 1, opAssoc.RIGHT, lambda _s, l, t: {
                                     'type': 'u'+t.asList()[0][0],
                                     'terms': [0, t.asList()[0][1]],
                                     'ops': ['u'+t.asList()[0][0]]
                                  }),
                                  (expop, 2, opAssoc.LEFT, lambda _s, l, t: {
                                      'type': 'exponentials',
                                      'terms': t.asList()[0][0::2],
                                      'ops': t.asList()[0][1::2]
                                  }),
                                  (multop, 2, opAssoc.LEFT, lambda _s, l, t: {
                                      'type': 'multipliers',
                                      'terms': t.asList()[0][0::2],
                                      'ops': t.asList()[0][1::2]
                                  }),
                                  (plusop, 2, opAssoc.LEFT, lambda _s, l, t: {
                                      'type': 'adders',
                                      'terms': t.asList()[0][0::2],
                                      'ops': t.asList()[0][1::2]
                                  }),
                                  (comparisonop, 2, opAssoc.LEFT, lambda _s, l, t: {
                                      'type': 'comparison',
                                      'terms': t.asList()[0][0::2],
                                      'ops': t.asList()[0][1::2]
                                  }),
                                  (notop, 1, opAssoc.RIGHT, lambda _s, l, t: {
                                      'type': 'not',
                                      'terms': [0, t.asList()[0][1]],
                                      'ops': ['u'+t.asList()[0][0]]
                                  }),
                                  (andop, 2, opAssoc.LEFT, lambda _s, l, t: {
                                      'type': 'and',
                                      'terms': t.asList()[0][0::2],
                                      'ops': t.asList()[0][1::2]
                                  }),
                                  (orop, 2, opAssoc.LEFT, lambda _s, l, t: {
                                      'type': 'or',
                                      'terms': t.asList()[0][0::2],
                                      'ops': t.asList()[0][1::2]
                                  }),
                                  ],
                                                                lpar=lparen.suppress(),
                                                                rpar=rparen.suppress())

# RULES - Expression type 2. Can mention only parameters and numbers
# (for parameters, namespaces are allowed, and also hierarchical naming)
# TODO Check if it can be evaluated with "ast_evaluator"
expression_with_parameters = arith_boolean_expression
# expression_with_parameters << operatorPrecedence(Or([positive_float, positive_int, simple_h_name]),  # Operand types
#                                                  [(signop, 1, opAssoc.RIGHT, lambda _s, l, t: {'type': 'u'+t.asList()[0][0], 'terms': [0, t.asList()[0][1]], 'ops': ['u'+t.asList()[0][0]]}),
#                                                   (multop, 2, opAssoc.LEFT, lambda _s, l, t: {'type': 'multipliers', 'terms': t.asList()[0][0::2], 'ops': t.asList()[0][1::2]}),
#                                                   (plusop, 2, opAssoc.LEFT, lambda _s, l, t: {'type': 'adders', 'terms': t.asList()[0][0::2], 'ops': t.asList()[0][1::2]}),
#                                                   ],
#                                                  lpar=lparen.suppress(),
#                                                  rpar=rparen.suppress())

expression_with_parameters_or_list_simple_ident = Or([arith_boolean_expression, list_simple_ident])

# RULES: Expression type 3. For hierarchies. Can mention only simple identifiers (codes) and numbers
hierarchy_expression << infixNotation(Or([positive_float, positive_int, simple_ident]),  # Operand types
                                           [(signop, 1, opAssoc.RIGHT, lambda _s, l, t: {'type': 'u'+t.asList()[0][0], 'terms': [0, t.asList()[0][1]], 'ops': ['u'+t.asList()[0][0]]}),
                                            (expop, 2, opAssoc.LEFT, lambda _s, l, t: {'type': 'exponentials', 'terms': t.asList()[0][0::2], 'ops': t.asList()[0][1::2]}),
                                            (multop, 2, opAssoc.LEFT, lambda _s, l, t: {'type': 'multipliers', 'terms': t.asList()[0][0::2], 'ops': t.asList()[0][1::2]}),
                                            (plusop, 2, opAssoc.LEFT, lambda _s, l, t: {'type': 'adders', 'terms': t.asList()[0][0::2], 'ops': t.asList()[0][1::2]}),
                                            ],
                                           lpar=lparen.suppress(),

                                           rpar=rparen.suppress())

# RULES: Expression type 4. For indicators. Can mention only numbers and core concepts
indicator_expression << infixNotation(Or([positive_float, positive_int,
                                               string, boolean,
                                               conditions_list,
                                               simple_h_name,
                                               func_call_extended]),  # Operand types
                                 [(signop, 1, opAssoc.RIGHT, lambda _s, l, t: {
                                     'type': 'u'+t.asList()[0][0],
                                     'terms': [0, t.asList()[0][1]],
                                     'ops': ['u'+t.asList()[0][0]]
                                  }),
                                  (expop, 2, opAssoc.LEFT, lambda _s, l, t: {
                                      'type': 'exponentials',
                                      'terms': t.asList()[0][0::2],
                                      'ops': t.asList()[0][1::2]
                                  }),
                                  (multop, 2, opAssoc.LEFT, lambda _s, l, t: {
                                      'type': 'multipliers',
                                      'terms': t.asList()[0][0::2],
                                      'ops': t.asList()[0][1::2]
                                  }),
                                  (plusop, 2, opAssoc.LEFT, lambda _s, l, t: {
                                      'type': 'adders',
                                      'terms': t.asList()[0][0::2],
                                      'ops': t.asList()[0][1::2]
                                  }),
                                  (comparisonop, 2, opAssoc.LEFT, lambda _s, l, t: {
                                      'type': 'comparison',
                                      'terms': t.asList()[0][0::2],
                                      'ops': t.asList()[0][1::2]
                                  }),
                                  (notop, 1, opAssoc.RIGHT, lambda _s, l, t: {
                                      'type': 'not',
                                      'terms': [0, t.asList()[0][1]],
                                      'ops': ['u'+t.asList()[0][0]]
                                  }),
                                  (andop, 2, opAssoc.LEFT, lambda _s, l, t: {
                                      'type': 'and',
                                      'terms': t.asList()[0][0::2],
                                      'ops': t.asList()[0][1::2]
                                  }),
                                  (orop, 2, opAssoc.LEFT, lambda _s, l, t: {
                                      'type': 'or',
                                      'terms': t.asList()[0][0::2],
                                      'ops': t.asList()[0][1::2]
                                  }),
                                  ],
                                 lpar=lparen.suppress(),
                                 rpar=rparen.suppress())

# [expression2 (previously parsed)] [relation_operator] processor_or_factor_name
relation_operators = Or([Literal('|'),  # Part-of
                         Literal('>'), Literal('<'),  # Directed flow
                         Literal('<>'), Literal('><'),  # Undirected flow
                         Literal('||')  # Upscale (implies part-of also)
                         ]
                        )
relation_expression = (Optional(arith_expression).setResultsName("weight") +
                       Optional(relation_operators).setResultsName("relation_type") +
                       factor_name.setResultsName("destination")
                       ).setParseAction(lambda _s, l, t: {'type': 'relation',
                                                          'name': t.destination,
                                                          'relation_type': t.relation_type,
                                                          'weight': t.weight
                                                          }
                                        )

# RULES: Expression type 5. For hierarchies (version 2). Can mention code_string (for codes), parameters and numbers
hierarchy_expression_v2 << infixNotation(Or([positive_float, positive_int, code_string, named_parameter]),  # Operand types
                                              [(signop, 1, opAssoc.RIGHT, lambda _s, l, t: {'type': 'u'+t.asList()[0][0], 'terms': [0, t.asList()[0][1]], 'ops': ['u'+t.asList()[0][0]]}),
                                            (multop, 2, opAssoc.LEFT, lambda _s, l, t: {'type': 'multipliers', 'terms': t.asList()[0][0::2], 'ops': t.asList()[0][1::2]}),
                                            (plusop, 2, opAssoc.LEFT, lambda _s, l, t: {'type': 'adders', 'terms': t.asList()[0][0::2], 'ops': t.asList()[0][1::2]}),
                                            ],
                                              lpar=lparen.suppress(),
                                              rpar=rparen.suppress())

# RULES: Level name
level_name = (simple_ident + Optional(plusop + positive_int)
              ).setParseAction(lambda t: {'type': 'level',
                                          'domain': t[0],
                                          'level': (t[1][0]+str(t[2]["value"])) if len(t) > 1 and t[1] else None
                                          }
                               )

# RULES: Time expression
# A valid time specification. Possibilities: Year, Month-Year / Year-Month, Time span (two dates)
period_name = Or([Literal("Year"), Literal("Semester"), Literal("Quarter"), Literal("Month")])
four_digits_year = Combine(Word(nums, min=4, max=4)+Optional(Literal(".0")).suppress())
month = Word(nums, min=1, max=2)
year_month_separator = oneOf("- /")
date = Group(Or([four_digits_year.setResultsName("y") +
                 Optional(year_month_separator.suppress()+month.setResultsName("m")),
                 Optional(month.setResultsName("m") + year_month_separator.suppress()) +
                 four_digits_year.setResultsName("y")
                 ]
                )
             )
date_month = Or([four_digits_year + year_month_separator + month, month + year_month_separator + four_digits_year])
two_dates_separator = oneOf("- /")
time_expression = Or([(date + Optional(two_dates_separator.suppress()+date)
                       ).setParseAction(
                                       lambda _s, l, t:
                                       {'type': 'time',
                                        'dates': [{k: int(v) for k, v in d.items()} for d in t]
                                        }
                                       ),
                      period_name.setParseAction(lambda _s, l, t:
                                                 {'type': 'time',
                                                  'period': t[0]})
                      ])

# RULES: "Relative to"
factor_unit = (simple_h_name.setResultsName("factor") + Optional(Regex(".*").setResultsName("unparsed_unit"))).setParseAction(lambda _s, l, t:
                                                                            {'type': 'factor_unit',
                                                                             'factor': t.factor,
                                                                             'unparsed_unit': t.unparsed_unit if t.unparsed_unit else ""})

# #### URL Parser ################################################################

url_chars = alphanums + '-_.~%+'
# "fragment" departs from standard URLs to allow WHITESPACEs (more convenient for users)
fragment = Combine((Suppress('#') + Word(url_chars+" ")))('fragment')
scheme = oneOf('http https ftp file data')('scheme')
host = Combine(delimitedList(Word(url_chars), '.'))('host')
port = Suppress(':') + Word(nums)('port')
user_info = (
Word(url_chars)('username')
  + Suppress(':')
  + Word(url_chars)('password')
  + Suppress('@')
)

query_pair = Group(Word(url_chars) + Suppress('=') + Word(url_chars))
query = Group(Suppress('?') + delimitedList(query_pair, '&'))('query')

path = Combine(
  Suppress('/')
  + OneOrMore(~query + Word(url_chars + '/'))
)('path')

url_parser = (
  scheme.setResultsName("scheme")
  + Suppress('://')
  + Optional(user_info).setResultsName("user_info")
  + Optional(host).setResultsName("host")
  + Optional(port).setResultsName("port")
  + Optional(path).setResultsName("path")
  + Optional(query).setResultsName("query")
  + Optional(fragment).setResultsName("fragment")
).setParseAction(lambda _s, l, t: {'type': 'url',
                                   'scheme': t.scheme,
                                   'user_info': t.user_info,
                                   'host': t.host,
                                   'port': t.port,
                                   'path': t.path,
                                   'query': t.query,
                                   'fragment': t.fragment})


# #################################################################################################################### #


def string_to_ast(rule: ParserElement, input_: str) -> Dict:
    """
    Convert the input string "input_" into an AST, according to "rule"

    :param rule:
    :param input_:
    :return: a dictionary conforming the AST (the format changes from rule to rule)
    """

    def clean_str(us):
        # "En dash"                 character is replaced by minus (-)
        # "Left/Right double quote" character is replaced by double quote (")
        # "Left/Right single quote" character is replaced by single quote (')
        # "€"                       character is replaced by "eur"
        # "$"                       character is replaced by "usd"
        return us.replace(u'\u2013', '-'). \
            replace(u'\u201d', '"').replace(u'\u201c', '"'). \
            replace(u'\u2018', "'").replace(u'\u2019', "'"). \
            replace('€', 'eur'). \
            replace('$', 'usd')

    if rule == unquoted_string:
        return input_
    else:
        res = rule.parseString(clean_str(input_), parseAll=True)
        res = res.asList()[0]
        while isinstance(res, list):
            res = res[0]
        return res


def is_year(s: str) -> bool:
    try:
        four_digits_year.parseString(s, parseAll=True)
    except pyparsing.ParseException:
        return False
    return True


def is_month(s: str) -> bool:
    try:
        date_month.parseString(s, parseAll=True)
    except pyparsing.ParseException:
        return False
    return True


def parse_string_as_simple_ident_list(in_list: str) -> typing.Optional[List[str]]:
    if in_list:
        try:
            res = list_simple_ident.parseString(in_list, parseAll=True)
            if res:
                return res.asList()
        except pyparsing.ParseException:
            pass
    return None


if __name__ == '__main__':
    from nexinfosys.model_services import State
    from dotted.collection import DottedDict

    number_interval_examples = [
        "(1, 2)",
        "[0.1, 1.0]",
        "[-3.14, 1)",
        "(-1, 3]"
    ]

    for e in number_interval_examples:
        try:
            ast = string_to_ast(number_interval, e)
            print(ast)
        except:
            print("Incorrect")

    processor_name_examples = [
        "BFGas_DE_2016",
        "BFGas_DE_2016.b"
    ]
    for e in processor_name_examples:
        try:
            ast = string_to_ast(processor_name, e)
            print(ast)
        except:
            print("Incorrect")

    examples = ["?a > 5 -> 3, a> 10 -> 6, 8?",
                "a * 3 >= 0.3",
                "'Hola'",
                "'Hola' + 'Adios'",
                "Param * 3 >= 0.3",
                "5 * Param1",
                "True",
                "(Param * 3 >= 0.3) AND (Param2 * 2 <= 0.345)",
                "cos(Param*3.1415)",
                "'Hola' + Param1"
    ]
    for e in examples:
        try:
            ast = string_to_ast(arith_boolean_expression, e)
            print(ast)
        except:
            traceback.print_exc()
            print("Incorrect")

    s = "c1 + c30 - c2 - 10"
    res = string_to_ast(hierarchy_expression, s)
    s = "ds.col"
    res = string_to_ast(h_name, s)

    s = State()
    examples = [
        "5-2017",
        "2017-5",
        "2017/5",
        "2017-05 - 2018-01",
        "2017",
        "5-2017 - 2018-1",
        "2017-2018",
        "Year",
        "Month"
    ]
    for example in examples:
        print(example)
        res = string_to_ast(time_expression, example)
        print(res)
        print(f'Is year = {is_year(example)}')
        print(f'Is month = {is_month(example)}')
        print("-------------------")

    for list1 in ["this, is, a,simple_ident , list", "this,is,'NOT', a,simple_ident , list", " word ", "", " , ", None]:
        parsed_list = parse_string_as_simple_ident_list(list1)
        print(f'Parsing list "{list1}": {parsed_list}')

    s.set("HH", DottedDict({"Power": {"p": 34.5, "Price": 2.3}}))
    s.set("EN", DottedDict({"Power": {"Price": 1.5}}))
    s.set("HH", DottedDict({"Power": 25}), "ns2")
    s.set("param1", 0.93)
    s.set("param2", 0.9)
    s.set("param3", 0.96)
    examples = [
        "EN(p1=1.5, p2=2.3)[d1='C11', d2='C21'].v2",  # Simply sliced Variable Dataset (function call)
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
    # for example in examples:
    #     print(example)
    #     res = string_to_ast(expression, example)
    #     print(res)
    #     issues = []
    #     value = ast_evaluator(res, s, None, issues)
    #     print(str(type(value))+": "+str(value))

