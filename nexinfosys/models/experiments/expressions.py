# -*- coding: utf-8 -*-

from nexinfosys.models.musiasem_concepts import *


class Variable:
    """ Used by ExpressionsEvaluationEngine to represent variables or parameters """
    def __init__(self, name):
        self._name = name
        self._type = None  # Parameter, factor, taxon, factortype, indicator
        self._origin = None  # The original object (Parameter, Factor, Taxon, FactorType, Indicator)
        self._values = []  # List of tuples (Literal, Expression). Expression refers to the one that caused obtaining the value, if None it is a "given", using
        # List of expressions where the variable appears. Each element is a tuple (expression, left|right).
        # "left"  if the variable appears to the left
        # "right" if the variable appears to the right
        self._expressions = []

    @property
    def values(self):
        return self._values

    def values_append(self, f: float, e: "Expression"=None):
        self._values.append((f, e))

    def values_set(self, f: float, e: "Expression"=None):
        self._values = [(f, e)]

    @property
    def expressions(self):
        return self._expressions

    @property
    def origin(self):
        return self._origin

    @origin.setter
    def origin(self, o):
        self._origin = o


class Expression:
    """ Used by ExpressionsEvaluationEngine to represent expressions """
    def __init__(self, e: str, expression: dict):
        self._expression = expression
        if isinstance(e, str):
            self._expression_string = e
        else:
            self._expression_string = None
        self._origin = None  # The original object (QualifiedQuantityExpression, Factor, FactorType, Taxon, TODO: Mapping)
        self._vars = None  # Variables mentioned in the expression

    @property
    def expression(self):
        return self._expression

    @property
    def origin(self):
        return self._origin

    @property
    def vars(self):
        return self._vars

    @vars.setter
    def vars(self, v):
        self._vars = v

    @origin.setter
    def origin(self, o):
        self._origin = o

    # ############################################################################################################ #
    @staticmethod
    def expressions_from_entity(entity, workspace=None) -> Union[List[str], List[dict]]:
        """ Elaborates a list of expressions in string or direct dictionary format """
        def get_factor_name(f: Factor):
            return f.processor.name+"."+f.name

        def add_taxon_hierarchy(h: Hierarchy) -> List[Expression]:
            """ Traverse the hierarchy and generate expressions. The expression is always:
                parent node = SUM children nodes. The sum may be weighted. The expression could be also given for the node
                if the node is a leaf node. The case for a parent node with a given expression should be studied, if two
                expressions should be generated or if
            """
            return []

        lst = []
        if isinstance(entity, FactorQuantitativeObservation):
            lst.append( get_factor_name(entity.factor) + "=" + entity.expression )
        elif isinstance(entity, Hierarchy):
            # TODO Find connections from and connections to
            # TODO Check that the hierarchy is of Taxon or FactorType
            lst = add_taxon_hierarchy(entity)
        elif isinstance(entity, Indicator):
            # TODO This will probably require functions to select a variable number of processors
            pass
        elif isinstance(entity, dict): # Direct expression
            lst.append(entity)

        return lst


class ExpressionsEngine:
    """ The procedure to evaluate is:
        1) Call one or more times "add_expression"
          Internally it calls "parse" and "find_vars"
          Prepare a dependency graph. Both vars and expressions are nodes, undirected arcs mean that a node conditions
          or is conditioned by other
        2) Call one or more times "add_variable_value"
          Appends
        3) Call "solve"
          Prepare a set pointing to ALL the expressions
          For each expression in the set,
            Call "eval"
            If the result is a variable with a solution, remove the expression from the list and 
              process the expressions in which the variable appears  
           
        4) "reset" to use the same expressions
           "change_variable_value" to 
    """
    def __init__(self, workspace=None):
        self._workspace = workspace  # To retrieve variables by their name
        self._variables = create_dictionary()  # type: Dictionary
        self._expressions = []
        # Must be set before internally calling "_eval"
        self._current_evaluated_expression = None  # type: Expression

    def reset(self):
        self.reset_expressions()
        self.reset_variables()

    def reset_expressions(self):
        self._expressions = []

    def reset_variables(self):
        self._variables = create_dictionary()

    def reset_solution_values(self):
        for k, v in self._variables.items():
            # If the value came from direct input, keep it
            # If the value came from the evaluation of expressions, remove it
            v._values = [val for val in v._values if not val[1]]

    def append_expressions(self, origin):
        """ Create the expression, add the appropriate records, and analyze it """
        # First, convert the object to a list of expression strings
        lst = Expression.expressions_from_entity(origin)
        for e in lst:
            # Parse the expression
            e_json = ExpressionsEngine._parse(e)
            # Create the Expression object
            expression = Expression(e, e_json)
            # Extract the variables appearing in the expression
            vars = []
            for v in ExpressionsEngine._find_vars(e_json):
                if v not in self._variables:
                    va = Variable(v)
                    va._origin = expression.origin
                    self._variables[v] = va
                else:
                    va = self._variables[v]
                # Make the variable point to the expression
                va.expressions.append(expression)
                # Make the expression point to the variable
                vars.append(va)
            # Add Expression to the list of expressions
            expression.origin = origin
            expression.vars = vars
            self._expressions.append(expression)

    def append_variable_value(self, n: str, f: float):
        if n in self._variables:
            self._variables[n].values_append(f)  # Given value. No Expression
        else:
            raise Exception("Variable '"+n+"' not found when trying to append a value to it")

    def set_variable_value(self, n: str, f: float):
        if n in self._variables:
            self._variables[n] = [(f, None)]
        else:
            raise Exception("Variable '"+n+"' not found when trying to set its value")

    @property
    def variables(self):
        return self._variables

    @staticmethod
    def _parse(exp_str):
        """ This needs the definition of a syntax, generation of syntactic and lexical rules, use of a component
            considering these rules to generate the JSON used by EVAL
        """
        if isinstance(exp_str, dict):
            j = exp_str  # Assume it is already a dictionary
        else:
            try:
                j = json.loads(exp_str)
            except Exception:
                # TODO Parse normally... "None" if some error ocurred (and an exception pointing to the problem)
                j = None

        return j

    @staticmethod
    def _find_vars(exp_json):
        """ Scan the expression to find variables, so they can be indexed """

        def find_vars_recurr(curr_json):
            if "lhs" in exp_json and "rhs" in curr_json:  # Equation
                find_vars_recurr(exp_json["lhs"])
                find_vars_recurr(exp_json["rhs"])
            elif "op" in curr_json and "oper" in curr_json:  # Arithmetic operations
                for i in curr_json["oper"]:
                    find_vars_recurr(i)
            elif "v" in curr_json:  # A variable
                # Take variable name
                # TODO Maybe standardize variable name. The same for evaluation
                vars_set.add(curr_json["v"])

        vars_set = set()
        find_vars_recurr(exp_json)
        return vars_set

    def _eval(self, exp_json):
        """ Evaluate a dictionary JSON containing the already parsed expression.
            It can return a value if all variables are known (needs access to a registry),
            True or False if it is an equation with no unknowns (it would be a comparison),
            a Variable if the expression solves to the value of Variable, or None if there is more than one unknown """
        if "lhs" in exp_json and "rhs" in exp_json:  # Equation
            lhs = self._eval(exp_json["lhs"])
            rhs = self._eval(exp_json["rhs"])
            n = (1 if isinstance(lhs, Variable) else 0) + (1 if isinstance(rhs, Variable) else 0)
            if n == 1:  # Only one Variable
                if isinstance(lhs, Variable):
                    v = lhs
                    q = rhs
                else:
                    v = rhs
                    q = lhs
                if q:
                    if isinstance(q, (int, float, ureg.Quantity)):
                        # Assign q to the Variable "v"
                        v.values_append(q, self._current_evaluated_expression)
                        return v  # <<<<<<<<<
                    else:
                        raise Exception("Unsupported type '"+str(type(q)))
                else:  # If the result is None, the Variable cannot be solved, the Expression is not evaluable
                    return None
            elif n == 2:  # Two variables
                # Return None, because both variables have no value
                return None
            else: # 0
                if type(lhs) == type(rhs):
                    return lhs == rhs
                else:
                    raise Exception("Incorrect expression. ??: "+str(self._current_evaluated_expression.expression))

        elif "op" in exp_json and "oper" in exp_json:  # Arithmetic operations
            # TODO When many observations appear, here is where combinatorial explosion can appear
            # TODO For instance, instead of just multiplying single multiplicands, assume each
            # TODO multiplicand can be a list of numbers. "itertools.product" could be used for this
            r_lst = [self._eval(i) for i in exp_json["oper"]]
            op = exp_json["op"]
            if op in ("*", "/"):
                acum = 1
            elif op in ("+", "-"):
                acum = 0

            for r in r_lst:
                if r:
                    if isinstance(r, (int, float, ureg.Quantity)):
                        if op == "*":
                            acum *= r
                        elif op == "/":
                            acum /= r
                        elif op == "+":
                            acum += r
                        elif op == "-":
                            acum -= r
                    elif isinstance(r, Variable): # Having a Variable here means we have an unknown, which is a stopper for the cascade solver
                        return None
                    else: # TODO Not controlled conditions also return None
                        return None
                else:  # TODO Not controlled conditions, return None
                    return None
            return acum
        elif "v" in exp_json:  # A variable. It can be anything: factor observation, parameter, ¿indicator?
            # Obtain the variable object and check if the variable has a value
            v = self._variables[exp_json["v"]]
            if v:
                if len(v.values) > 0:
                    return v.values[0][0]  # TODO Only one solution for now. Consider how to process several solutions. ¿"itertools.product"?
                else:
                    return v
            else:
                raise Exception("Variable '"+exp_json("v")+"' not found. What happened?")
        elif "n" in exp_json:  # A quantity
            # Because we are evaluating numerically, for now use only the number and the unit
            # TODO In the future, the uncertainty could also be used
            q = exp_json["n"]
            if "u" in exp_json:
                # Check validity of the unit with Pint
                try:
                    u = ureg(exp_json["u"])
                except pint.errors.UndefinedUnitError:
                    # The user should know that the specified unit is not recognized
                    raise Exception("The specified unit '" + exp_json["u"] + "' is not recognized")
                q *= u
            else:
                q *= ureg("dimensionless")
            return q
        elif "f" in exp_json:  # A function call. A world of options...
            return None

    @staticmethod
    def generate_expression(exp_json):
        """ Generate an expression string from JSON. It involves generating proper variable names """
        pass

    @staticmethod
    def generate_expression_for_sympy(exp_json):
        # TODO Rename variables using Sympy valid names
        # TODO Keep a map of these renames, from Sympy to MuSIASEM name and also vice versa
        # TODO The first is needed to fill results back after solving
        # TODO The second to assign already known variable values
        # TODO When renaming try to keep as much as possible the original name, so the generated
        # TODO equations can be readable
        pass

    @staticmethod
    def is_right_side(exp_json):
        pass

    def cascade_solver(self):
        """ Simple solver based on cascading of already known variables.
          Prepare a set pointing to ALL the expressions
          For each expression in the set,
            Call "eval"
            If the result is a variable with a solution, remove the expression from the list and
              process the expressions in which the variable appears

        """
        def evaluate(e: Expression):
            nonlocal some_change
            self._current_evaluated_expression = e
            r = self._eval(e.expression)
            if r and isinstance(r, Variable):  # A Variable has value and the Expression is solved closed
                some_change = True
                # Do not evaluate it more
                open_expressions.remove(e)
                # Remove the expression from the variables pointing to it
                for v in e.vars:
                    v.expressions.remove(e)
                # Now, evaluate the expressions related to the new found Variable
                for e in r.expressions:
                    evaluate(e)
            if e in pending_expressions:
                pending_expressions.remove(e)

        # Set of expressions where things can change
        open_expressions = set(self._expressions)
        # Reset all values and states. Except parameters.
        self.reset_solution_values()
        # Sweep expressions and repeat while there are new
        some_change = True
        while some_change:
            some_change = False
            # Set of expressions inside open_expressions which have not been iterated
            pending_expressions = open_expressions.copy()
            while pending_expressions:
                # Take one expression from the set
                exp = pending_expressions.pop()
                pending_expressions.add(exp)
                # Evaluate it (and if the solution to a Variable is found, recurse to related Expressions)
                evaluate(exp)

