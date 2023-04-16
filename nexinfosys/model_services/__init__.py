import base64
import json
import uuid
from abc import ABCMeta, abstractmethod  # Abstract Base Class

import logging
from typing import List, Union, Dict, Any

from nexinfosys import Issue, IssuesOutputPairType
from nexinfosys.common.helper import create_dictionary, PartialRetrievalDictionary

logger = logging.getLogger(__name__)


class IExecutableCommand(metaclass=ABCMeta):
    """ A command prepared for its execution. Commands have direct access to the current STATE """

    @abstractmethod
    def execute(self, state: "State") -> IssuesOutputPairType:
        """
        Execute the command. At the same time, generate a list of issues.
        At this point, it is assumed there are no syntactic errors

        :param state:
        :return: (list of issues, output)
        """
        raise Exception("Execute not implemented")
        # return None, None  # Issues, Output

    @abstractmethod
    def estimate_execution_time(self):
        pass

    @abstractmethod
    def json_serialize(self) -> Dict:
        pass

    # @abstractmethod
    def json_deserialize(self, json_input: Union[dict, str, bytes, bytearray]) -> List[Issue]:
        """
        Read command parameters from a JSON string
        Check the validity of the JSON input
        After this, the command can be executed ("execute")
        
        :param json_input: JSON in a Unicode String 
        :return: --- (the object state is updated, ready for execution)
        """
        issues = []
        if isinstance(json_input, dict):
            self._content = json_input
        else:
            self._content = json.loads(json_input)
        return issues


class Scope:
    """ The scope allows to assign names to entities using a registry """

    def __init__(self, name=None):
        self._name = name  # A name for the scope itself
        self._registry = create_dictionary()

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, name):
        self._name = name

    def __contains__(self, key):  # "in" operator to check if the key is present in the dictionary
        return key in self._registry

    def __getitem__(self, name):
        if name in self._registry:
            return self._registry[name]
        else:
            return None

    def __setitem__(self, name: str, entity):
        if name not in self._registry:
            existing = True
        else:
            existing = False

        self._registry[name] = entity
        return existing

    def __delitem__(self, name):
        del self._registry[name]

    def list(self):
        """ List just the names of variables """
        return self._registry.keys()

    def list_pairs(self):
        """ List tuples of variable name and value object """
        return [(k, v2) for k, v2 in self._registry.items()]


class Namespace:
    def __init__(self):
        self.__scope = []  # List of scopes
        self.__current_scope = None  # type: Scope
        self.__current_scope_idx = -1
        self.new_scope()

    # The registry will have "nested" scopes (names in the current scope take precedence on "higher" scopes)
    # When searching for names, the search will go from the most recent scope to the oldest
    def new_scope(self, name=None):
        """ Create a new scope """
        self.__current_scope = Scope()
        self.__scope.append(self.__current_scope)
        self.__current_scope_idx = len(self.__scope) - 1
        if not name:
            name = "Scope" + str(self.__current_scope_idx)
        self.__current_scope.name = name

    def close_scope(self):
        if self.__current_scope:
            del self.__scope[-1]
        if self.__current_scope_idx >= 0:
            self.__current_scope_idx -= 1
            if self.__current_scope_idx >= 0:
                self.__current_scope = self.__scope[-1]
            else:
                self.__current_scope = None

    def list_names(self, scope=None):
        """ Returns a list of the names of the registered entities of the "scope" or if None, of the CURRENT scope """
        if not scope:
            scope = self.__current_scope

        return scope.list()

    def list(self, scope=None):
        """
            Returns a list of the names and values of the registered entities of
            the "scope" or if None, of the CURRENT scope
        """
        if not scope:
            scope = self.__current_scope

        return scope.list_pairs()

    def list_all_names(self):
        """
            Returns a list of the names of registered entities considering the scopes
            Start from top level, end in bottom level (the current one, which takes precedence)
            :return:
        """
        t = create_dictionary()
        for scope in self.__scope:
            t.update(scope._registry)

        return t.keys()

    def list_all(self):
        """
            Returns a list of the names and variables of registered entities considering the scopes
            Start from top level, end in bottom level (the current one, which takes precedence)

        :return:
        """
        t = create_dictionary()
        for scope in self.__scope:
            t.update(scope._registry)

        return [(k, v2) for k, v2 in t.items()]

    def set(self, name: str, entity):
        """ Set a named entity in the current scope. Previous scopes are not writable. """
        if self.__current_scope:
            var_exists = name in self.__current_scope
            self.__current_scope[name] = entity
            # if var_exists:
            #     logger.warning("'" + name + "' overwritten.")

    def get(self, name: str, scope=None, return_scope=False):
        """ Return the entity named "name". Return also the Scope in which it was found """
        if not scope:
            for scope_idx in range(len(self.__scope) - 1, -1, -1):
                if name in self.__scope[scope_idx]:
                    if return_scope:
                        return self.__scope[scope_idx][name], self.__scope[scope_idx]
                    else:
                        return self.__scope[scope_idx][name]
            else:
                # logger.warning(
                #     "The name '" + name + "' was not found in the stack of scopes (" + str(len(self.__scope)) + ")")
                if return_scope:
                    return None, None
                else:
                    return None
        else:
            # TODO Needs proper implementation !!!! (when scope is a string, not a Scope instance, to be searched in the list of scopes "self.__scope")
            if name in scope:
                if return_scope:
                    return scope[name], scope
                else:
                    return scope[name]
            else:
                logger.error("The name '" + name + "' was not found in scope '" + scope.name + "'")
                if return_scope:
                    return None, None
                else:
                    return None


class State:
    """
    -- "State" in memory --

    Commands may alter State or may just read it
    It uses a dictionary of named Namespaces (and Namespaces can have several scopes)
    Keeps a registry of variable names and the objects behind them.

        It is basically a list of Namespaces. One is active by default.
        The others have a name. Variables inside these other Namespaces may be accessed using that
        name then "::", same as C++

    State Serialization functions specialized in the way State is used in MuSIASEM are in the "serialization" module:

    serialize_state
    deserialize_state

    """

    def __init__(self, d: Dict[str, Any] = None):
        self._default_namespace = ""
        self._namespaces = create_dictionary()

        if d is not None and len(d) > 0:
            self.update(d)

    def new_namespace(self, name):
        self._namespaces[name] = Namespace()
        if self._default_namespace is None:
            self._default_namespace = name

    @property
    def default_namespace(self):
        return self._default_namespace

    @default_namespace.setter
    def default_namespace(self, name):
        if name is not None:  # Name has to have some value
            self._default_namespace = name

    def del_namespace(self, name):
        if name in self._namespaces:
            del self._namespaces

    def list_namespaces(self):
        return self._namespaces.keys()

    def list_namespace_variables(self, namespace_name=None):
        if namespace_name is None:
            namespace_name = self._default_namespace

        return self._namespaces[namespace_name].list_all()

    def update(self, d: Dict[str, Any], namespace_name=None):
        if namespace_name is None:
            namespace_name = self._default_namespace

        if namespace_name not in self._namespaces:
            self.new_namespace(namespace_name)

        for name, entity in d.items():
            self._namespaces[namespace_name].set(name, entity)
        # self._namespaces[namespace_name].update(d)

    def set(self, name, entity, namespace_name=None):
        if namespace_name is None:
            namespace_name = self._default_namespace

        if namespace_name not in self._namespaces:
            self.new_namespace(namespace_name)

        self._namespaces[namespace_name].set(name, entity)

    def get(self, name, namespace_name=None, scope=None):
        if not namespace_name:
            namespace_name = self._default_namespace

        if namespace_name not in self._namespaces:
            self.new_namespace(namespace_name)

        return self._namespaces[namespace_name].get(name, scope)


def get_case_study_registry_objects(state, namespace=None):
    """
    Obtain the main entries of the state

    :param state: Input state (modified also)
    :param namespace: State supports several namespaces. This one serves to specify which one. Default=None
    :return: Tuple: (global index, processor sets, hierarchies, datasets, mappings)
    """
    # Index of ALL objects
    glb_idx = state.get("_glb_idx", namespace)
    if not glb_idx:
        glb_idx = PartialRetrievalDictionary()
        state.set("_glb_idx", glb_idx, namespace)

    # ProcessorSet dict (dict of sets)
    p_sets = state.get("_processor_sets", namespace)
    if not p_sets:
        p_sets = create_dictionary()
        state.set("_processor_sets", p_sets, namespace)

    # Hierarchies Dict
    hh = state.get("_hierarchies", namespace)
    if not hh:
        hh = create_dictionary()
        state.set("_hierarchies", hh, namespace)
    # Datasets Dict
    datasets = state.get("_datasets", namespace)
    if not datasets:
        datasets = create_dictionary()
        state.set("_datasets", datasets, namespace)
    # Mappings Dict
    mappings = state.get("_mappings", namespace)
    if not mappings:
        mappings = create_dictionary()
        state.set("_mappings", mappings, namespace)

    return glb_idx, p_sets, hh, datasets, mappings


class LocallyUniqueIDManager:
    """
    Obtains UUID but encoded in base85, which still is ASCII, but is more compact than the UUID standard hexadecimal
    representation
    """
    class __LocallyUniqueIDManager:
        def __init__(self, c: int = 0):
            self.val = c

        def get_new_id(self, inc):
            return base64.a85encode(uuid.uuid1().bytes).decode("ascii")

        def __str__(self):
            return repr(self) + self.val
    instance = None

    def __init__(self, arg=0):
        if not LocallyUniqueIDManager.instance:
            LocallyUniqueIDManager.instance = LocallyUniqueIDManager.__LocallyUniqueIDManager(arg)
        else:
            LocallyUniqueIDManager.instance.val = arg

    def get_new_id(self, inc: int = 1):
        return self.instance.get_new_id(inc)

    def __getattr__(self, name):
        return getattr(self.instance, name)


"""

API
* Open/close interactive session
* Identify
* Open a reproducible session (optionally load existing command_executors/state). Close it, optionally save.
* CRUD case studies, versions and variables (objects)

* Browse datasets
* Browse case study objects: mappings, hierarchies, grammars

* Submit Worksheet
  - Interactive session
  - Open work session
  - Submit file
	- the file produces a sequence of command_executors
	- execute
	  - elaborate output file and compile issues
  - Close Interactive session (save, new case study version)
  - Close user session
* Submit R script
  - Interactive session
  - Open work session
  - Submit file
	- the file produces command_executors
	- execute
	  - elaborate output file ¿?, compile issues
  - Close work session (save, new case study version)
  - Close Interactive session
* Execute sparse command_executors (from R client, Python client, Google Sheets...)
  - Interactive session
  - parse then execute the command

* Export/import
  - Internal export. No need to be identified
  - Packaged case study export into Zenodo
  - Import case study (reverse of "internal export"). No need to be identified, the import needs a close work session with save or not
* Manage users
  - Link authenticators with identities
* Manage permissions
  - Share/unshare, READ, CONTRIBUTE, SHARE, EXPORT

RESTFul

* Open/close the interactive session
  - Open interactive, persisted session. Creation date registered. Register from where does the connection come
    (last call is also registered so after some time the session can be closed)
    POST /isession  -> return the ID, generate a COOKIE for the interactive session
  - Close interactive session
    DELETE /isession/ -> (the cookie has to be passed)
* Identify
  - Method 1. Through a OAuth2 token

USE CASES
* Start a case study from scratch
* Continue working on a case study. Implies saving the case study. A status flag for the case study (in elaboration, ready, publishable, ...)
* Play with an existing case study. CRUD elements, modify parameters, solve, analyze (read), ....
* Start an anonymous case study from scratch. It will not be saved
* Create case study from an existing one, as new version ("branch") or totally new case study
  *
* Analyze case study (open a case study, play with it)

"""

"""
Module containing high level function calls, controlling the different use cases
These functions are called by the RESTful interface, which is the gate for the different other clients: R client, Web,...

The functions allow a user to start a Work Session on a Case Study. All work is in memory. Specific functions allow
storing in database or in file.

A Work Session has a memory state related to one or more case studies, and a user. To shorten command_executors, there will be a
default case study.

* Queries (to database) encompassing several case studies? That would break the assumption of a case study per work
  session ¿do not assume that then? It implies having a default case study -context- to simplify command_executors
  * It would be like multi-database queries. The result is a valid structure, the retrieval process has to deal with opening
* Spin-off objects (objects born inside a case study which can be used in other case studies). Consider clone or
  reference options.


Use cases:
* Start a case study from scratch
* Continue working on a case study. Implies saving the case study. A status flag for the case study (in elaboration, ready, publishable, ...)
* Play with an existing case study. CRUD elements, modify parameters, solve, analyze (read), ....
* Start an anonymous case study from scratch. It will not be saved
* Create case study from an existing one, as new version ("branch") or totally new case study
  *
* Export case study for publication
* Export objects
* Analyze case study
* Import case study
* Import objects


Work Session level command types:
* Open a session
* Create a case study
* Load case study (from DB, from file)
* Clone case study. Specify if it is a new version or a new case study
  * If it is a new case study, reset metadata
* Save case study. Only for case studies with metadata.
  * The case study
  *
* Case study command_executors
  * Execute
  * Add to the case study


Case study level command types:
* Case study Metadata ("librarian/archivist")
* Data import ("traders"). Prepare external data for its use into the case study
* Specification ("writers"). Modify the structures and values composing the case study, including metadata of elements of the case study
* Solve ("info-reactors"). Deduce new information from existing structures/constraints
* Read/Query ("readers")
* Visualization ("story tellers")

(In-memory) Object types:
* Work session
  * Work session variables
* Case study
* Hierarchy
  * Categories
  * Flows
  * Funds
* Transformation from a hierarchy to another
* Indicator + Benchmark
  * Metabolic rate
  * Many others
* Grammar. A grammar is specialized in a case study. It also serves as a set of constraints.
  * Changes should be allowed before "instantiation"
* Sequences connection
* Upscale transform
* Dataset
* Data process
* Dataset source
* NUSAP matrix or profile
* Analysis matrices: end use, environmental impact, externalization
* DPSIR
* Clustering


The main objects are:
* Work session
* Case study
* Command

A work session can hold a registry of several command_executors.
A case study contains objects generated by command_executors. ¿References
A command can generate command_executors
A persisted case study must register sessions. It must register either a sequence of command_executors or a generator of command_executors (a Spreadsheet file, an R script). The three possibilities

"""


"""
The higher level object is a case study
It can be embedded in a file or registered
Each case study can have several IDs: OID, UUID, name
A sequence of command_executors
Modes of work
* exploratory elaboration of a version of a case study
* review of a version. READ ONLY command_executors, not added to the case study itself
* continue elaborating version of a case study (is a new version?)

Commands. A case study is evolved through command_executors which produce MuSIASEM primitives and provoke moving them, by ETL, by solving, output, etc.
Commands need to be adapted to the different interfaces, like Spreadsheet file or R scripts
Commands are executed in sequence, in transactions (the command_executors are grouped, then submitted in a pack)
Special command_executors to signal START of SUBMISSION and END of SUBMISSION of case study
Commands may be stored under a registered case study or simply executed and forgotten

Command
Several types. Compatibility with tool, ETL/specification/solving/visualization/export, process control (new case study, new submission, new transaction, login) or not
Metadata
Hierarchy

Pre-steps (in no particular order)
* Categories, taxonomies
* Mappings
* Dataset

Steps
* Identify compartments
* Identify relations
* Values
* Reasoning-Solving

"""

"""
* angular
* pyparsing - antlr
* python avanzado
* selenium

* estructuras, api, comandos, formato excel, persistencia

* estructura del proyecto en nexinfosys, incluyendo Python y R
* jupyterhub
* interacción (análisis), empaquetado para reproducibilidad y para demostración

* interface: restful. authentication (OAuth, JWS, OAuth2, other?)
* interface: navigation diagram
* excel: 
* command_executors: specification
* model: enumerate
* patterns: command, data structures, trace from command to generated entities, duck typing, jobs execution queue
* requirements: list of command_executors
* project structure. multiple languages, multiple modules, tests, README, setup, other?
* taiga 

"""

"""
almacenes (server URL)
casos de estudio (UUID)
envíos (ID, relative to the case study)
transacciones (ID, relative to the transaction)
comandos (ID, relative to the transaction)
primitivas (ID, relative to the command)

proc
fact
pf
valor. expresión

jerarq o lista
procs en jerarquía
fact en jerarquía


declare hierarchy1
add p
p is h1.asdf
p is h2.wer
p [p1 p2]
p3 [p1 p4]
(p1 € p y p3)

f [f1 f2]

add q

p.f1=3 m³*.F2
p.F2=5 m²

p1 > p2
p1.f1 > p2.f1

p1.f1 = (QQ)
p1.

swagger r
list command_executors, find an Excel expression

"""