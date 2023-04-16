"""
Persistent store, in database

* server (URL, UUID)
  * case study (UUID). The case study can be stored in several servers
    * submission (UUID). Several versions of the case study
      * transaction. A submission can be composed of several transactions
        * commands. A transaction is made of several commands
          * primitives. Commands can generate primitives. This should not be persisted, but generated in memory
          * command output. Command output may be stored
    * public primitives
  * users
  * groups
  * ACLs

"""
import datetime
import logging
import threading
import traceback
from queue import Queue

from sqlalchemy import Column, Boolean, Integer, String, Unicode, DateTime, LargeBinary, ForeignKey
from copy import deepcopy
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.orderinglist import ordering_list
from sqlalchemy.orm import relationship, backref, composite, scoped_session, sessionmaker, class_mapper, ColumnProperty, RelationshipProperty
from sqlalchemy.orm.state import InstanceState
from sqlalchemy.types import TypeDecorator, CHAR
# import psycopg2  # Only for automatic detection of dependencies
from sqlalchemy.dialects import postgresql
from sqlalchemy.dialects.postgresql import UUID, JSONB
import uuid
import jsonpickle

from nexinfosys.models import MODEL_VERSION
import sys

__author__ = 'rnebot'

DBSession = scoped_session(sessionmaker())  # TODO - Is this thread safe ??


class GUID(TypeDecorator):
    """
    Platform-independent GUID type.

    Uses Postgresql's UUID type, otherwise uses
    CHAR(32), storing as stringified hex values.

    """
    impl = CHAR

    cache_ok = True

    def load_dialect_impl(self, dialect):
        if dialect.name == 'postgresql':
            return dialect.type_descriptor(UUID())
        else:
            return dialect.type_descriptor(CHAR(32))

    def process_bind_param(self, value, dialect):
        if value is None:
            return value
        elif dialect.name == 'postgresql':
            return str(value)
        else:
            if not isinstance(value, uuid.UUID):
                return "%.32x" % uuid.UUID(value).int
            else:
                # hexstring
                return "%.32x" % value.int

    def process_result_value(self, value, dialect):
        # if dialect.name == 'postgresql':
        #     return uuid.UUID(value)
        # else:
        #     return value
        if value is None:
            return value
        else:
            return uuid.UUID(value)


def exec_highly_recursive_function(func, params, size=20000):

    tmp = sys.getrecursionlimit()
    tmp2 = threading.stack_size()
    sys.setrecursionlimit(size)
    threading.stack_size(0x1000000)
    que = Queue()
    try:
        t = threading.Thread(target=lambda q, arg1: q.put(func(arg1)), args=(que, params))
        t.start()
        t.join()
    except:
        traceback.print_exc()
    threading.stack_size(tmp2)
    sys.setrecursionlimit(tmp)
    return que.get()


def serialize_from_object(obj):
    def encode(o_):
        return jsonpickle.encode(o_)

    # tmp = sys.getrecursionlimit()
    # sys.setrecursionlimit(10000)
    # tmp_str = jsonpickle.encode(obj)
    # sys.setrecursionlimit(tmp)
    # return tmp_str

    tmp_str = exec_highly_recursive_function(encode, obj)
    return tmp_str


def deserialize_to_object(s):
    def decode(s_):
        return jsonpickle.decode(s_)

    # tmp = sys.getrecursionlimit()
    # sys.setrecursionlimit(10000)
    # tmp_str = decode(s)
    # sys.setrecursionlimit(tmp)
    # return tmp_str

    tmp_obj = exec_highly_recursive_function(decode, s)
    return tmp_obj


class BaseMixin(object):
    # query = DBSession.query_property()
    # @declared_attr
    # def __tablename__(cls):
    # return cls.__name__.lower()

    # def __new__(cls, *args, **kwargs):
    #     obj = super().__new__(cls)
    #     if '_sa_instance_state' not in obj.__dict__:
    #         obj._sa_instance_state = InstanceState(obj, obj._sa_class_manager)
    #     return obj

    def __deepcopy__(self, memo):
        cls = self.__class__
        logging.debug(str(cls))
        result = cls.__new__(cls)
        memo[id(self)] = result
        for k in class_mapper(cls).iterate_properties:
            if isinstance(k, ColumnProperty):
                name = k.columns[0].name
                getattr(self, name, None)
            elif isinstance(k, RelationshipProperty):
                #if k.back_populates:
                name = k.strategy.key
                getattr(self, name, None)

        for k, v in self.__dict__.items():
            deepcopy(v, memo)
            # setattr(result, k, deepcopy(v, memo))
        return result

    # def __getstate__(self):
    #     state = self.__dict__.copy()
    #     if '_sa_instance_state':
    #         del state['_sa_instance_state']
    #     if isinstance(self, CaseStudy):
    #         if 'versions' in state:
    #             del state['versions']
    #     elif isinstance(self, CaseStudyVersion):
    #         if 'sessions' in state:
    #             del state['sessions']
    #     elif isinstance(self, CaseStudyVersionSession):
    #         if 'commands' in state:
    #             del state['commands']
    #     elif isinstance(self, CommandsContainer):
    #         if 'children_commands' in state:
    #             del state['children_commands']
    #     elif isinstance(self, User):
    #         if 'authenticators' in state:
    #             del state['authenticators']
    #         if 'groups' in state:
    #             del state['groups']
    #     return state
    #
    # def __setstate__(self, state):
    #     self.__dict__.update(state)

ORMBase = declarative_base(cls=BaseMixin)

# dictalchemy.make_class_dictable(ORMBase)

# ---------------------------------------------------------------------------------------------------------------------


def force_load(obj1):

    def force_load_int(obj):
        cls = obj.__class__
        result = cls.__new__(cls)
        memo[id(obj)] = result
        for k in class_mapper(cls).iterate_properties:
            if isinstance(k, ColumnProperty):
                name = k.columns[0].name
                getattr(obj, name, None)
            elif isinstance(k, RelationshipProperty):
                name = k.strategy.key
                v = getattr(obj, name, None)
                if v:
                    try:
                        for it in v:
                            if id(it) not in memo:
                                force_load_int(it)
                    except TypeError:
                        if id(v) not in memo:
                            force_load_int(v)
    memo = {}
    force_load_int(obj1)
    return obj1

# ---------------------------------------------------------------------------------------------------------------------
# Authentication
# Authorization
#


class Authenticator(ORMBase):  # CODES
    """ List of valid authenticators """
    __tablename__ = "authenticators"

    id = Column(Integer, primary_key=True, autoincrement=True)
    uuid = Column(GUID, unique=True)
    name = Column(String(80))
    validation_endpoint = Column(String(1024))


class User(ORMBase):
    """ Identities.
     Users can have one or more authenticators
     Permissions (ACLs) are assigned to Users or Groups of users """
    __tablename__ = "users"

    id = Column(Integer, primary_key=True, autoincrement=True)
    uuid = Column(GUID, unique=True)
    name = Column(String(80))
    email = Column(String(80))
    creation_time = Column(DateTime, default=datetime.datetime.utcnow())
    deactivation_time = Column(DateTime)


class UserAuthenticator(ORMBase):
    """ Recognized user authenticator """
    __tablename__ = "users_authenticators"

    user_id = Column(Integer, ForeignKey(User.id), nullable=False, primary_key=True)
    user = relationship(User, backref=backref("authenticators", cascade="all, delete-orphan"))
    authenticator_id = Column(Integer, ForeignKey(Authenticator.id), nullable=False, primary_key=True)
    authenticator = relationship(Authenticator)

    email = Column(String(80))


class Group(ORMBase):
    __tablename__ = "groups"

    id = Column(Integer, primary_key=True, autoincrement=True)
    uuid = Column(GUID, unique=True)
    name = Column(String(80))


class GroupUser(ORMBase):
    __tablename__ = "groups_users"
    group_id = Column(Integer, ForeignKey(Group.id), nullable=False, primary_key=True)
    user_id = Column(Integer, ForeignKey(User.id), nullable=False, primary_key=True)
    group = relationship(Group, backref=backref("users", cascade="all, delete-orphan"))
    user = relationship(User, backref=backref("groups", cascade="all, delete-orphan"))


class ObjectType(ORMBase):  # CODES
    """ Processor, Flow or Fund (Factor), Hierarchy, ... """
    __tablename__ = "object_types"

    id = Column(Integer, primary_key=True, autoincrement=True)
    uuid = Column(GUID, unique=True)
    name = Column(String(80), nullable=False)


class PermissionType(ORMBase):  # CODES
    __tablename__ = "permission_types"

    id = Column(Integer, primary_key=True, autoincrement=True)
    uuid = Column(GUID, nullable=False)  # Object ID (ACL are on objects with UUID)
    name = Column(String(80))


class ACL(ORMBase):
    """ List of permissions on an object. The detail """
    __tablename__ = "permissions"

    id = Column(Integer, primary_key=True, autoincrement=True)
    uuid = Column(GUID, nullable=False)  # Object ID (ACL are on objects with UUID)
    object_type = Column(Integer, ForeignKey(ObjectType.id), nullable=False)
    object_id = Column(GUID, nullable=False)


class ACLDetail(ORMBase):
    __tablename__ = "permissions_detail"

    id = Column(Integer, primary_key=True, autoincrement=True)
    acl_id = Column(Integer, ForeignKey(ACL.id))
    acl = relationship(ACL, backref=backref("detail", cascade="all, delete-orphan"))
    user = Column(Integer, ForeignKey(User.id))
    group = Column(Integer, ForeignKey(Group.id))
    permission_id = Column(Integer, ForeignKey(PermissionType.id))  # Read, Export (Share?), Modify, Delete (depends on the type of object)
    permission = relationship(PermissionType)

# ---------------------------------------------------------------------------------------------------------------------


class Server(ORMBase):
    """ In case there are federated servers (for the future). The use of UUIDs is related to this. """
    __tablename__ = "servers"

    id = Column(Integer, primary_key=True, autoincrement=True)
    uuid = Column(GUID, unique=True)
    url = Column(String(1024))  # RESTful endpoint of remote server
    cached_name = Column(String(80))
    cached_description = Column(String(1024))

# ---------------------------------------------------------------------------------------------------------------------


class PedigreeTemplate(ORMBase):
    __tablename__ = "nusap_pedigree_templates"

    id = Column(Integer, primary_key=True, autoincrement=True)
    uuid = Column(GUID, unique=True)
    # TODO A list of lists. Each master list has a position, from 1 to n, n being the number of lists, without jumps
    # TODO These lists have a name and a description
    # TODO Each element of a contained list has also a position, plus a short label and a description

# ---------------------------------------------------------------------------------------------------------------------
# Objects supporting "process" in case studies (evolution in time, improvement, collaboration, tracking of changes,...)
# ---------------------------------------------------------------------------------------------------------------------


class CaseStudyStatus(ORMBase):  # CODES
    __tablename__ = "cs_cod_status_codes"

    id = Column(Integer, primary_key=True, autoincrement=True)
    uuid = Column(GUID, unique=True)
    # ELABORATION, FINISHED, FINISHED_PUBLISHED
    name = Column(String(80), nullable=False)

"""
Two special case studies
* One to contain shared objects, like grammars and hierarchies
* Other for unregistered tests (those with no metadata). Case studies 
"""


class CaseStudy(ORMBase):
    __tablename__ = "cs"

    id = Column(Integer, primary_key=True, autoincrement=True)
    uuid = Column(GUID, unique=True, default=str(uuid.uuid4()))
    name = Column(String(80), default=None)
    oid = Column(String(128), nullable=True)
    internal_code = Column(String(20), nullable=True)
    description = Column(String(1024), default=None)
    areas = Column(String(10), default=None)  # W(ater), E(nergy), F(ood), L(and), C(limate -change-), ...
    geographic_level = Column(String(4), nullable=True)  # R(egional), C(ountry), E(urope), S(ectoral)
    restriction_level = Column(String(3), nullable=True)  # I(nternal), C(onfidential), P(ublic)
    version = Column(String(10), nullable=True)

    def __copy__(self):
        cs = CaseStudy()
        # CODES are NOT copied !!!
        cs.uuid = str(uuid.uuid4())  # NEW UUID !!!!
        cs.oid = None
        cs.internal_code = None
        # Other fields (not CODES)
        cs.name = self.name
        cs.description = self.description
        cs.areas = self.areas
        cs.restriction_level = self.restriction_level
        cs.geographic_level = self.geographic_level
        cs.version = self.version

        return cs

    def __init__(self):
        if not self.uuid:
            self.uuid = str(uuid.uuid4())

# class Diagram(ORMBase):
#     __tablename__ = 'cs_diagrams'
#
#     id = Column(Integer, primary_key=True, autoincrement=True)
#     page = Column(String(80), nullable=False)
#     description = Column(String(512), nullable=True)
#     content = Column(JSON, nullable=True)


class CaseStudyVersion(ORMBase):
    """
    A case study can evolve. Different versions
    """
    __tablename__ = "cs_versions"

    id = Column(Integer, primary_key=True, autoincrement=True)
    uuid = Column(GUID, unique=True, default=str(uuid.uuid4()))
    name = Column(String(256), default=None)

    case_study_id = Column(Integer, ForeignKey(CaseStudy.id))
    case_study = relationship(CaseStudy, backref=backref("versions", cascade="all, delete-orphan"))

    creation_instant = Column(DateTime, default=None)

    status_id = Column(Integer, ForeignKey(CaseStudyStatus.id))
    status = relationship(CaseStudyStatus)

    issues = Column(Unicode, nullable=True, default=None)  # If the session resulted in issues (warnings, errors,...) register here
    state = Column(Unicode, nullable=True, default=None)  # Persisted state after the execution of all commands

    state_version = Column(Integer, nullable=True, default=MODEL_VERSION)

    def __copy__(self):
        vs = CaseStudyVersion()
        # vs.id = # NOT copied, a new ID !!!
        # vs.uuid = # NOT copied, a new UUID !!!!
        vs.name = self.name
        # vs.case_study =  NOT COPIED, it must be assigned after COPY !!!!
        # vs.creation_instant = NOT COPIED, the default value -current time- is kept !!!!
        vs.status_id = self.status_id
        return vs

    def __init__(self):
        # if "_sa_instance_state" not in self.__dict__:
        #     self.__dict__.update({"_sa_instance_state": InstanceState(self, self._sa_class_manager)})
        if not self.uuid:
            self.uuid = str(uuid.uuid4())


class CaseStudyVersionSession(ORMBase):
    """
    A version is elaborated in different steps, which are called "work sessions"
    
    """
    __tablename__ = "cs_versions_sessions"

    id = Column(Integer, primary_key=True, autoincrement=True)
    uuid = Column(GUID, unique=True, default=str(uuid.uuid4()))

    version_id = Column(Integer, ForeignKey(CaseStudyVersion.id))
    version = relationship(CaseStudyVersion, backref=backref("sessions", order_by="CaseStudyVersionSession.open_instant", cascade="all, delete-orphan"))

    # If FALSE, this session loads and resumes state from previous sessions. TRUE, start from "zero" status
    restarts = Column(Boolean, default=False)  # Default is "continue" with previous state

    who_id = Column(Integer, ForeignKey(User.id), nullable=False)
    who = relationship(User)

    open_instant = Column(DateTime, default=datetime.datetime.utcnow())
    close_instant = Column(DateTime, default=None)

    # Outcomes of the execution of commands in the Session
    issues = Column(Unicode, nullable=True, default=None)  # If the session resulted in issues (warnings, errors,...) register here
    state = Column(Unicode, nullable=True, default=None)  # Persisted state after the execution of all commands

    state_version = Column(Integer, nullable=True, default=MODEL_VERSION)

    def __init__(self):
        if not self.uuid:
            self.uuid = str(uuid.uuid4())

    def __copy__(self):
        s = CaseStudyVersionSession()
        # s.uuid = NOT copied, a new UUID !!!!
        # s.version = NOT copied, it has to be assigned !!!
        s.restarts = False  # Always FALSE !!!
        s.who = self.who
        s.open_instant = self.open_instant
        s.close_instant = self.close_instant
        s.issues = self.issues
        s.state = self.state
        s.state_version = self.state_version
        return s


class CommandsContainer(ORMBase):
    """ The CommandsContainer object is either the representation of a primitive command or of a generator of command_executors.
        It is not the executable command
        For a primitive command, the content is the serialized JSON content elaborated by the command itself.
        In this case, generator_type is "primitive", content_type "application/json"
        For command generators, the content may be a Spreadsheet file or an R script.
        The generator_type could also be "Spreadsheet", "Rscript".
        The content type for the first would be "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet".
    """
    __tablename__ = "cs_versions_sessions_cmd_gens"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(80))

    session_id = Column(Integer, ForeignKey(CaseStudyVersionSession.id))
    session = relationship(CaseStudyVersionSession, backref=backref("commands", order_by="CommandsContainer.order",
                           collection_class=ordering_list('order'), cascade="all, delete-orphan"))

    order = Column(Integer)
    generator_type = Column(String(48))  # "Primitive", "Spreadsheet", "Rscript"
    content_type = Column(String(128))  # "application/json", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", "application/text"
    content = Column(LargeBinary, nullable=True)  # A JSON string, a Worksheet file, an R script, ...

    # When the command was executed (the result is in the state of the version)
    execution_start = Column(DateTime, nullable=True)
    execution_end = Column(DateTime, nullable=True)

    parent_command_id = Column(Integer, ForeignKey("cs_versions_sessions_cmd_gens.id"))
    parent_command = relationship("CommandsContainer", backref=backref("children_commands", remote_side=[id], order_by="CommandsContainer.order",
                                  collection_class=ordering_list('order')), cascade="all, delete-orphan")
    # Role of the command regarding its parent command (to make child commands independent of the order,
    # like "named parameters", or "iterated command")
    parent_role = Column(String(128))

    @staticmethod
    def create(generator_type, file_type, file):
        """
        Generates command_executors from an input stream (string or file)
        There must be a factory to parse stream 
        :param generator_type: 
        :param file_type: 
        :param file: It can be a stream or a URL or a file name
        """
        c = CommandsContainer()
        c.name = None
        c.generator_type = generator_type
        c.content_type = file_type
        c.content = file  # TODO Read all the content IN-MEMORY
        # c.parent_command # TODO Commands do not allow parents for now
        c.parent_role = None
        c.execution_start = None
        c.execution_end = None
        return c

    def __copy__(self):
        c = CommandsContainer()
        c.name = self.name
        # c.session NOT COPIED, assign it directly !!!!
        c.order = self.order
        c.generator_type = self.generator_type
        c.content_type = self.content_type
        c.content = self.content
        c.parent_command_id = None
        # TODO c.parent_command_id = NOT COPIED, assign it directly !!!!
        # TODO or Copy children
        c.parent_role = self.parent_role
        return c


def load_table(sf, clazz, d):
    """
    Insert pairs (key, value) into a relational table
    It loads a dictionary "d" containing keys and values, into a relational table associated to the class "clazz",
    using the session factory "sf"
    
    :param sf: 
    :param clazz: 
    :param d: 
    :return: 
    """
    session = sf()
    for k, v in d.items():
        i = session.query(clazz).filter(clazz.uuid == k).first()
        if not i:
            ins = clazz()
            ins.uuid = k
            ins.name = v
            session.add(ins)
    session.commit()
    sf.remove()
