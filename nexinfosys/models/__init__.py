# -*- coding: utf-8 -*-
"""
This package contains core models. It is divided in modules, a module per model topic. Some of the topics are
persistent (relational database) some are for memory (and they are persisted by serialization):

* Hierarchies in database
* Hierarchies in memory
* Statistical datasets
* MuSIASEM core concepts
* MuSIASEM generative concepts
* MuSIASEM problem specification
* MuSIASEM methodology support

"""
import logging
from collections import namedtuple

MODEL_VERSION = 1

# Ad-hoc structures
# CodeMutable = namedlist("Code", ["code", "description", "level", "children", "parents"])
# ConceptImmutable = namedtuple("ConceptTuple", "name istime description attributes code_list")
CodeImmutable = namedtuple("CodeTuple", "code description level children")
log_level = logging.DEBUG