"""
High level functions for different use cases:

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

"""

# All functions use the interactive session
# They need to open a session to which commands can be added


def close_use_case():
    """ Many of the use cases open a"""
    return None


def start_case_study():
    """
    No parameters
    :return:
    """
    return None


def continue_working_case_study():
    """
    Need the code of the case study. Or the version.
    Closed version cannot be opened this way
    :return:
    """


def play_with_existing_case_study():
    """
    Open case study
    The session will not be persisted
    Or would it be just a normal session without final "persist"?
    :return:
    """


def start_anonymous_case_study():
    """
    Open case study
    Without identity
    Do not allow saving, only during session
    ¿What's the difference with start case study and not save it?
    Maybe when opening a case study WARN that, if the user is not identified, nothing can be persisted
    Also, think about limiting command_executors regarding resources consumption (memory AND CPU)

    :return:
    """