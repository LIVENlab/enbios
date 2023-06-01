from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Optional, Union

import bw2data
from bw2data.backends import ActivityDataset
from prompt_toolkit import PromptSession
from prompt_toolkit.buffer import Buffer
from prompt_toolkit.completion import Completer, Completion
from prompt_toolkit.formatted_text.base import FormattedText
from voluptuous import default_factory

try:
    import enbios2
    # import enbios2.generic
    # from enbios2.generic.enbios2_logging import get_logger
except ImportError as err:
    print("importing enbios2")
    # print(err)
    import sys

    for dir_ in reversed(Path(__file__).parents):
        if dir_.name == "enbios2":
            sys.path.insert(0, dir_.as_posix())
            break
from enbios2.generic.enbios2_logging import get_logger


class State(Enum):
    PROJECT = "project"
    IN_PROJECT = "in_project"
    DATABASE = "database"
    IN_DATABASE = "in_database"
    METHOD = "method"


logger = get_logger(__file__)


class CustomBuffer(Buffer):
    def insert_completion(self, completion, overwrite_before=0):
        self.delete_before_cursor(overwrite_before)
        self.text = completion.text  # Replace the entire text with the completion


tab_message = ". Press [tab] or [space] to list all options 🌕"
exit_message = ". Press [ctrl+c] to exit 🔚"

GLOBAL_COMMAND_RESET = "RESET"


@dataclass
class PromptStore:
    """
    stuff that is stored and collected and will be exported
    """
    project: Optional[str] = None
    methods: list[tuple] = field(default_factory=list)


@dataclass
class PromptState:
    state: State = State.PROJECT
    current_db: bw2data.backends.base.SQLiteBackend = None
    current_candidates: list[str] = field(default_factory=list)  # current candidates depend on the status
    selected_candidates: list[str] = field(default_factory=list)  # for multi-selection


class CustomCompleter(Completer):
    def __init__(self):
        super(CustomCompleter, self).__init__()
        self.message: Union[str, FormattedText] = ""  # current message before each input
        self.state: PromptState = PromptState()
        self.data: PromptStore = PromptStore()

        self.reset()

    def reset(self):
        self.data = PromptStore()
        self.state = PromptState()

        self.set_message("select a brightway project")
        self.state.current_candidates = self.get_candidates()

    def get_candidates(self) -> list[str]:
        """
        get the candidates depending on the current status
        :return: all candidates for the current status
        """
        if self.state.state == State.PROJECT:
            return [p.name for p in bw2data.projects]
        elif self.state.state == State.IN_PROJECT:
            return [State.DATABASE.value, State.METHOD.value]
        elif self.state.state == State.DATABASE:
            return bw2data.databases
        elif self.state.state == State.IN_DATABASE:
            # logger.debug(f"current_db: {self.state.current_db}")
            candidates = list((act.name for act in ActivityDataset.select(ActivityDataset.name).where(
                ActivityDataset.database == self.state.current_db.name)))
            return candidates
        elif self.state.state == State.METHOD:
            return ["_".join(m) for m in bw2data.methods]
        return []

    def get_completions(self, document, complete_event):
        word = document.get_word_before_cursor()

        # If there's no word before the cursor, yield all words as completions.
        if not word:
            for known_word in self.state.current_candidates:
                yield Completion(known_word, start_position=-len(word))
            return

        # If the word before the cursor is a substring of a known word, yield it as a completion.
        for known_word in self.state.current_candidates:
            if word.lower() in known_word.lower():
                if known_word in self.state.selected_candidates:
                    style = 'bg:#a0ff70'  # green
                    yield Completion(known_word, start_position=-len(word), style=style)
                else:
                    yield Completion(known_word, start_position=-len(word))

    def set_status(self, new_state: State, text: Optional[str] = None):
        """
        set the status and update the candidates depending on the new status
        :param new_state:
        :return:
        """
        self.state.state = new_state
        if self.state.state == State.IN_DATABASE:
            self.state.current_db = bw2data.Database(text)
        self.state.current_candidates = self.get_candidates()

    def set_message(self, msg: Union[str, list[tuple[str, str]]]):
        """
        set the message before the input.
        can be string or formatted text. Will add newline and >>> automatically
        :param msg:
        :return:
        """
        if isinstance(msg, str):
            self.message = msg + "\n>>> "
        else:
            self.message = FormattedText(msg + [("", "\n>>>")])

    def set_next(self, text: str):
        logger.debug(f"{self.state.state} / set_next: {text}")
        if text == GLOBAL_COMMAND_RESET:
            self.reset()
        elif self.state.state == State.PROJECT:
            print(text, type(text), self.state.current_candidates)
            if text in self.state.current_candidates:
                print("selected project: ", text)
                bw2data.projects.set_current(text)
                self.data.project = text
                self.set_status(State.IN_PROJECT)
                self.set_message("select 'database' or 'method'")
            else:
                print(f"project: {text} not found")
        elif self.state.state == State.IN_PROJECT:
            if text == State.DATABASE.value:
                self.set_status(State.DATABASE)
                self.set_message(
                    f"There are {len(self.state.current_candidates)} databases. Select any number of candidates one by one")
            elif text == State.METHOD.value:
                self.set_status(State.METHOD)
                self.set_message(
                    f"There are {len(self.state.current_candidates)} methods. Select any number of candidates one by one")
            else:
                print(f"command: {text} not found")
        elif self.state.state == State.DATABASE:
            print("loading database...")
            self.set_status(State.IN_DATABASE, text)
            self.set_message(
                f"There are {len(self.state.current_candidates)} activities. Select any number of candidates one by one")
        elif self.state.state == State.METHOD:
            try:
                # check validity
                selected_index = self.state.current_candidates.index(text)
                selected_method = list(bw2data.methods)[selected_index]
                if selected_method in self.data.methods:
                    # remove the method again
                    self.data.methods.remove(selected_method)
                    self.state.selected_candidates.remove(text)
                    self.set_message([("", "You "),
                                      ("bg:#a00000", "deselected"),
                                      ("",
                                       f" {text}. You selected {len(self.state.selected_candidates)} methods.")])
                else:
                    self.data.methods.append(selected_method)
                    self.state.selected_candidates.append(text)
                    self.set_message(f"You selected {len(self.state.selected_candidates)} methods.")
            except ValueError:
                print(f"method: {text} not found")

    def toolbar_msg(self):
        messages: list[tuple[str, str]] = []
        if self.state.state == State.PROJECT:
            messages.append(('bold', "🔥 Select a brightway project"))
        elif self.state.state == State.IN_PROJECT:
            messages.append(('bold', f"🔥 Project: '{self.data.project}'. Select 'database' or 'method'"))
        elif self.state.state == State.METHOD:
            messages.append(('bold', f"🔥 Project: '{self.data.project}'. Select methods..."))
        return FormattedText(messages + [("", tab_message), ("", exit_message)])


custom_completer = CustomCompleter()

session = PromptSession(completer=custom_completer,
                        bottom_toolbar=custom_completer.toolbar_msg)

# bw2data.projects.set_current("uab_bw_ei39")
# #
# candidates = list(ActivityDataset.select(ActivityDataset).where(
#     ActivityDataset.database == "ei391"))
# print(candidates[0])


while True:
    try:
        text = session.prompt(custom_completer.message)
        custom_completer.set_next(text)
    except KeyboardInterrupt:
        text = input("enter 'q' to quit. Anything else to get back to work\n>>> ")
        if text == "q":
            break
