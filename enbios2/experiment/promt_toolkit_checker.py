from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional, Union

import bw2data
from prompt_toolkit import PromptSession
from prompt_toolkit.buffer import Buffer
from prompt_toolkit.completion import Completer, Completion
from prompt_toolkit.formatted_text.base import FormattedText
from prompt_toolkit.lexers import PygmentsLexer
from pygments.lexers.python import Python3Lexer

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

PROJECT = "project"
IN_PROJECT = "in_project"
DATABASE = "database"
METHOD = "method"

logger = get_logger(__file__)


class CustomBuffer(Buffer):
    def insert_completion(self, completion, overwrite_before=0):
        self.delete_before_cursor(overwrite_before)
        self.text = completion.text  # Replace the entire text with the completion


tab_message = ". Press [tab] or [space] to list all options 🌕"
exit_message = ". Press [ctrl+c] to exit 🔚"


@dataclass
class PromptStore:
    """
    stuff that is stored and collected and will be exported
    """
    project: Optional[str] = None
    methods: list[tuple] = field(default_factory=list)


class CustomCompleter(Completer):
    def __init__(self):
        super(CustomCompleter, self).__init__()

        self.data: PromptStore = PromptStore()

        self.message: Union[str, FormattedText] = ""  # current message before each input
        self.set_message("select a brightway project")
        self.status = PROJECT  # status defined the autocompletion candidates
        self.current_candidates = self.get_candidates()  # current candidates depend on the status
        self.selected_candidates = []  # for multi-selection...

    def get_candidates(self) -> list[str]:
        """
        get the candidates depending on the current status
        :return: all candidates for the current status
        """
        if self.status == PROJECT:
            return [p.name for p in bw2data.projects]
        elif self.status == IN_PROJECT:
            return [DATABASE, METHOD]
        elif self.status == DATABASE:
            return bw2data.databases
        elif self.status == METHOD:
            return ["_".join(m) for m in bw2data.methods]
        return []

    def update_candidates(self):
        """
        update the candidates depending on the current status
        """
        self.current_candidates = self.get_candidates()

    def get_completions(self, document, complete_event):
        word = document.get_word_before_cursor()

        # If there's no word before the cursor, yield all words as completions.
        if not word:
            for known_word in self.current_candidates:
                yield Completion(known_word, start_position=-len(word))
            return

        # If the word before the cursor is a substring of a known word, yield it as a completion.
        for known_word in self.current_candidates:
            if word.lower() in known_word.lower():
                if known_word in self.selected_candidates:
                    style = 'bg:#a0ff70'  # green
                    yield Completion(known_word, start_position=-len(word), style=style)
                else:
                    yield Completion(known_word, start_position=-len(word))

    def set_status(self, status: str):
        """
        set the status and update the candidates depending on the new status
        :param status:
        :return:
        """
        self.status = status
        self.update_candidates()

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
        logger.debug(f"{self.status} / set_next: {text}")
        if self.status == PROJECT:
            if text in self.current_candidates:
                print("selected project: ", text)
                bw2data.projects.set_current(text)
                self.data.project = text
                self.set_status(IN_PROJECT)
                self.set_message("select 'database' or 'method'")
            else:
                print(f"project: {text} not found")
        elif self.status == IN_PROJECT:
            if text == DATABASE:
                self.status = DATABASE
            elif text == METHOD:
                self.set_status(METHOD)
                self.set_message(
                    f"There are {len(self.current_candidates)} methods. Select any number of candidates one by one")
            else:
                print(f"command: {text} not found")
        elif self.status == METHOD:
            try:
                # check validity
                selected_index = self.current_candidates.index(text)
                selected_method = list(bw2data.methods)[selected_index]
                if selected_method in self.data.methods:
                    # remove the method again
                    self.data.methods.remove(selected_method)
                    self.selected_candidates.remove(text)
                    self.set_message([("", "You "),
                                      ("bg:#a00000", "deselected"),
                                      ("",
                                       f" {text}. You selected {len(self.selected_candidates)} methods.")])
                else:
                    self.data.methods.append(selected_method)
                    self.selected_candidates.append(text)
                    self.set_message(f"You selected {len(self.selected_candidates)} methods.")
            except ValueError:
                print(f"method: {text} not found")

    def toolbar_msg(self):
        messages: list[tuple[str, str]] = []
        if self.status == PROJECT:
            messages.append(('bold', "🔥 Select a brightway project"))
        elif self.status == IN_PROJECT:
            messages.append(('bold', f"🔥 Project: '{self.data.project}'. Select 'database' or 'method'"))
        elif self.status == METHOD:
            messages.append(('bold', f"🔥 Project: '{self.data.project}'. Select methods..."))
        return FormattedText(messages + [("", tab_message), ("", exit_message)])


custom_completer = CustomCompleter()

session = PromptSession(lexer=PygmentsLexer(Python3Lexer), completer=custom_completer,
                        bottom_toolbar=custom_completer.toolbar_msg)

while True:
    try:
        text = session.prompt(custom_completer.message)
        custom_completer.set_next(text)
    except KeyboardInterrupt:
        text = input("enter 'q' to quit. Anything else to get back to work\n>>> ")
        if text == "q":
            break
