from contextlib import nullcontext
import difflib
from typing import List, Optional, Tuple, TypeVar, Union
from rich.console import Console
from rich.logging import RichHandler
import rich.progress
from typing import ContextManager
import logging
import sys
from .settings import Settings, logger, APP_NAME
from .cloudutils import request
from miniscutil import dict_diff

console = Console(stderr=True)
# general rule: this logger is used for internal logs only.
# [todo] getting duplicate messages from same logger?
logger.addHandler(
    RichHandler(
        level=logging.DEBUG,
        markup=True,
        show_path=False,
        console=console,
        log_time_format=r"[%X]",
    )
)

pre_tag = rf"[cyan]\[{APP_NAME}][/cyan]"


def user_warning(*args, **kwargs):
    console.print(pre_tag, "[red]WARN[/]", *args, **kwargs)


def user_info(*args, **kwargs):
    """Use this to print info that we can reasonably expect the user will want to see.

    We use this instead of logger.info because these are more messages.
    And we want to include fancy rich formatting."""
    console.print(pre_tag, *args, **kwargs)


def internal_error(*args, **kwargs):
    console.log(pre_tag, "[red]INTERNAL ERROR[/]", *args, **kwargs)


def internal_warning(*args, **kwargs):
    # [todo] log level goes here.
    # console.log(pre_tag, "INTERNAL WARNING", *args, )
    return


def debug(*args, **kwargs):
    logger.debug(*args, **kwargs)


def is_interactive_terminal():
    """Returns true if this program is running in an interactive terminal
    that we can reasonably expect a human to interact with."""
    return sys.__stdin__.isatty()


def decorate(x: str, desc: str):
    return f"[{desc}]{x}[/{desc}]"


T = TypeVar("T")


def tape_progress(
    file: T,
    total: Optional[int],
    bigsize: int = 2**23,
    message: Optional[Union[str, Tuple]] = None,
    description: str = "Reading",
    transient: bool = True,
    **kwargs,
) -> ContextManager[T]:
    """Use this to show a little progress bar as a process reads through the given file.

    Args:
        file: The file to read.
        total: The content length of the file in bytes. If this is None then no message is shown.
        transient: whether the meter should go away once it is done.
        message: a long text to say as a user_info before the progress starts.
            If the content is too small then this will be shown as a debug message.
        description: A little description to put before the progress bar.

    By default, the progress bar is only shown for
    """
    if total is not None and total > bigsize:
        if message:
            if isinstance(message, tuple):
                user_info(*message)
            else:
                user_info(message)
        prog = rich.progress.wrap_file(
            file=file,  # type: ignore
            total=total,
            transient=transient,
            description=description,
            **kwargs,
        )
        return prog  # type: ignore
    else:
        if message:
            if isinstance(message, tuple):
                debug(*message)
            else:
                debug(message)
        return nullcontext(file)


def pp_diff(s1: str, s2: str) -> List[str]:
    """Takes a pair of strings with newlines and diffs them in a pretty way.

    Returns a string that should start on a newline.
    """
    xs = list(difflib.ndiff(s1.splitlines(keepends=True), s2.splitlines(keepends=True)))

    def m(x: str):
        if x.startswith("+"):
            return decorate(x, "green")
        if x.startswith("-"):
            return decorate(x, "red")
        return x

    return "".join(map(m, xs)).splitlines(keepends=False)


_ALREADY_PRINTED = set()


def pp_diffs(old_deps: dict[str, str], new_deps: dict[str, str]) -> str:
    global _ALREADY_PRINTED
    lines = []
    diff = dict_diff(old_deps, new_deps)
    if len(diff.add) > 0:
        for x in diff.add:
            lines.append(decorate("+++ " + str(x), "green"))
    if len(diff.rm) > 0:
        for x in diff.rm:
            lines.append(decorate("--- " + str(x), "red"))
    if len(diff.mod) > 0:
        for x, (v1, v2) in diff.mod.items():
            lines.append(decorate("~~~ " + str(x), "yellow"))
            if (v1, v2) in _ALREADY_PRINTED:
                continue
            _ALREADY_PRINTED.add((v1, v2))
            lines += pp_diff(v1, v2)
    return "\n".join(lines)



def print_jwt_status() -> bool:
    """Returns true if we are authenticated with a JWT auth."""
    cfg = Settings.current()
    jwt = cfg.get_jwt()
    if jwt is None:
        print("Not logged in.")
        return False
    headers = {"Authorization": f"Bearer {jwt}"}
    response = request("GET", "/user", headers=headers)
    if response.status_code == 200:
        print("Logged in.")
        return True
    if response.status_code == 401:
        cfg.invalidate_jwt()
        print("Login session expired.")
        return False
    if response.status_code == 403:
        cfg.invalidate_jwt()
        print("Invalid JWT. Deleting.")
        return False
    response.raise_for_status()
    raise NotImplementedError(response)

def print_api_key_status() -> None:
    api_key = Settings.current().get_api_key()
    if api_key is None:
        print("No API key found.")
        return
    response = request("GET", "/user")
    if response.status_code == 200:
        print("API key valid.")
    elif response.status_code == 401:
        print("API key not valid.")
    else:
        response.raise_for_status()
        print("Unknown response", response.status_code, response.text)
