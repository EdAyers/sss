""" All the code for working with the console.

We use the rich console to do everything, but ideally all of the rich-specific code should be
in this file so that we can drop rich as a dependency if needed.

Diagnostics semantics
----------------

The philosophy of HitSave is that ``@memo`` just acts like a cached function.
Hence if any parts of the caching, pickling, uploading process fails within a ``@memo``, we should not disrupt execution.
I find working with `logger` oesn't work well with rich's rich formatting and filtering repeats.
So we are going to abstract over both ``rich.console`` and ``logging``, so that we can tune exactly how logs are delivered.

Generally there are these levels:
- debug: excessive logging about what is going on.
- info: information that the user will probably want to see.
- warning: not execution-stopping, but something is probably going wrong and you should change your code to make the warning go away.
- error: something went wrong that stopped functionality.

And these 'types':
- user: it is something that the user should be aware of or fix.
- internal: it is something that should be reported to HitSave for us to fix.

We have the following message api. All messages use the rich printing convention.

- ``debug()`` is used for detailing what HitSave is up to. They should be off by default. Users may be interested if they are reporting a bug to us.
- ``internal_error()`` is used to report errors that are the HitSave dev's fault. Ideally these are reported with telemetry.
  If exceptions happen in `@memo` they should always end up as internal errors and behaviour falls back to default.
- ``user_error()`` happens when the user uses our api wrong but the error is recoverable.
  Eg type errors, value errors etc.
  These should always occur at the point that the api is called and not deep in fn stack.
  Fatal errors should throw exceptions.
- ``user_warning()`` we can still work, but the user is not using things optimally.
  In these cases, we should tell the user, and give them isntructions on how to fix it. These are things like:
    - user is caching a function that is fast (ie execution time is shorter than load-from-disk time)
    - user used an object that we don't know how to hash.
- ``user_info()`` things where the user will probably want an explanation of what is going on.
  Eg the reason why a cache got invalidated. Important not to spam.
  Another important case for info is any operation where the main execution is blocked for more than a few seconds; eg if you are blocking with an upload.

The plan for this module is to also deal with telemetry, reporting errors to the user, deduplicating messages and so on.

"""
from contextlib import nullcontext
import difflib
from typing import List, Optional, Tuple, TypeVar, Union
from rich.console import Console
from rich.logging import RichHandler
import rich.progress
from typing import ContextManager
import logging
import sys

from miniscutil import dict_diff

console = Console(stderr=True)

# general rule: this logger is used for internal logs only.
logger = logging.getLogger("hitsave")
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

pre_tag = r"[cyan]\[hitsave][/cyan]"


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
        bigsize: when the total is less than this, we do not bother showing the progress bar.

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
