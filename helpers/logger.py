import os
import sys
import time
from datetime import datetime
from pprint import pformat
from typing import Union, AnyStr, Any


def eprint(*args, **kwargs):
    """
    wrapper for print to stderr

    :param args:
    :param kwargs:
    :return:
    """
    print(*args, file=sys.stderr, **kwargs)


class Logger:
    def __init__(self):
        self.logs = {}
        self.contexts = {}
        self.out_file = None
        self.err_file = None

    def register_context(self, context, timed=True):
        """
        when log to the same context, reset the timer if used

        :param context: name of the context
        :param timed: when timed is false, will not record delta time on this context
        :return:
        """
        self.contexts[context] = time.time() if timed else None

    def reset_contexts(self):
        """
        reset all context timers
        :return:
        """
        for context, timer in self.contexts.items():
            if timer is not None:
                self.contexts[context] = time.time()

    def add_log(self, group: str,
                title: str,
                message: str,
                context: Union[str, None] = None,
                format_template="[{}] [{}] {}"):
        """
        add log to logger

        :param group: a group that groups the logs.
        :param context:
        :param title:
        :param message:
        :param format_template:
        :return:
        """
        log = self._build_log(context, title, message, format_template)

        if group not in self.logs:
            self.logs[group] = []

        self.logs[group].append(log)

    def write_log(self,
                  title: str,
                  message: str,
                  context: Union[str, None] = None,
                  format_template: str = "[{}] [{}] {}",
                  log_level: str = ""):
        """
        write the log directly

        :param context: a task/progress/step that indicate what is going on. log to the same context will reset timer
        :param title:
        :param message:
        :param format_template:
        :param log_level: if "error", "warning" given, log will be written to  stderr
        :return:
        """
        log = self._build_log(context, title, message, format_template)
        if log_level == "warning" or log_level == "error":
            self._write_to_err(log)
        else:
            self._write_to_out(log)

    def write_err_log(self, message: Any, title: str = "error"):
        self.write_log(title, pformat(message), context="error", log_level="error")

    def _build_log(self, context: Union[str, None], title: str, message: str, format_template: str):
        """
        build a log from stretch
        """
        # create context if not exists
        if context is None:
            context = "default"

        if context not in self.contexts:
            self.register_context(context, timed=False)



        # put the log
        log = format_template.format(context, title, message)

        # get and reset elapsed time
        timer = self.contexts[context]
        if timer is not None:
            self.contexts[context] = time.time()
            elapsed_time = self.contexts[context] - timer

            log = f"{log}: {elapsed_time:.3f} s"

        return log

    def get_logs(self) -> dict:
        """
        get the logs in dict

        :return:
        """
        return self.logs

    def write_logs(self, sprt: str = "=", sprt_len: int = 10) -> None:
        """
        write all logs added and clear logger
        always write to "out"

        :return:
        """
        separation = sprt * sprt_len

        for group, log_list in self.logs.items():
            self._write_to_out(f"Group: {group}")
            for log in log_list:
                self._write_to_out(f"\t{log}")

        self._write_to_out(separation)

        self.logs.clear()

    def _write_to_out(self, log: str) -> None:
        """
        directly write to "out"

        :param log:
        :return:
        """
        if self.out_file:
            with open(self.out_file, 'a+', encoding='utf-8') as file:
                file.write(f"{log}\n")
        else:
            print(log)

    def _write_to_err(self, log: str) -> None:
        """
        directly write to "error"

        :param log:
        :return:
        """
        if self.err_file:
            with open(self.err_file, 'a+', encoding='utf-8') as file:
                file.write(f"{log}\n")

        else:
            eprint(log)

    def out_redirect(self, file_path: str, file_name: str = None) -> None:
        """
        set location where redirects "out" log of this logger

        :param file_path:
        :param file_name:
        :return:
        """
        if not file_name:
            file_name = "out.txt"
        self.out_file = os.path.join(file_path, f"{time.strftime('%Y%m%d-%H%M%S')}-{file_name}")

    def err_redirect(self, file_path: str, file_name: str = None) -> None:
        """
        set location where redirects "error" log of this logger

        :param file_path:
        :param file_name:
        :return:
        """
        if not file_name:
            file_name = "err.txt"
        self.err_file = os.path.join(file_path, f"{time.strftime('%Y%m%d-%H%M%S')}-{file_name}")
