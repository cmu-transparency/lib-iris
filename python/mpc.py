""" Multiprocessing utilities. """

import threading as th
import copy

# http://nbviewer.jupyter.org/gist/minrk/4563193
import sys
import os
from contextlib import contextmanager

import misc

# we need a lock, so that other threads don't snatch control
# while we have set a temporary parent
stdout_lock = th.Lock()  # pylint: disable=invalid-name


@contextmanager
def set_stdout_parent(parent):
    """A context manager for setting a particular parent for sys.stdout the parent determines the
    destination cell of output.
    """

    save_parent = sys.stdout.parent_header
    with stdout_lock:
        sys.stdout.parent_header = parent
        try:
            yield
        finally:
            # the flush is important, because that's when the parent_header actually has its effect
            sys.stdout.flush()
            sys.stdout.parent_header = save_parent


class Tasker(th.Thread):  # pylint: disable=too-many-instance-attributes
    """Resumable sets of tasks."""

    def __init__(self, filename, input_maker, evaluator):
        th.Thread.__init__(self)

        self.filename = filename

        self.tasks = list(input_maker())
        self.evaluator = evaluator

        self.completed_lock = th.Lock()
        self.completed_all = misc.load_or_new(filename, dict())
        self.completed = None
        self._update_num_completed()

        self.num_tasks = len(self.tasks)

        self.running = False

    def reset(self):
        """Reset the completed set of tasks."""

        if os.path.exists(self.filename):
            os.remove(self.filename)
        self.completed = dict()

    def _update_num_completed(self):
        self.completed = self.get_completed_of_tasks()
        self.num_completed_all = len(self.completed_all.keys())
        self.num_completed = len(self.completed.keys())

    def get_completed_of_tasks(self):
        """Return all of the completed tasks and record priour completions."""

        ret = {}
        for task in self.tasks:
            if task in self.completed_all:
                ret[task] = self.completed_all[task]
        return ret

    def get_completed_all(self):
        """Get all of the completed tasks including ones from previous runs."""

        temp = None
        with self.completed_lock:
            temp = copy.copy(self.completed_all)
        return temp

    def get_completed(self):
        """Get current run's completed tasks."""

        temp = None
        with self.completed_lock:
            temp = copy.copy(self.completed)
        return temp

    def stop(self):
        """Stop execution."""

        self.running = False

    def __str__(self):
        return "[{self.num_completed}/{self.num_tasks} {self.num_completed_all}]".format(
            **locals()
        )

    def run(self):
        """Start execution."""

        self.running = True
        thread_parent = sys.stdout.parent_header
        with set_stdout_parent(thread_parent):
            for task in self.tasks:
                if task in self.completed:
                    continue
                misc.printme("{self} running: {task} ".format(**locals()))
                result = self.evaluator(task)
                with self.completed_lock:
                    self.completed_all[task] = result
                    self._update_num_completed()
                misc.printme(" done\n")
                misc.save(self.filename, self.completed_all)
                if not self.running:
                    return
