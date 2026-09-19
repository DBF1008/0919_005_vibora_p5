"""
Test-only compatibility shim so the legacy codebase can run the manual
test scripts on Python 3.10+. Not imported by production code.
"""
import collections
import collections.abc
import multiprocessing

for _name in ('Callable', 'Mapping', 'MutableMapping', 'Iterable', 'Sequence', 'Hashable'):
    if not hasattr(collections, _name):
        setattr(collections, _name, getattr(collections.abc, _name))

# multiprocessing defaults to "spawn" on macOS which cannot pickle the app;
# RequestHandler is a fork-based design.
try:
    multiprocessing.set_start_method('fork', force=True)
except (RuntimeError, ValueError):
    pass
