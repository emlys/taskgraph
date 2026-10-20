"""TaskGraph init module."""

import sys
from . import _parsl_multiprocessing_override
sys.modules["parsl.multiprocessing"] = _parsl_multiprocessing_override

from .Task import TaskGraph
from .Task import _TASKGRAPH_DATABASE_FILENAME
from .Task import __version__

__all__ = ['__version__', 'TaskGraph', '_TASKGRAPH_DATABASE_FILENAME']
