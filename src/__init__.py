import os
import sys

# Add the src directory to sys.path
src_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "pypeline_functions"))
if src_dir not in sys.path:
    sys.path.insert(0, src_dir)

import logging

# Setup logging for the package
logging.getLogger(__name__).addHandler(logging.NullHandler())

try:
    from ._version import __version__
except ImportError:
    __version__ = "unknown"

from pypeline_functions import google_takeout, spotify, utils

__all__ = [
    "google_takeout",
    "spotify",
    "utils",
]
