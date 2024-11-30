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

from pypeline_functions import models, parsers, sources, utils
from pypeline_functions.extract_google_takeout_seed import extract_google_takeout_seed
from pypeline_functions.extract_spotify_seed import extract_spotify_seed
from pypeline_functions.google_takeout_seed_to_bigquery import google_takeout_seed_to_bigquery
from pypeline_functions.spotify_seed_to_bigquery import spotify_seed_to_bigquery

__all__ = [
    "extract_google_takeout_seed",
    "extract_spotify_seed",
    "google_takeout_seed_to_bigquery",
    "models",
    "parsers",
    "sources",
    "spotify_seed_to_bigquery",
    "utils",
]
