import os
import sys

# Add the src directory to sys.path
src_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'src'))
if src_dir not in sys.path:
    sys.path.insert(0, src_dir)

import logging

# Setup logging for the package
logging.getLogger(__name__).addHandler(logging.NullHandler())

try:
    from ._version import __version__
except ImportError:
    __version__ = "unknown"

from .extract_google_takeout_seed import extract_google_takeout_seed
from .extract_spotify_seed import extract_spotify_seed
from .google_takeout_seed_to_bigquery import google_takeout_seed_to_bigquery
from .spotify_seed_to_bigquery import spotify_seed_to_bigquery

__all__ = [
    "extract_google_takeout_seed",
    "extract_spotify_seed",
    "google_takeout_seed_to_bigquery",
    "spotify_seed_to_bigquery",
]
