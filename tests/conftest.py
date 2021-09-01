import os
import sys

import pytest

LIB_DIR = os.path.join('lib', os.path.dirname('.'))
sys.path.insert(0, os.path.abspath(LIB_DIR))
sys.path.append(os.path.join(os.path.dirname(__file__), 'helpers'))
