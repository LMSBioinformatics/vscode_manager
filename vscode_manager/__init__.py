from pathlib import Path
from string import Template


__prog__ = 'vscode_manager'
__version__ = '1.0'
__author__ = 'George Young'
__maintainer__ = 'George Young'
__email__ = 'bioinformatics@lms.mrc.ac.uk'
__status__ = 'Production'
__license__ = 'MIT'

SESSION_STORE = Path.home() / '.vscode_manager'
SESSION_STORE.mkdir(exist_ok=True)
