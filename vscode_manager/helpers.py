from contextlib import ContextDecorator
from dataclasses import dataclass
from functools import partial
import logging
from pathlib import Path, PurePath
import signal
import sys
import tempfile
from time import sleep
from typing import Callable, Dict, List, Generator, Union
from typing_extensions import Self
import urllib.request
from urllib.error import URLError

import sh
import yaml

from vscode_manager import __prog__, __version__, __status__, SESSION_STORE


# globals #####################################################################


load_yaml = partial(yaml.load, Loader=yaml.Loader)
dump_yaml = partial(yaml.dump, Dumper=yaml.Dumper)
yaml_header = f'# {__prog__} v{__version__} ({__status__})'

sacct = sh.Command('/opt/slurm/22.05.8/bin/sacct')
scancel = sh.Command('/opt/slurm/22.05.8/bin/scancel')

partitions = {
    'int': {
        'cpu': 8,
        'mem': 250,
        'gpu': 2,
        'time': 16,
        'qos': 'qos_int',
        'nodes': ('compute001', 'compute002', 'compute003', 'compute004',
            'compute005', 'compute006', 'hmem001', 'gpu001', 'gpu002',
            'gpu003', 'gpu004')
    },
    'cpu': {
        'cpu': 16,
        'mem': 250,
        'gpu': 0,
        'time': 72,
        'qos': 'qos_batch',
        'nodes': ('compute001', 'compute002', 'compute003', 'compute004',
            'compute005', 'compute006', 'hmem001')
    },
    'gpu': {
        'cpu': 56,
        'mem': 500,
        'gpu': 4,
        'time': 168,
        'qos': 'qos_batch',
        'nodes': ('gpu001', 'gpu002', 'gpu003', 'gpu004')
    },
    'hmem': {
        'cpu': 64,
        'mem': 4000,
        'gpu': 0,
        'time': 168,
        'qos': 'qos_batch',
        'nodes': ('hmem001', )
    }
}


# classes #####################################################################


class ShutdownHandler(logging.StreamHandler):
    ''' Logging handler that forces exit(1) on ERROR'''

    def emit(self, record) -> None:
        super().emit(record)
        if record.levelno >= logging.ERROR:
            sys.exit(1)


class SignalHandler(ContextDecorator):
    ''' Wrapper class / decorator that intercepts SIGINTs and SIGTERMs to allow
    shutdown functions to be run '''

    def __init__(self, f: Callable, *args, **kwargs) -> None:
        self._shutdown_function = partial(f, *args, **kwargs)

    def _handle_interrupt(self, sig, frame) -> None:
        logger = get_logger('vscode_signal_handler')
        logger.warn('Kill signal received, running shutdown functions')
        self._shutdown_function()
        sys.exit()

    def __enter__(self) -> Self:
        signal.signal(signal.SIGINT, self._handle_interrupt)
        signal.signal(signal.SIGTERM, self._handle_interrupt)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        pass


@dataclass
class Request:
    ''' Job request class object'''

    partition: str
    cpu: int
    mem: int
    gpu: int
    time: int
    qos: str = ''

    def __post_init__(self) -> None:
        ''' Validate the request against the partition limits '''
        logger = get_logger('vscode_request')
        try:
            assert 1 <= self.cpu <= partitions[self.partition]['cpu']
            assert 1 <= self.mem <= partitions[self.partition]['mem']
            assert 0 <= self.gpu <= partitions[self.partition]['gpu']
            assert 1 <= self.time <= partitions[self.partition]['time']
            self.qos = partitions[self.partition]['qos']
        except AssertionError:
            logger.error(
                f'Invalid request for SLURM partition: {self.partition}')  # exit(1)

    def format(self, job_name: str, tmpfile: Path) -> List[str]:
        ''' Return string list of kwargs for `sbatch` '''
        return [
            '--job-name', job_name,
            '--output', tmpfile.name,
            '--partition', self.partition,
            '--qos', self.qos,
            '--ntasks', '1',
            '--cpus-per-task', f'{self.cpu}',
            '--gpus', f'{self.gpu}',
            '--mem', f'{self.mem}G',
            '--time', f'{self.time}:00:00',
            '--signal', 'B:SIGTERM@60',
            '--parsable'
        ]


class Job:
    ''' Cluster job class object '''

    def __init__(self, job_id: str, quiet: bool=False) -> None:
        self.job_id = job_id
        self._quiet = quiet
        self.query()

    def query(self) -> None:
        '''
        Query `sacct` for the status of a job & update object attributes

        Parameters:
            job_id (str): the SLURM job id
        '''
        logger = get_logger('vscode_job', self._quiet)
        with SignalHandler(cancel_job, self.job_id):
            try:
                run_sacct = partial(
                    sacct,
                    '-PXn',
                    '--format', 'JobName,Partition,State,NodeList',
                    '-j', self.job_id
                )
                while not (sacct_line := run_sacct()) or \
                        sacct_line.startswith('allocation'):
                    sleep(2)
            except sh.ErrorReturnCode:
                logger.error('`sacct` had a non-zero exit status')  # exit(1)
        sacct_line = sacct_line.strip().split('|')
        for k, v in zip(('job_name', 'partition', 'state', 'node'), sacct_line):
            setattr(self, k, v)

    @property
    def is_running(self) -> bool:
        self.query()
        return self.state == 'RUNNING'

    @property
    def is_pending(self) -> bool:
        self.query()
        return self.state == 'PENDING'

    def wait(self, backoff: int=1, backoff_max: int=60) -> None:
        '''
        Wait for a job to schedule, retrying with backoff

        Parameters:
            job_id (str): the SLURM job id
        '''
        logger = get_logger('vscode_job', self._quiet)
        with SignalHandler(cancel_job, self.job_id):
            attempt = 1
            while self.is_pending:
                sleep_time = min(backoff * (2 ** attempt), backoff_max)
                logger.info(f'... Trying again in {sleep_time}s')
                sleep(sleep_time)
                attempt += 1


class Session(Job):
    ''' vscode session class object, subclassing Job '''

    @property
    def url(self) -> str:
        try:
            return self._url
        except AttributeError:
            return ''

    @url.setter
    def url(self, url: str) -> None:
        self._url = url

    @property
    def is_alive(self) -> bool:
        if self.is_running:
            try:
                urllib.request.urlopen(self.url).getcode()
                return True
            except URLError:
                pass
        return False

    def _as_dict(self) -> Dict[str, str]:
        ''' Return the object as a dict '''
        return {
            self.job_id: {
                'job_name': self.job_name,
                'partition': self.partition,
                'node': self.node,
                'url': self.url
            }
        }

    def write(self) -> None:
        ''' Write (atomic) the session information as YAML '''
        self.query()
        _, path = tempfile.mkstemp(dir=SESSION_STORE, text=True)
        with open(path, 'w') as F:
            print(yaml_header, file=F)
            dump_yaml(self._as_dict(), F)
        Path(path).rename(SESSION_STORE / f'{self.job_id}.yml')

    @classmethod
    def load(cls, yml: Path) -> Union["Session", None]:
        ''' Load a YAML file, returning a Session object. If the session is no
        longer running, delete the YAML and return None. '''
        job_id = PurePath(yml).stem
        session = cls(job_id)
        if session.state not in ('RUNNING', 'PENDING'):
            yml.unlink()
            return None
        with open(yml) as F:
            session_yaml = load_yaml(F)
        session.url = session_yaml[job_id]['url']
        return session


# functions ###################################################################


def get_logger(name: str, quiet: bool=False) -> logging.Logger:
    '''
    Return a new logger with the specified name

    Parameters:
        name (str): name presented by the logger
        quiet (bool): verbose output?
    Returns:
        logging.Logger: the Logger object
    '''

    logger = logging.getLogger(name)
    logger.setLevel(logging.WARN if quiet else logging.INFO)
    if not logger.hasHandlers():
        logger.addHandler(ShutdownHandler())
    return logger


def get_vscode_jobs() -> Generator[Session, None, None]:
    ''' Yield running Session objects for running sessions '''

    for yml in SESSION_STORE.glob('*.yml'):
        session = Session.load(yml)
        if session is not None:
            yield session


def cancel_job(job_id: str) -> None:
    '''
    `scancel` a submitted or running job

    Parameters:
        job_id (str): the SLURM job id
    '''
    logger = get_logger('vscode_stop')
    logger.warn(f'Terminating job {job_id} and cleaning up')
    try:
        scancel(job_id, b=True, s='TERM')
    except sh.ErrorReturnCode:
        logger.error('`scancel` had a non-zero exit status')  # exit(1)
