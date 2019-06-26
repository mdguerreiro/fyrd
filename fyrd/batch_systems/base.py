import sys as _sys
import os as _os
import time as _time
import errno as _errno
import pkgutil as _pkgutil
from functools import wraps

from fyrd import logme as _logme
from fyrd import conf as _conf
from fyrd import FYRD_SUCCESS, \
    FYRD_NOT_RUNNING_ERROR, FYRD_STILL_RUNNING_ERROR

import Pyro4
from Pyro4.errors import ConnectionClosedError


# Add Pyro4 remote traceback on exceptions, only shown after exit.
_sys.excepthook = Pyro4.util.excepthook


# Set Fyrd Clients to work in stateless mode (disconnect after each request)
# This option is here in order to free server threads from the pool.
# There is no need to bind again the proxy (_pyroBind, _pyroReconnect) given
# that Pyro4 automatically reconnects in _pyroInvoke if proxy is disconnected.
# Note: this option is only recommended in SERVERTYPE=thread on fyrd server.
STATELESS_CONNECTION = False
PERSISTENT_LIST = ['__init__', '__del__', 'get_uri', 'get_server', 'release']


def pyro_traceback(func):
    """
        Decorator to add Pyro4 server traceback in case of exception and
        stateless connection (disconnect after some client calls).

    """
    @wraps(func)
    def wrapper_pyro_traceback(*args, **kwargs):
        try:
            ret = func(*args, **kwargs)
            # If stateless option is enabled and class method not in persistent
            # disconnect from server to free thread from pool at server side
            if STATELESS_CONNECTION and func.__name__ not in PERSISTENT_LIST:
                # args[0] is self (BatchSystemClient base instance)
                args[0].release(force=True)
            return ret
        except Exception as e:
            msg = ''.join(Pyro4.util.getPyroTraceback()).strip()
            raise type(e)(msg) from e
    return wrapper_pyro_traceback


class MetaClassDecorator(type):
    """
        MetaClass used to add decorators to all members of a derived class.
        One advantage compared to class decorators is the fact that subclasses
        inherit the metaclass.

        The __new__ method is called to create and allocate the derived class
        instance, and requires three arguments used for `type` constructor:
            * name of the class (name)
            * list of base classes (bases)
            * dictionary with local attributes and methods (local)

        The modified __dict__ with the decorators is used on the type.__new__
        constructor and returned as the Class object.

    """
    def __new__(cls, name, bases, local):
        # Check any member in __dict__ that is callable,
        # and add the decorator on it.
        for attr in local:
            value = local[attr]
            if callable(value):
                local[attr] = pyro_traceback(value)
        return type.__new__(cls, name, bases, local)


class BatchSystemError(Exception):
    """Exception raised when an error is returned from the batch system
    """
    def __init__(self, message, stdout=None, stderr=None):
        super(BatchSystemError, self).__init__(message)
        self.stdout = stdout
        self.stderr = stderr


class BatchSystemClient(metaclass=MetaClassDecorator):
    NAME = None

    @property
    def python_path(self):
        """Gets the path of the remote python interpreter.
        """
        server = self.get_server()
        return server.python_path

    @property
    def qtype(self):
        server = self.get_server()
        return server.qtype

    def __init__(self, remote=True, uri=None, server_class=None):
        """Creates a BatchSystemClient object.
        All functionalities (submit, kill, gen_scripts...) are redirected to
        the server to do it as generic as possible. If some batch system needs
        to execute some of the methods locally they can be overloaded.

        To get the proper configuration values the ``NAME`` attribute must
        be set in the subclasses to the batch system name (``slurm``,
        ``local``, ``torque``...)

        Parameters
        ----------
            remote: bool, optional
                Specify if the batch system will run using Pyro4, or a local
                object.
            uri: str, optional
                If the batch system runs on a remote node, a Pyro4 object URI
                can be specified.
            server_class: class, optional
                If the batch system runs locally, a server class must be
                provided, where it'll be the methods to execute. It should be
                a subclass of BatchSystemServer. Example:

                .. code-block:: python

                    SlurmClient(remote=False, server_class=SlurmServer)
        """
        self.remote = remote

        self.uri = None
        self.server = None
        self.connected = False
        self.max_con_retries = 10

        if self.remote:
            if uri:
                self.uri = uri
            else:
                self.uri = _conf.get_option(self.NAME, 'server_uri')
                _logme.log('Getting uri from config file', 'info')
                if not self.uri:
                    raise ValueError('Can\'t find URI in config file.')
            self.connected = self.connect()
        elif server_class:
            self.server = server_class()
        else:
            raise ValueError('No class provided for local execution. Missing '
                             'server_class parameter.')

    def get_uri(self):
        """Get the URI of the remote object

        TODO: Get URI from config file if is not specified in the constructor.
        """
        return self.uri

    def get_server(self):
        """Get the remote object server.
        """
        return self.server

    def connect(self, raise_on_error=True, force=False):
        """ Connect to the remote server and save the remote object in
            ``self.server``.

        Parameters
        ----------
            raise_on_error: bool, optional
                Raise an exception or not if an error happens.
            force: bool, optional
                If the server is already connected, this function won't try to
                connect again if this argument is not set to True.

        Returns
        -------
            success: bool
                True if the server have been started correctly, False if not.
        """
        if not self.remote:
            _logme.log('Trying to connect to server when not working on '
                       'remote. Aborting.', 'warn')
            return False

        if self.connected and not force:
            _logme.log('Trying to connect to server, but it\'s already '
                       'connected. Aborting.', 'warn')
            return False

        uri = self.get_uri()
        if not uri:
            if raise_on_error:
                raise ValueError('Cannot get server uri')
            return False

        server = Pyro4.Proxy(uri)

        for i in range(self.max_con_retries):
            try:
                server._pyroBind()
                break
            except Pyro4.errors.CommunicationError:
                # Test for bad connection
                _logme.log("Cannot bind to server, "
                           "retrying ({}/{}).".format(i + 1,
                                                      self.max_con_retries),
                           'error')
                _time.sleep(3)

        if i == (self.max_con_retries - 1):
            _logme.log(
                "Cannot bind to server still. Failing."
                'critical'
            )
            if raise_on_error:
                raise ConnectionError('Cannot get server')
            return False
        _logme.log('Connected to Pyro4 server: {}'.format(uri), 'info')
        self.server = server
        return True

    def release(self, force=False):
        if self.connected or force:
            server = self.get_server()
            server._pyroRelease()
            self.connected = False

    def is_server_running(self):
        server = self.get_server()
        try:
            server.ping()
            return True
        except Pyro4.errors.CommunicationError:
            return False

    def is_module_installed(self, module):
        """Asks if the module is installed on the Pyro4 server.

        Parameters
        ----------
            module: str
                Module name to ask for on the Pyro4 server.

        Returns
        -------
            success: bool
                True if the server has the module installed.
        """
        server = self.get_server()
        return server.is_module_installed(module)

    def shutdown(self):
        # Pyro4 server waits a little time before shutting down, probably to be
        # able to respond the method call. Sleep 0.5 seconds before returning.
        # https://github.com/irmen/Pyro4/blob/c588305fd79a2e92a487d9fefff410148c8a5db5/src/Pyro4/core.py#L1241
        server = self.get_server()
        server.shutdown()
        _time.sleep(0.5)

    ###########################################################################
    #                           Functionality Test                            #
    ###########################################################################

    def queue_test(self, warn=True):
        """Check that this batch system can be used.

        Parameters
        ----------
        warn : bool
            log a warning on fail

        Returns
        -------
        batch_system_functional : bool
        """
        server = self.get_server()
        return server.queue_test(warn=warn)

    ###########################################################################
    #                         Normalization Functions                         #
    ###########################################################################

    def normalize_job_id(self, job_id):
        """Convert the job id into job_id, array_id."""
        server = self.get_server()
        return server.normalize_job_id(job_id)

    def normalize_state(self, state):
        """Convert state into standadized (slurm style) state."""
        server = self.get_server()
        return server.normalize_state(state)

    ###########################################################################
    #                             Job Submission                              #
    ###########################################################################

    def gen_scripts(self, job_object, command, args, precmd, modstr):
        """Build the submission script objects.

        This script should almost certainly work by formatting `_scrpts.CMND_RUNNER_TRACK`.
        The result should be a script that can be executed on a node by the batch system.
        The format of the output is important, which is why `_scrpts.CMND_RUNNER_TRACK`
        should be used; if it is not used, then be sure to copy the format of the outfile
        in that script.

        Parameters
        ---------
        job_object : fyrd.job.Job
        command : str
            Command to execute
        args : list
            List of additional arguments
        precmd : str
            String from options_to_string() to add at the top of the file, should
            contain batch system directives
        modstr : str
            String to add after precmd, should contain module directives.

        Returns
        -------
        fyrd.script_runners.Script
            The submission script
        fyrd.script_runners.Script, or None if unneeded
            As execution script that will be called by the submission script,
            optional
        """
        server = self.get_server()
        return server.gen_scripts(job_object, command, args, precmd, modstr)

    def submit(self, file_name, dependencies=None, job=None, args=None,
               kwds=None):
        """Submit any file with dependencies.

        If your batch system does not handle dependencies, then raise a
        NotImplemented error if dependencies are passed.

        Parameters
        ----------
        file_name : str
            Path to an existing file
        dependencies : list, optional
            List of dependencies
        job : fyrd.job.Job, optional, not required
            A job object for the calling job
        args : list, optional, not required
            A list of additional command line arguments to pass when submitting
        kwds : dict or str, optional, not required
            A dictionary of keyword arguments to parse with options_to_string, or
            a string of option:value,option,option:value,....

        Returns
        -------
        job_id : str
        """
        server = self.get_server()
        # TODO: Convert path to remote path ????
        return server.submit(file_name,
                             dependencies=dependencies,
                             job=job,
                             args=args,
                             kwds=kwds)

    ###########################################################################
    #                             Job Management                              #
    ###########################################################################
    def kill(self, job_ids):
        """Terminate all jobs in job_ids.

        Parameters
        ----------
        job_ids : list or str
            A list of valid job ids or a single valid job id

        Returns
        -------
        success : bool
        """
        server = self.get_server()
        return server.kill(job_ids)

    ###########################################################################
    #                              Queue Parsing                              #
    ###########################################################################
    def queue_parser(self, user=None, partition=None, job_id=None):
        """Iterator for queue parsing.

        Parameters
        ----------
        user : str, optional
            User name to pass to qstat to filter queue with
        partiton : str, optional
            Partition to filter the queue with
        job_id: str, optional
            Job ID to filter the queue with

        Yields
        ------
        job_id : str
        array_id : str or None
        name : str
        userid : str
        partition : str
        state :str
        nodelist : list
        numnodes : int
        cntpernode : int or None
        exit_code : int or Nonw
        """
        server = self.get_server()
        return server.queue_parser(
                user=user, partition=partition, job_id=job_id
                )

    def parse_strange_options(self, option_dict):
        """Parse all options that cannot be handled by the regular function.

        Parameters
        ----------
        option_dict : dict
            All keyword arguments passed by the user that are not already defined
            in the Job object

        Returns
        -------
        list
            A list of strings to be added at the top of the script file
        dict
            Altered version of option_dict with all options that can't be handled
            by `fyrd.batch_systems.options.option_to_string()` removed.
        list
            A list of command line arguments to pass straight to the submit
            function
        """
        server = self.get_server()
        return server.parse_strange_options(option_dict)


class BatchSystemServer(object):
    NAME = None

    @Pyro4.expose
    @property
    def python_path(self):
        """Gets the path of the remote python interpreter.
        """
        return _sys.executable

    @Pyro4.expose
    @property
    def qtype(self):
        return self.NAME

    @classmethod
    def uri_file(cls):
        uri_filename = '{}_queue.uri'.format(cls.NAME)
        run_dir = _conf.CONFIG_PATH
        return _os.path.join(run_dir, uri_filename)

    @classmethod
    def pid_file(cls):
        pid_filename = '{}_queue.pid'.format(cls.NAME)
        run_dir = _conf.CONFIG_PATH
        return _os.path.join(run_dir, pid_filename)

    @classmethod
    def log_file(cls):
        log_filename = '{}_queue.log'.format(cls.NAME)
        run_dir = _conf.CONFIG_PATH
        return _os.path.join(run_dir, log_filename)

    def __init__(self):
        """Creates a BatchSystemServer object.
        Note that there're some virtual function that **MUST** be overwritten.

        To get the proper configuration values the ``NAME`` attribute must
        be set in the subclasses to the batch system name (``slurm``,
        ``local``, ``torque``...)

        Parameters
        ----------
        """
        self.running = False
        self.daemon = None

    def _system_modules(self):
        """Returns a list with all packages, subpackages and modules available.
        """
        modules = []
        # Iterate over packages obtaining importer, modname and ispkg
        for _, modname, _ in _pkgutil.walk_packages(path=None,
                                                    onerror=lambda x: None):
            modules.append(modname)

        return modules

    def _server_running(self):
        """Return True if server currently running.
        """
        # Check pid file
        if not _os.path.isfile(self.pid_file()):
            return False

        # Get pid from file
        with open(self.pid_file(), 'r') as f:
            pid = int(f.read().strip())
            if pid <= 0:
                return False

        # Check can signal with pid
        try:
            _os.kill(pid, 0)
        except OSError:
            # Any kind of error: ESRCH, EPERM, EINVAL
            return False
        else:
            return True

    @classmethod
    def start_server(cls, host=None, port=None, objId=None):
        """Class method that created the server daemon.
        """
        obj = cls()
        return obj.daemonize(host=host, port=port, objId=objId)

    def daemonize(self, host=None, port=None, objId=None):
        """Creates the server daemon.
        """
        if self._server_running():
            self.running = True
            _logme.log('Daemon is already running on port {}'.format(port),
                       'error', logfile=_sys.stdout)
            return FYRD_STILL_RUNNING_ERROR

        args = {}
        if host:
            args['host'] = host
        if port:
            args['port'] = port

        # Fork Server process
        self.pid = _os.fork()
        if self.pid == 0:
            # Server forked process
            with Pyro4.Daemon(**args) as daemon:

                if not objId:
                    objId = self.__class__.__name__
                uri = daemon.register(self, objectId=objId)

                _logme.log('Daemon is running on uri = {}'.format(uri), 'info')
                self.running = True
                self.daemon = daemon

                # Write PID and URI to conf files
                with open(self.pid_file(), 'w') as f:
                    f.write(str(_os.getpid()))
                with open(self.uri_file(), 'w') as f:
                    f.write(str(uri))

                # MN closes stdout and stderr after ssh logout, bind them to a
                # file to have logging and avoid IO errors.

                # Close stdout and stderr
                _os.close(1)
                _os.close(2)

                # File will be open to fd 1 (stdout)
                with open(self.log_file(), 'w'):

                    # Dup fd 1 to be used as stderr
                    _os.dup(1)

                    # Pyro4 server loop to service incoming requests,
                    # until someone breaks this or calls shutdown().
                    try:
                        daemon.requestLoop()
                    except Exception as e:
                        print(e)
                    finally:
                        _sys.stdout.flush()
                        _sys.stderr.flush()

        else:
            # Wait some time to make sure the process started
            _time.sleep(1)
            if self._server_running():
                _logme.log('Server has started correctly', 'info',
                           logfile=_sys.stdout)
                return FYRD_SUCCESS
            else:
                _logme.log('Server has not started correctly', 'error',
                           logfile=_sys.stdout)
                return FYRD_NOT_RUNNING_ERROR

    @Pyro4.expose
    def is_module_installed(self, module):
        """Checks if the module is installed on the Pyro4 server.

        Parameters
        ----------
            module: str
                Module name to ask for on the Pyro4 server.

        Returns
        -------
            success: bool
                True if the server has the module installed.
        """
        import importlib
        try:
            is_installed = importlib.util.find_spec(module)
            _logme.log(
                'Module {} is installed?: {}'.format(module, is_installed),
                'debug')

        # Python3.7 compatibility
        except ModuleNotFoundError:
            _logme.log('Module {} is NOT installed'.format(module), 'debug')
            return False

        # <Python3.6 compatibility
        if is_installed is None:
            return False
        else:
            return True

    @Pyro4.expose
    def shutdown(self):
        if self.running:
            _logme.log('Pyro4 daemon shutdown.', 'info')
            self.daemon.shutdown()
            self.running = False

            if _os.path.exists(self.pid_file()):
                _os.remove(self.pid_file())
            if _os.path.exists(self.uri_file()):
                _os.remove(self.uri_file())

    @Pyro4.expose
    def ping(self):
        return 'pong'

    ###########################################################################
    #                         Pure Virtual Functions                          #
    ###########################################################################
    @Pyro4.expose
    def queue_test(self, warn=True):
        raise NotImplementedError()

    @Pyro4.expose
    def normalize_job_id(self, job_id):
        raise NotImplementedError()

    @Pyro4.expose
    def normalize_state(self, state):
        raise NotImplementedError()

    @Pyro4.expose
    def gen_scripts(self, job_object, command, args, precmd, modstr):
        raise NotImplementedError()

    @Pyro4.expose
    def submit(self, file_name, dependencies=None, job=None, args=None,
               kwds=None):
        raise NotImplementedError()

    @Pyro4.expose
    def kill(self, job_ids):
        raise NotImplementedError()

    @Pyro4.expose
    def queue_parser(self, user=None, partition=None, job_id=None):
        raise NotImplementedError()

    @Pyro4.expose
    def parse_strange_options(self, option_dict):
        raise NotImplementedError()
