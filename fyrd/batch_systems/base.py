from fyrd import logme as _logme
from fyrd import conf as _conf

import Pyro4


class BatchSystemError(Exception):
    """Exception raised when an error is returned from the batch system
    """
    def __init__(self, message, stdout=None, stderr=None):
        super(BatchSystemError, self).__init__(message)
        self.stdout = stdout
        self.stderr = stderr


class BatchSystemClient(object):
    NAME = None
    def __init__(self, remote=True, uri=None, server_class=None):
        """Creates a BatchSystemClient object.
        All functionalities (submit, kill, gen_scripts...) are redirected to
        the server to do it as generic as possible. If some batch system needs
        to execute some of the moethods locally they can be overloaded.

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
                If the barch system runs locally, a server class must be
                provided, where it'll be the methods to execute. It should be
                a subclass of BatchSystemServer. Example:

                .. code-block:: python

                    SlurmClient(remote=False, server_class=SlurmServer)
        """
        self.remote = remote

        self.uri = None
        self.server = None
        self.connected = False
        self.max_con_retries = 2

        if self.remote:
            if uri:
                self.uri = uri
            else:
                self.uri = _conf.get_option(self.NAME, 'uri')
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

        server =  Pyro4.Proxy(uri)

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

    def release(self):
        if self.connected:
            self.server._pyroRelease()
            self.connected = False

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

    def submit(file_name, dependencies=None, job=None, args=None, kwds=None):
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
    def queue_parser(self, user=None, partition=None):
        """Iterator for queue parsing.

        Parameters
        ----------
        user : str, optional
            User name to pass to qstat to filter queue with
        partiton : str, optional
            Partition to filter the queue with

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
        return server.queue_parser(user=user, partition=partition)


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

    @classmethod
    def start_server(cls):
        """Class method that created the server daemon.
        """
        obj = cls()
        obj.daemonize()
        return obj

    def daemonize(self, host=None, port=None, objId=None):
        """Creates the server daemon.
        """
        if self.running:
            # TODO: Find or create a better exception
            raise Exception('Daemon already running')

        args = {}
        if host:
            args['host'] = host
        if port:
            args['port'] = port

        with Pyro4.Daemon(**args) as daemon:
            if not objId:
                objId = self.__class__.__name__
            uri = daemon.register(self, objectId=objId)

            print('Daemon running. Object uri =', uri)
            self.running = True
            self.daemon = daemon
            daemon.requestLoop()
        self.running = False

    def shutdown(self):
        if self.running:
            _logme.log('Pyro4 daemon shutdown.', 'info')
            self.daemon.shutdown()
            self.running = False

    def is_running(self):
        return len(self.daemon.sockets) > 0

    ###########################################################################
    #                         Pure Virtual Functions                          #
    ###########################################################################
    def queue_test(self, warn=True):
        raise NotImplementedError()

    def normalize_job_id(self, job_id):
        raise NotImplementedError()

    def normalize_state(self, state):
        raise NotImplementedError()

    def gen_scripts(self, job_object, command, args, precmd, modstr):
        raise NotImplementedError()

    def submit(self, file_name, dependencies=None, job=None, args=None, kwds=None):
        raise NotImplementedError()

    def kill(self, job_ids):
        raise NotImplementedError()

    def queue_parser(self, user=None, partition=None):
        raise NotImplementedError()

    def parse_strange_options(self, option_dict):
        raise NotImplementedError()
