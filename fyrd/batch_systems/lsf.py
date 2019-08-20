# -*- coding: utf-8 -*-
"""
LSF parsing functions.
"""
import os as _os
import re as _re
import sys as _sys
import pwd as _pwd     # Used to get usernames for queue
import datetime as _dt

from six import text_type as _txt
from six import string_types as _str
from six import integer_types as _int

import Pyro4

from .. import run as _run
from .. import conf as _conf
from .. import logme as _logme
from .. import ClusterError as _ClusterError
from .. import script_runners as _scrpts
from .. import submission_scripts as _sscrpt

from .base import BatchSystemClient, BatchSystemServer

_Script = _sscrpt.Script

SUFFIX = 'lsf'

# Define lsf-to-slurm mappings
LSF_SLURM_STATES = {
    'PEND': 'pending',      # PEND: Pending
    'PROV': 'pending',      # PROV: Proven state
    'PSUSP': 'suspended',   # PSUSP: Suspended (pending)
    'RUN': 'running',       # RUN: Running
    'USUSP': 'suspended',   # USUSP: Suspended while running
    'SSUSP': 'suspended',   # SSUSP: Suspended by LSF
    'DONE': 'completed',    # DONE: Done finished
    'EXIT': 'failed',       # EXIT: Failure (exit code != 0)
    'UNKWN': 'failed',      # UNKWN: Unknown
    'WAIT': 'held',         # WAIT: Waiting for others (chunk job queue)
    'ZOMBI': 'killed'       # ZOMBI: In zombi state
}


@Pyro4.expose
class LSFServer(BatchSystemServer):
    NAME = 'lsf'

    def metrics(self, job_id=None):
        """Iterator to get metrics from LSF system.

        Parameters
        ----------
        job_id: str, optional
            Job ID to filter the queue with

        Yields
        ------
        line : str

        """
        _logme.log('Getting job metrics', 'debug')

        # No data for the following tags in LSF:
        #   'AveCPUFreq', 'AveDiskRead', 'AveDiskWrite', 'ConsumedEnergy'
        # So we fill them with None.
        # Use 'exit_code' as None flag
        none = 'user'
        fwdth = 400  # Used for fixed-width parsing of bjobs
        fields = (
            'jobid', 'queue', 'exec_host', 'nexec_host', 'effective_resreq',
            none, none, none, 'mem', none,
            'submit_time', 'start_time', 'finish_time', 'run_time'
        )
        time_tags = [10, 11, 12]
        elapsed_tag = 13
        flen = len(fields)

        # Arguments used for bjobs:
        #   - noheader: remove header from 1st row
        #   - a: show jobs in all states
        #   - X: display uncondensed output for hosts
        #   - o: customized output formats
        #   - jobid: get metrics only for that job
        #
        qargs = [
            'bjobs', '-noheader', '-a', '-X', '-o',
            '"{}"'.format(' '.join(['{0}:{1}'.format(field, fwdth)
                                    for field in fields]))
        ]
        if job_id:
            qargs.append(' {}'.format(job_id))

        try:
            bjobs = [
                [k[i:i+fwdth].strip() if field != none else "Not Available"
                     for i, field in zip(range(0, fwdth*flen, fwdth), fields)]
                for k in _run.cmd(qargs)[1].split('\n')
            ]
        except Exception as e:
            _logme.log('Error running bjobs to get the metrics: {}'
                       .format(str(e)), 'error')
            bjobs = []

        # Normalize Time Tags from LSF format to SLURM format
        for idx, job in enumerate(bjobs):
            for tag in time_tags:
                job[tag] = self.normalize_time(job[tag])

            job[elapsed_tag] = self.normalize_elapsed(job[elapsed_tag])
            bjobs[idx] = tuple(job)

        # Yield metric iterator
        for line in bjobs:
            yield line

    ###########################################################################
    #                           Functionality Test                            #
    ###########################################################################
    def queue_test(self, warn=True):
        """Check that LSF can be used.

        Just looks for bsub and bjobs.

        Parameters
        ----------
        warn : bool
            log a warning on fail

        Returns
        -------
        batch_system_functional : bool
        """
        log_level = 'error' if warn else 'debug'
        bsub = _conf.get_option('queue', 'bsub')
        if (bsub is not None and _os.path.dirname(bsub)
                and not _run.is_exe(bsub)):
            _logme.log(
                'Cannot use LSF as sbatch path set in conf to {0}'
                .format(bsub) + ' but that path is not an executable',
                log_level
            )
            return False
        bsub = bsub if bsub else 'bsub'
        bsub = _run.which(bsub) if not _os.path.dirname(bsub) else bsub
        if not bsub:
            _logme.log(
                'Cannot use LSF as cannot find bsub', log_level
            )
            return False
        qpath = _os.path.dirname(bsub)
        bjobs = _os.path.join(qpath, 'bjobs')
        return _run.is_exe(bjobs)

    ###########################################################################
    #                         Normalization Functions                         #
    ###########################################################################
    def normalize_job_id(self, job_id):
        """Convert the job id into job_id, array_id."""
        # Look for job_id and array_id.
        # Arrays are specified in LSF using the following syntax:
        #   - job_id[array_id] (e.g. 1234[1])
        jobquery = _re.compile(r'(\d+)(\[(\d+)\])?')
        job_id, _, array_id = jobquery.match(job_id).groups()

        return job_id, array_id

    def normalize_state(self, state):
        """Convert state into standadized (LSF style) state."""
        if state.upper() in LSF_SLURM_STATES:
            state = LSF_SLURM_STATES[state.upper()]
        return state

    def normalize_time(self, lsf_time):
        """Convert LSF time into standarized (LSF style) time:
            LSF format:
                * Aug 18 14:31 [E|L|X] (MMM DD HH:MM)

                [E|L|X] Job estimation modifiers
                - A dash indicates that the pending, suspended, or job with no
                  run limit has no estimated finish time.

            SLURM format:
                * 2019-08-18T14:31:00 (YYYY-MM-DDTHH:MM:SS)
        """
        # LSF and SLURM formats
        LSF_FORMAT = "%b %d %H:%M"
        SLURM_FORMAT = "%Y-%m-%dT%H:%M:%S"

        # Dash indicates no estimated finish time
        if lsf_time == '-':
            return 'None'

        # Remove optional job estimation modifiers [ELX]
        timequery = _re.compile(r'^([^ELX]+)( [ELX])?$')
        lsf_time, est_mod = timequery.match(lsf_time).groups()

        # Convert to LSF time
        date = _dt.datetime.strptime(lsf_time, LSF_FORMAT)
        # Year is not included in LSF time! include current Year
        date = date.replace(year=_dt.datetime.now().year)
        return date.strftime(SLURM_FORMAT)

    def normalize_elapsed(self, lsf_elapsed):
        """Convert LSF elapsed time into standarized (LSF style) time:
            LSF format:
                * 122 second(s)

            SLURM format:
                * 02:02 [DD-[HH:]]MM:SS
        """
        # Dash indicates no estimated finish time
        if lsf_elapsed == '-':
            return '00:00'

        # Parse seconds from LSF format
        elapsedquery = _re.compile(r'^(\d+) second\(s\)$')
        lsf_elapsed = int(elapsedquery.match(lsf_elapsed).groups()[0])

        # Convert to LSF elapsed format
        elapsed = _dt.timedelta(seconds=lsf_elapsed)
        days = elapsed.days
        hours, remainder = divmod(elapsed.seconds, 3600)
        minutes, seconds = divmod(remainder, 3600)
        s = ("{:02d}-".format(days) if days else "") + \
            ("{:02d}:".format(hours) if hours else "") + \
            "{:02d}:{:02d}".format(minutes, seconds)
        return s

    ###########################################################################
    #                             Job Submission                              #
    ###########################################################################
    def gen_scripts(self, job_object, command, args, precmd, modstr):
        """Can't create the scripts in server side since Job and Script object
        won't be serialized correctly by Pyro4.

        Parameters
        ---------
        job_object : fyrd.job.Job
        command : str
            Command to execute
        args : list
            List of additional arguments, not used in this script.
        precmd : str
            String from options_to_string() to add at the top of the file,
            should contain batch system directives
        modstr : str
            String to add after precmd, should contain module directives.

        Returns
        -------
        fyrd.script_runners.Script
            The submission script
        fyrd.script_runners.Script
            The execution script
        """
        raise NotImplementedError()

    def submit(self, script_file_name, dependencies=None):
        """Submit any file with dependencies to LSF.

        Parameters
        ----------
        script_file_name : str
            Path of the script to be submitted
        dependencies : list
            List of dependencies

        Returns
        -------
        results: dict
            Dictionary containing the results and/or errors.
            If the execution have no errors, it returns the job_id as the
            result.
        """
        _logme.log('Submitting to LSF', 'debug')
        if dependencies:
            deps = '-w "{}"'.format(
                '&&'.join(['done({})'.format(d) for d in dependencies]))
            args = ['bsub', deps, '<', script_file_name]
        else:
            args = ['bsub', '<', script_file_name]
        # Try to submit job 5 times
        code, stdout, stderr = _run.cmd(args, tries=5)
        if code == 0:
            # Job id is returned like this by LSF:
            #   'Job <165793> is submitted to queue <sequential>.'
            jobquery = _re.compile(r'<(\d+)>')
            job_id, _ = self.normalize_job_id(jobquery.findall(stdout)[0])
        else:
            _logme.log('bsub failed with code {}\n'.format(code) +
                       'stdout: {}\nstderr: {}'.format(stdout, stderr),
                       'critical')
            # raise _CalledProcessError(code, args, stdout, stderr)
            # XXX: ?????
            # Pyro4 can't serialize CalledProcessError
            return {'error': True, 'stdout': stdout, 'stderr': stderr}

        return {'error': False, 'result': job_id}

    ###########################################################################
    #                            Job Management                               #
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
        o = _run.cmd('bkill {0}'.format(' '.join(_run.listify(job_ids))),
                     tries=5)
        return o[0] == 0

    ###########################################################################
    #                              Queue Parsing                              #
    ###########################################################################
    def queue_parser(self, user=None, partition=None, job_id=None):
        """Iterator for LSF queues.

        Use the `bjobs -o` command to get standard data across implementation.
        To fully read all jobs (finished and unfinished), option `-a` is used.
        Jobs are retired from the LSF history after reaching interval specified
        by CLEAN_PERIOD in lsb.params (default period is 1 hour).

        Parameters
        ----------
        user : str, optional
            User name to pass to bjobs to filter queue with
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
        exit_code : int or None
        """
        try:
            if job_id:
                int(job_id)
        except ValueError:
            job_id = None

        fwdth = 400  # Used for fixed-width parsing of bjobs
        fields = [
            'jobid', 'name', 'user', 'queue', 'stat',
            'exec_host', 'nexec_host', 'slots', 'exit_code'
        ]
        flen = len(fields)

        # Arguments used for bjobs:
        #   - noheader: remove header from 1st row
        #   - a: show jobs in all states
        #   - X: display uncondensed output for hosts
        #   - o: customized output formats
        #
        qargs = [
            'bjobs', '-noheader', '-a', '-X', '-o',
            '"{}"'.format(' '.join(['{0}:{1}'.format(field, fwdth)
                                    for field in fields]))
        ]
        #
        # Parse queue info by length
        #  - Each job entry is separated by '\n'
        #  - Each job entry is a tuple with each field value
        # [ (fld1, fld2, fl3, ...), (...) ]
        #
        # bjobs returns 'No unfinished job found' on stderr when list is empty
        #
        bjobs = [
            tuple(
                [k[i:i+fwdth].strip() for i in range(0, fwdth*flen, fwdth)]
            ) for k in _run.cmd(qargs)[1].split('\n') if k
        ]

        # Sanitize data
        for binfo in bjobs:

            if len(binfo) == len(fields):
                # jobid -> bid ($jobid)
                # name -> bname ($job_name | $job_name[#array_num])
                # user -> buser ($user_name)
                # queue -> bpartition ($queue)
                # stat -> bstate (PEND | RUN | DONE | EXIT...)
                # exec_host -> bndlst (cpus*nodeid:cpus*nodeid...)
                # nexec_host -> bnodes ($num_nodes)
                # slots -> bcpus ($total_tasks)
                # exit_code -> bcode ($exit_code)
                [bid, bname, buser, bpartition, bstate,
                 bndlst, bnodes, bcpus, bcode] = binfo
            else:
                _sys.stderr.write('{}'.format(repr(binfo)))
                raise _ClusterError('Queue parsing error, expected {} items '
                                    'in output of bjobs, got {}\n'
                                    .format(len(fields), len(binfo)))

            # If not my partition go to next extry
            if partition and bpartition != partition:
                continue

            # Normalize bid and barr
            if not isinstance(bid, (_str, _txt)):
                bid = str(bid) if bid else None
            bid, barr = self.normalize_job_id(bid)

            # Normalize nodes, cpus, state and exit_code
            if not isinstance(bnodes, _int):
                bnodes = int(bnodes) if bnodes else None
            if not isinstance(bcpus, _int):
                bcpus = int(bcpus) if bcpus else None
            if not isinstance(bcode, _int):
                try:
                    bcode = int(bcode) if bcode else None
                except ValueError:
                    bcode = None
            bstate = self.normalize_state(bstate)

            # If user or job id are used to filter skip to next if not found
            if buser.isdigit():
                buser = _pwd.getpwuid(int(buser)).pw_name
            if user and buser != user:
                continue
            if job_id and (job_id != bid):
                continue

            # Attempt to parse nodelist
            #   LSF node list:"16*s01r1b14:16*s01r1b12:16*s01r1b08:16*s01r1b28"
            bnodelist = []
            nodequery = _re.compile(r'(\d+\*)?(\w+)')
            if bndlst:
                # [ (cores, node1), (cores, node2), ...]
                if nodequery.search(bndlst):
                    nsplit = nodequery.findall(bndlst)
                    for nrg in nsplit:
                        cores, node = nrg
                        bnodelist.append(node)
                else:
                    bnodelist = bndlst.split(':')

            yield (bid, barr, bname, buser, bpartition, bstate, bnodelist,
                   bnodes, bcpus, bcode)

    def parse_strange_options(self, option_dict):
        """Parse all options that cannot be handled by the regular function.
        Handled on client side.

        Parameters
        ----------
        option_dict : dict
            All keyword arguments passed by the user that are not already
            defined in the Job object

        Returns
        -------
        list
            A list of strings to be added at the top of the script file
        dict
            Altered version of option_dict with all options that can't be
            handled by `fyrd.batch_systems.options.option_to_string()` removed.
        None
            Would contain additional arguments to pass to sbatch, but these
            are not needed so we just return None
        """
        raise NotImplementedError


class LSFClient(BatchSystemClient):
    """Overwrite simple methods that can be executed in localhost, to avoid
    some network overhead.
    """

    NAME = 'lsf'
    PREFIX = '#BSUB'
    PARALLEL = 'mpirun'

    def metrics(self, job_id=None):
        server = self.get_server()
        return server.metrics(job_id=job_id)

    def normalize_job_id(self, job_id):
        """Convert the job id into job_id, array_id."""
        # Look for job_id and array_id:
        # e.g.: 1234[1]
        jobquery = _re.compile(r'(\d+)(\[(\d+)\])?')
        job_id, _, array_id = jobquery.match(job_id).groups()

        return job_id, array_id

    def normalize_state(self, state):
        """Convert state into standadized (LSF style) state."""
        if state.upper() in LSF_SLURM_STATES:
            state = LSF_SLURM_STATES[state.upper()]
        return state

    def gen_scripts(self, job_object, command, args, precmd, modstr):
        """Build the submission script objects.

        Creates an exec script as well as a submission script.

        Parameters
        ---------
        job_object : fyrd.job.Job
        command : str
            Command to execute
        args : list
            List of additional arguments, not used in this script.
        precmd : str
            String from options_to_string() to add at the top of the file,
            should contain batch system directives
        modstr : str
            String to add after precmd, should contain module directives.

        Returns
        -------
        fyrd.script_runners.Script
            The submission script
        fyrd.script_runners.Script
            The execution script
        """
        scrpt = '{}.{}.{}'.format(job_object.name, job_object.suffix, SUFFIX)

        # Use a single script to run the job and avoid using srun in order to
        # allow sequential and parallel executions to live together in job.
        # NOTE: the job is initially executed in sequential mode, and the
        # programmer is responsible of calling their parallel codes by means
        # of self.PARALLEL preffix.
        job_object._mode = 'remote'
        sub_script = _scrpts.CMND_RUNNER_TRACK.format(
            precmd=precmd, usedir=job_object.runpath, name=job_object.name,
            command=command
        )
        job_object._mode = 'local'

        # Create the sub_script Script object
        sub_script_obj = _Script(
            script=sub_script, file_name=scrpt, job=job_object
        )

        return sub_script_obj, None

    def submit(self, script, dependencies=None,
               job=None, args=None, kwds=None):
        """Submit any file with dependencies to LSF.

        Parameters
        ----------
        script : fyrd.Script
            Script to be submitted
        dependencies : list
            List of dependencies
        job : fyrd.job.Job, not implemented
            A job object for the calling job, not used by this functions
        args : list, not implemented
            A list of additional command line arguments to pass when
            submitting, not used by this function
        kwds : dict or str, not implemented
            A dictionary of keyword arguments to parse with options_to_string,
            or a string of option:value,option,option:value,.... Not used by
            this function.

        Returns
        -------
        job_id : str
        """
        script.job_object._mode = 'remote'
        result = self.get_server().submit(
                script.file_name, dependencies=dependencies
                )
        script.job_object._mode = 'local'
        return result

    def parse_strange_options(self, option_dict):
        """Parse all options that cannot be handled by the regular function.

        Parameters
        ----------
        option_dict : dict
            All keyword arguments passed by the user that are not already
            defined in the Job object

        Returns
        -------
        list
            A list of strings to be added at the top of the script file
        dict
            Altered version of option_dict with all options that can't be
            handled by `fyrd.batch_systems.options.option_to_string()` removed.
        None
            Would contain additional arguments to pass to sbatch, but these
            are not needed so we just return None
        """
        outlist = []

        # Submitter should provide number of cores per node, otherwise we can
        # not figure out cpus_per_task or nodes.
        cores_per_node = None
        if 'cores_per_node' in option_dict:
            cores_per_node = int(option_dict.pop('cores_per_node'))

        # Convert nodes to tasks (-n): use cores_per_node and nodes
        nodes = None
        if 'nodes' in option_dict:
            if not cores_per_node:
                raise _ClusterError('Error parsing LSF options: cannot set '
                                    'the number of nodes in job without '
                                    'specifying \'cores_per_node\' option.')

            nodes = int(option_dict.pop('nodes'))
            # 'tasks' option is not compatible with 'nodes'
            if 'tasks' in option_dict:
                option_dict.pop('tasks')

        # Number of tasks (-n)
        tasks = None
        if 'tasks' in option_dict:
            tasks = int(option_dict.pop('tasks'))

        # Convert cpus_per_task to tasks_per_node (-span["ptile=#tasks_node"])
        if 'cpus_per_task' in option_dict:
            if not cores_per_node:
                raise _ClusterError('Error parsing LSF options: cannot set '
                                    'the cpus per tasks in job without '
                                    'specifying \'cores_per_node\' option.')

            cpus_per_task = int(option_dict.pop('cpus_per_task'))
            tasks_per_node = max(cores_per_node / cpus_per_task, 1)
            # 'tasks_per_node' option is not compatible with 'cpus_per_task'
            if 'tasks_per_node' in option_dict:
                option_dict.pop('tasks_per_node')

        # First look for tasks_per_node, if it's not there change to cores
        # Cores refers to the max number of processors to use per node (ppn)
        if 'tasks_per_node' in option_dict:
            tasks_per_node = int(option_dict.pop('tasks_per_node'))
            if 'cores' in option_dict:
                # Avoid option parser to raise errors
                option_dict.pop('cores')
        elif 'cores' in option_dict:
            tasks_per_node = int(option_dict.pop('cores'))
            if not nodes:
                nodes = 1

        # Use the final 'tasks_per_node' to define tasks if 'nodes' is passed
        if nodes:
            tasks = nodes * tasks_per_node

        # Add the LSF parameters that define the job size (-n, and span[ptile])
        outlist.append('{} -n {}'.format(self.PREFIX, tasks))
        outlist.append('{} -R "span[ptile={}]"'.format(self.PREFIX,
                                                       tasks_per_node))

        if 'exclusive' in option_dict:
            option_dict.pop('exclusive')
            outlist.append('{} -x'.format(self.PREFIX))

        # Parse time in LSF no seconds -> [hour:]minutes (remove seconds)
        if 'time' in option_dict:
            time = option_dict.pop('time')
            timequery = _re.compile(r'((\d+\:)?(\d+))')
            time = timequery.search(time)[0]
            outlist.append('{} -W {}'.format(self.PREFIX, time))

        # Remove qos option if any
        if 'qos' in option_dict:
            option_dict.pop('qos')

        return outlist, option_dict, None
