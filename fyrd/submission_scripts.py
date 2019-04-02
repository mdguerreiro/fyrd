# -*- coding: utf-8 -*-
"""
Classes to build submission scripts.
"""
import os  as _os
import sys as _sys
import inspect as _inspect
import cloudpickle as _pickle

###############################################################################
#                               Import Ourself                                #
###############################################################################

from . import run as _run
from . import logme as _logme
from . import script_runners as _scrpts


class Script(object):

    """A script string plus a file name."""

    written = False

    @property
    def file_name(self):
        file_name = _os.path.join(self.job_object.scriptpath, self._file_name)
        return file_name

    def __init__(self, file_name, script, job):
        """Initialize the script and file name."""
        self.script     = script
        self._file_name = file_name
        self.job_object = job

    def write(self, overwrite=True):
        """Write the script file."""
        _logme.log('Script: Writing {}'.format(self.file_name), 'debug')
        pth = _os.path.split(_os.path.abspath(self.file_name))[0]
        if not _os.path.isdir(pth):
            raise OSError('{} Does not exist, cannot write scripts'
                          .format(pth))
        if overwrite or not _os.path.exists(self.file_name):
            with open(self.file_name, 'w') as fout:
                fout.write(self.script + '\n')
            self.written = True
            return self.file_name
        else:
            return None

    def clean(self, delete_output=None):
        """Delete any files made by us."""
        if delete_output:
            _logme.log('delete_output not implemented in Script', 'debug')
        if self.written and self.exists:
            _logme.log('Script: Deleting {}'.format(self.file_name), 'debug')
            _os.remove(self.file_name)

    @property
    def exists(self):
        """True if file is on disk, False if not."""
        return _os.path.exists(self.file_name)

    def __repr__(self):
        """Display simple info."""
        return "Script<{}(exists: {}; written: {})>".format(
            self.file_name, self.exists, self.written)

    def __str__(self):
        """Print the script."""
        return repr(self) + '::\n\n' + self.script + '\n'


class Function(Script):

    """A special Script used to run a function."""

    @property
    def pickle_file(self):
        pickle_file = _os.path.join(self.job_object.scriptpath,
                                    self._pickle_file)
        return pickle_file

    @property
    def outfile(self):
        outfile = _os.path.join(self.job_object.scriptpath, self._outfile)
        return outfile

    def __init__(self, file_name, python, function, job, args=None, kwargs=None,
                 imports=None, syspaths=None, pickle_file=None, outfile=None):
        """Create a function wrapper.

        NOTE: Function submission will fail if the parent file's code is not
        wrapped in an if __main__ wrapper.

        Parameters
        ----------
        file_name : str
            A root name to the outfiles
        function : callable
            Function handle.
        job: Job instance
            Job instance of the function/script call
        args : tuple, optional
            Arguments to the function as a tuple.
        kwargs : dict, optional
            Named keyword arguments to pass in the function call
        imports : list, optional
            A list of imports, if not provided, defaults to all current
            imports, which may not work if you use complex imports.  The list
            can include the import call, or just be a name, e.g ['from os
            import path', 'sys']
        syspaths : list, optional
            Paths to be included in submitted function
        pickle_file : str, optional
            The file to hold the function.
        outfile : str, optional
            The file to hold the output.
        """
        def user_package(package):
            """
                Guess if the package belongs to the python system or is a user
                package (either installed with pip or private module).

                Args:
                    package (str):
                        Package name

                Returns:
                    Returns False if the package belongs to the python system,
                    True otherwise.

            """
            import imp

            # Get first section of the module
            module = package.split('.')[0]

            # Check if module is in the package directory and not in built-ins
            try:
                file, path, desc = imp.find_module(module)
                if desc[2] == imp.PKG_DIRECTORY:
                    return True
                else:
                    return False
            except ImportError:
                return False

        def inspect_object_module(obj, obj_name):
            """
                If the object's class/method contains a __module__ attribute,
                it changes its scope to __main__ in order to allow serializers
                to include the object in the pickled file.

                Args:
                    obj (instance or class/method):
                        Object instance or class or method type to change
                        __module__ attribute.

                    obj_name (str):
                        Name of the Object (either class type or instance).

            """
            import inspect

            # Obj is a class type
            if inspect.isclass(obj):
                base_cls = obj.mro()

            # Obj is a method/function object
            elif inspect.isfunction(obj):
                base_cls = [obj]

            # Instance object, get class type
            else:
                base_cls = type(obj).mro()

            # For each base class or function method
            for cls in base_cls:
                # Check if the class/method is an user package and is not
                # installed on the remote Pyro4 server, then change scope
                if hasattr(cls, '__module__') and cls.__module__ != '__main__'\
                        and user_package(cls.__module__) \
                        and not job.batch.is_module_installed(cls.__module__):
                    _logme.log(
                        'Changing module for {}:{} ({}) to \'__main__\''
                        .format(obj_name, cls.__module__, cls), 'debug'
                    )

                    try:
                        orig_module = cls.__module__
                        cls.__module__ = '__main__'
                        self.pickle_modules[cls] = orig_module
                        self.pickle_objects[cls.__name__] = orig_module

                    # TypeError: can't set attributes of built-in type
                    except Exception:
                        pass

        _logme.log('Building Function for {}'.format(function), 'debug')
        self.function = function
        self.parent   = _inspect.getmodule(function)
        self.args     = args
        self.kwargs   = kwargs

        ##########################
        #  Take care of imports  #
        ##########################
        # Note: this can be directly managed by serialization modules such as
        # dill or cloudpickle and it is not really required to do it here. If
        # you want to disable this option set pickle_imports = False
        pickle_imports = True
        if pickle_imports:
            impts = _run.indent("None")
            func_import = ""

            # Fix global function/class imports where pickle.load fails due to
            # missing imports in the remote node.  This workarround modifies
            # module name in order to avoid trying to load modules when pickle
            # file is loaded remotely. This operation is only performed over
            # object instances (functions, classes, arguments) that belong to
            # the user package space (either installed with pip or private).
            #
            # NOTE: I have not found any better way of not being intrusive with
            # classes or functions not defined by the user. So far so good...

            # Keep track of the objects modified in order to restore them after
            # serializing data in order to allow the module process to run.
            # Dictionary with:
            # - key: class/method/obj type
            # - value: module where is implemented
            self.pickle_modules = {}
            self.pickle_objects = {}

            # Check for objects imported in the user function scope
            # Dictionary with:
            # - key: variable/function/type name
            # - value: instance, method or class type
            # Change cls.__module__ by '__main__'
            for obj_name, obj in self.function.__globals__.items():
                inspect_object_module(obj, obj_name)

            # Same for args
            if self.args:
                for arg in self.args:
                    inspect_object_module(arg, str(arg))

            # Same for kwargs
            if self.kwargs:
                for kwarg_name, kwarg in self.kwargs.items():
                    inspect_object_module(kwarg, kwarg_name)

            _logme.log("List of modules changed: "
                       "{}".format(self.pickle_modules), 'debug')

        else:
            filtered_imports = _run.get_all_imports(
                function, {'imports': imports}, prot=True
            )

            # Get rid of duplicates and join imports
            impts = _run.indent('\n'.join(set(filtered_imports)), '    ')
            impts = _run.indent("None")

            # Import the function itself
            func_import = _run.indent(_run.import_function(function), '    ')
            func_import = ""

            # sys paths
            if syspaths:
                _logme.log('Syspaths: {}'.format(syspaths), 'debug')
                impts = (_run.indent(_run.syspath_fmt(syspaths), '    ')
                         + '\n\n' + impts)

        # Set file names
        self.job_object   = job
        self._pickle_file = pickle_file if pickle_file else file_name + '.pickle.in'
        self._outfile     = outfile if outfile else file_name + '.pickle.out'

        # Create script text
        script = '#!{}\n'.format(python)
        script += _scrpts.FUNC_RUNNER.format(name=file_name,
                                             modimpstr=func_import,
                                             imports=impts,
                                             pickle_file=self.pickle_file,
                                             out_file=self.outfile)

        super(Function, self).__init__(file_name, script, job)

    def write(self, overwrite=True):
        """Write the pickle file and call the parent Script write function."""
        _logme.log('Writing pickle file {}'.format(self.pickle_file), 'debug')
        with open(self.pickle_file, 'wb') as fout:
            _pickle.dump((self.function, self.args, self.kwargs), fout)
        super(Function, self).write(overwrite)
        self.restore_modules()

    def restore_modules(self):
        # Restore module imports if they were modified
        if hasattr(self, 'pickle_modules'):
            _logme.log("Restoring pickled_modules "
                       "{}".format(self.pickle_modules), 'debug')
            for cls, orig_module in self.pickle_modules.items():
                cls.__module__ = orig_module

    def clean(self, delete_output=False):
        """Delete the input pickle file and any scripts.

        Parameters
        ----------
        delete_output : bool, optional
            Delete the output pickle file too.
        """
        if self.written:
            if _os.path.isfile(self.pickle_file):
                _logme.log('Function: Deleting {}'.format(self.pickle_file),
                           'debug')
                _os.remove(self.pickle_file)
            else:
                _logme.log('Function: {} already gone'
                           .format(self.pickle_file), 'debug')
            if delete_output:
                if _os.path.isfile(self.outfile):
                    _logme.log('Function: Deleting {}'.format(self.outfile),
                               'debug')
                    _os.remove(self.outfile)
                else:
                    _logme.log('Function: {} already gone'
                               .format(self.outfile), 'debug')
        super(Function, self).clean(None)
