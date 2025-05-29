import sys
import os

python_version_min = '2.7.0'
pandas_version_min = '0.7.0'

global_results = ''


def main():
    output_environment_info()
    check_libraries()
    print(global_results)


def output_environment_info():
    """Output detailed Python environment information"""
    pyVersion = sys.version_info
    append_to_results('=== Python Environment Information ===')
    append_to_results('Python version: {}.{}.{}'.format(pyVersion[0], pyVersion[1], pyVersion[2]))
    append_to_results('Python executable: {}'.format(sys.executable))
    
    # Detect environment type
    env_type = detect_environment_type()
    if env_type:
        append_to_results('Environment: {}'.format(env_type))
    
    # Output library versions
    append_to_results('=== Library Versions ===')
    output_library_version('numpy')
    output_library_version('pandas') 
    output_library_version('scipy')
    output_library_version('sklearn', 'scikit-learn')
    output_library_version('matplotlib')
    
    append_to_results('=== Dependency Check Results ===')


def detect_environment_type():
    """Detect if running in virtual environment, conda, etc."""
    # Check for virtual environment
    if hasattr(sys, 'real_prefix') or (hasattr(sys, 'base_prefix') and sys.base_prefix != sys.prefix):
        venv_path = getattr(sys, 'prefix', 'Unknown')
        return 'Virtual Environment ({})'.format(venv_path)
    
    # Check for conda
    if 'CONDA_DEFAULT_ENV' in os.environ:
        return 'Conda Environment ({})'.format(os.environ['CONDA_DEFAULT_ENV'])
    
    # Check for conda by executable path
    if 'conda' in sys.executable.lower() or 'anaconda' in sys.executable.lower():
        return 'Conda/Anaconda Environment'
    
    return 'System Python'


def output_library_version(library_name, display_name=None):
    """Output the version of a library if available"""
    if display_name is None:
        display_name = library_name
    
    try:
        module = __import__(library_name)
        version = getattr(module, '__version__', 'Unknown version')
        append_to_results('{}: {}'.format(display_name, version))
    except ImportError:
        append_to_results('{}: Not installed'.format(display_name))


def check_libraries():
    check_min_python()
    isPython3 = sys.version_info >= (3, 0)
    if isPython3:
        check_library('io')
    else:
        check_library('StringIO')
    check_library('math')
    check_library('traceback')
    check_library('socket')
    check_library('struct')
    check_library('os')
    check_library('json')
    check_library('base64')
    check_library('pickle')
    check_library('scipy')
    check_library('sklearn')
    
    if check_library('matplotlib'):
        if not check_library('matplotlib', ['pyplot']):
            print(
                'It appears that the \'pyplot\' class in matplotlib ' +
                'did not import correctly.\nIf you are using a Python ' +
                'virtual environment please install the \'_tkinter\' module.' +
                '\nPython must be installed as a framework for matplotlib ' +
                'to work properly.')

    check_library('numpy')
    if check_library('pandas', ['DataFrame'], pandas_version_min):
        check_min_pandas()
    # Check for optional pyarrow library
    append_to_results('=== Apache Arrow Support ===')
    if check_library('pyarrow'):
        append_to_results('Apache Arrow support is available (recommended for better performance)')
        output_library_version('pyarrow')
    else:
        append_to_results('Apache Arrow support not available (optional - install pyarrow for better performance)')


def check_min_python():
    base = python_version_min.split('.')
    pyVersion = sys.version_info
    result = check_min_version(base, sys.version_info)
    if result:
        append_to_results('Installed python does not meet min requirement')


def check_min_pandas():
    min_pandas = pandas_version_min.split('.')
    try:
        import pandas

        actual_pandas = pandas.__version__.split('.')
        # Some versions of pandas (i.e included in the Intel Python distro)
        # have version numbers with more than 3 parts (e.g. 0.22.0+0.ga00154d.dirty).
        # So, this check is now commented out
        # if len(actual_pandas) is not len(min_pandas):
        #    raise Exception()
        result = check_min_version(min_pandas, actual_pandas)
        if result:
            append_to_results(
                'Installed pandas does not meet the minimum requirement: version ' + pandas_version_min)
    except:
        append_to_results('A problem occurred when trying to import pandas')


def check_min_version(base, actual):
    ok = False
    equal = False
    for i in range(len(base)):
        if int(actual[i]) > int(base[i]):
            ok = True
            equal = False
            break;
        if not ok and int(actual[i]) < int(base[i]):
            equal = False
            break;
        if int(actual[i] == int(base[i])):
            equal = True

    return not ok and not equal


def check_library(library, cls=[], version=None):
    ok = True
    if not check_library_available(library):
        ok = False
        result = 'Library "' + library + '" is not available'
        if version is not None:
            result += ', minimum version = ' + version
        append_to_results(result)
    else:
        for c in cls:
            if not is_class_available(library, c):
                ok = False
                append_to_results(
                    'Required class ' + c + ' in library ' + library + ' is not available')
    return ok


def is_class_available(library, cls):
    env = {}
    exec (
        'try:\n\tfrom ' + library + ' import ' + cls + '\n\tresult = True\nexcept:\n\tresult = False',
        {}, env)
    return env['result']


def check_library_available(library):
    env = {}
    exec (
        'try:\n\timport ' + library + '\n\tresult = True\nexcept:\n\tresult = False',
        {}, env)
    return env['result']


def append_to_results(line):
    global global_results
    global_results += line + '\n'


main()
