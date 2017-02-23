import ast
import textwrap
import mock
import os

# Store original __import__
orig_import = __import__
# This will be the B module
b_mock = mock.Mock()

def import_mock(name, *args):
    valids = ['sys','os','re', 'codecs']
    if name not in valids:
        return b_mock
    return orig_import(name, *args)
    

def parse_setup(setup_filename):
    __file__ = 'dummy'
    """Parse setup.py and return args and keywords args to its setup
    function call

    """
    mock_setup = textwrap.dedent('''\
    def setup(*args, **kwargs):
        __setup_calls__.append((args, kwargs))
    ''')
    parsed_mock_setup = ast.parse(mock_setup, filename=setup_filename)
    with open(setup_filename, 'rt') as setup_file:
        parsed = ast.parse(setup_file.read())
        for index, node in enumerate(parsed.body[:]):
            if (
                not isinstance(node, ast.Expr) or
                not isinstance(node.value, ast.Call) or
                node.value.func.id != 'setup'
            ):
                continue
            parsed.body[index:index] = parsed_mock_setup.body
            break

    fixed = ast.fix_missing_locations(parsed)
    codeobj = compile(fixed, setup_filename, 'exec')
    local_vars = {}
    global_vars = {'__setup_calls__': [], '__file__':'getDep.py'}
    try:
        exec(codeobj, global_vars, local_vars)
    except ImportError:
        with mock.patch('builtins.__import__', side_effect=import_mock):
            exec(codeobj, global_vars, local_vars)
    print(global_vars['__setup_calls__'])
    print('work done')
    i = global_vars['__setup_calls__'][0]
    print('cool we made it here bud')
    if i:
        if 'install_requires' in i or 'requires' in i:
            return i[1].get('install_requires', []) + i[1].get('requires', [])
    else:
        return None
        
#i = parse_setup('setups/setup.py')
    