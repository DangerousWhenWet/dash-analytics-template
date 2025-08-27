#pylint: disable=line-too-long, missing-docstring, trailing-whitespace, too-few-public-methods
from typing import Dict, Any

from RestrictedPython import compile_restricted, safe_globals
import pandas as pd
import numpy as np


def safe_exec(code_string, **kwargs) -> Dict[str, Any]:
    """
    evaluates literal Python code text in a restricted environment in effort to make it difficult/impossible to damage the host machine
    all provided kwargs are passed into the environment
    all kwargs are passed back out as a dictionary after execution
    """
    byte_code = compile_restricted(code_string, filename="<safe_exec>", mode="exec")
    if byte_code is None:
        raise ValueError("Code compilation failed")
    if hasattr(byte_code, 'errors') and byte_code.errors:
        raise ValueError(f"Code errors: {byte_code.errors}")
    
    class RestrictedPandas:
        def __getattr__(self, name):
            if name in ['read_csv', 'read_json', 'read_excel', 'read_pickle', 'to_pickle']:
                raise AttributeError(f"File operation {name} not allowed")
            return getattr(pd, name)
    
    class RestrictedNumpy:
        def __getattr__(self, name):
            if name in ['load', 'save', 'loadtxt', 'savetxt', 'fromfile', 'tofile']:
                raise AttributeError(f"File operation {name} not allowed")
            return getattr(np, name)
    
    # Globals - restricted environment
    globals_dict = safe_globals.copy()
    globals_dict.update({
        'pd': RestrictedPandas(), 
        'np': RestrictedNumpy()
    }) # type: ignore

    locals_dict = kwargs.copy()
    exec(byte_code, globals_dict, locals_dict) #pylint: disable=exec-used
    return locals_dict
