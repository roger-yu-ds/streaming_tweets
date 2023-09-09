#------------------------------------------------------------------------------#
# Imports                                                                   ####
#------------------------------------------------------------------------------#

from src.utils.misc import str_right
from pandas import DataFrame as pdDataFrame, Series as pdSeries
# from pyspark.sql.dataframe import DataFrame as psDataFrame
from numpy import ndarray, isreal, isscalar, all
from os.path import exists,dirname
import validators


#------------------------------------------------------------------------------#
# is_*() functions                                                          ####
#------------------------------------------------------------------------------#


def is_in(elem, lst):
    assert is_str(elem) or not has_len(elem)
    assert has_len(lst)
    return elem in lst


def is_equal(obj1, obj2):
    return obj1==obj2


def is_str(obj):
    return isinstance(obj, str)


def is_list(obj):
    return isinstance(obj, list)


def is_bool(obj):
    return isinstance(obj, bool)


def is_int(obj):
    return isinstance(obj, int)


def is_float(obj):
    return isinstance(obj, float)


def is_dict(obj):
    return isinstance(obj, dict)


def is_positive(obj):
    return obj > 0


def is_real(obj):
    return isreal(obj)


def is_scalar(obj):
    return isscalar(obj)


def is_url_valid(url:str):
    assert is_str(url)
    try:
        return validators.url(url)
    except:
        return False


def is_pddataframe(obj):
    return isinstance(obj, pdDataFrame)


def is_pdseries(obj):
    return isinstance(obj, pdSeries)


def is_ndarray(obj):
    return isinstance(obj, ndarray)


# def is_psdataframe(obj):
#     return isinstance(obj, psDataFrame)


def is_path_valid(path:str):
    assert is_str(path)
    return exists(path)


def is_path_dir_valid(path:str):
    assert is_str(path)
    return is_path_valid(dirname(path))


def is_path_csv(path:str):
    assert is_str(path)
    try:
        str_right(path,4)==".csv"
    except:
        return False
    else:
        return True


def all_path_csv(paths:list):
    assert all_str(paths)
    if has_len(paths):
        return all([is_path_csv(path) for path in paths])
    else:
        return is_path_csv(paths)



#------------------------------------------------------------------------------#
# is_*_or_*() functions                                                     ####
#------------------------------------------------------------------------------#


def is_float_or_int(obj):
    return isinstance(obj, (float, int))


def is_pddataframe_or_pdseries(obj):
    return isinstance(obj, (pdDataFrame, pdSeries))


def is_pddataframe_or_pdseries_or_ndarray(obj):
    return isinstance(obj, (pdDataFrame, pdSeries, ndarray))


def is_list_or_ndarray(obj):
    return isinstance(obj, (list, ndarray))


def is_list_or_none(obj):
    return isinstance(obj, (list, type(None)))


def is_dict_or_none(obj):
    return isinstance(obj, (dict, type(None)))



#------------------------------------------------------------------------------#
# has_*() funcions                                                          ####
#------------------------------------------------------------------------------#


def has_len(obj):
    return hasattr(obj, "__len__")


def has_shape(obj):
    return hasattr(obj, "shape")



#------------------------------------------------------------------------------#
# all_*() functions                                                         ####
#------------------------------------------------------------------------------#


def all_in(sequence1, sequence2):
    """
    Confirm that all elems of one sequence are definitely contained within another
    """
    return all(elem in sequence2 for elem in sequence1)


def all_str(obj):
    if has_len(obj):
        return all([is_str(elem) for elem in obj])
    else:
        return is_str(obj)
    

def all_list(obj):
    return all([is_list(elem) for elem in obj])


def all_bool(obj):
    if has_len(obj):
        return all([is_bool(elem) for elem in obj])
    else:
        return is_bool(obj)


def all_int(obj):
    if has_len(obj):
        return all([is_int(elem) for elem in obj])
    else:
        return is_int(obj)
    

def all_float(obj):
    if has_len(obj):
        return all([is_float(elem) for elem in obj])
    else:
        return is_float(obj)


def all_positive(obj):
    if has_len(obj):
        return all([is_positive(elem) for elem in obj])
    else:
        return is_positive(obj)


def all_dict(obj):
    if has_len(obj):
        return all([is_dict(elem) for elem in obj])
    else:
        return is_dict(obj)


def all_real(obj):
    if has_len(obj):
        return all([is_real(elem) for elem in obj])
    else:
        return is_real(obj)


def all_scalar(obj):
    if has_len(obj):
        return all([is_scalar(elem) for elem in obj])
    else:
        return is_scalar(obj)


def all_dataframe(obj):
    if has_len(obj):
        return all([is_pddataframe(elem) for elem in obj])
    else:
        return is_pddataframe(obj)


def all_ndarray(obj):
    if has_len(obj):
        return all([is_ndarray(elem) for elem in obj])
    else:
        return is_ndarray(obj)


def all_path_valid(obj):
    if has_len(obj):
        return all([is_path_valid(elem) for elem in obj])
    else:
        return is_path_valid(obj)
    
    
def all_path_dir_valid(obj):
    if has_len(obj):
        return all([is_path_dir_valid(elem) for elem in obj])
    else:
        return is_path_dir_valid(obj)
    
    
def all_equal(obj):
    return len(set(obj))==1



#------------------------------------------------------------------------------#
# all_*_or_*() functions                                                    ####
#------------------------------------------------------------------------------#


def all_dataframe_or_series(obj):
    if has_len(obj):
        return all([is_pddataframe_or_pdseries(elem) for elem in obj])
    else:
        return is_pddataframe_or_pdseries(obj)


def all_float_or_int(obj):
    if has_len(obj):
        return all([is_float_or_int(elem) for elem in obj])
    else:
        return is_float_or_int(obj)
    
    
def all_dataframe_or_series_or_ndarray(obj):
    if has_len(obj):
        return all([is_pddataframe_or_pdseries_or_ndarray(elem) for elem in obj])
    else:
        return is_pddataframe_or_pdseries_or_ndarray(obj)

