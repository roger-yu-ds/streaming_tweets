#==============================================================================#
#                                                                              #
#    Title: Title                                                              #
#    Purpose: Purpose                                                          #
#    Notes: Notes                                                              #
#    Author: chrimaho                                                          #
#    Created: Created                                                          #
#    References: References                                                    #
#    Sources: Sources                                                          #
#    Edited: Edited                                                            #
#                                                                              #
#==============================================================================#



#------------------------------------------------------------------------------#
# Imports                                                                   ####
#------------------------------------------------------------------------------#

from src.utils import assertions as a
import re, inspect


#------------------------------------------------------------------------------#
# get_*() functions                                                         ####
#------------------------------------------------------------------------------#


def get_shape(object, return_str:bool=False):
    
    # Assertions
    if not a.has_shape(object):
        return "`object` has no shape."
    else:
        shape = object.shape
        
    # Determine whether or not to use strings
    if return_str:
        shape = " x ".join(str(dim) for dim in shape)
    
    # Return
    return shape


def get_list_proportions(lst:list):
    """
    Get the proportions of each occurance of a class from within a list
    """
    
    # Assertions
    assert a.is_list_or_ndarray(lst)
    
    # Set variables
    prop = {}
    dist = list(set(lst))
    
    # Get proportions
    for val in dist:
        prop[val] = sum(map(lambda x: x==val, lst))/len(lst)
    
    # Return
    return prop


def get_func_name():
    return inspect.stack()[1][3]



#------------------------------------------------------------------------------#
# String functions                                                          ####
#------------------------------------------------------------------------------#


def str_right(string:str, num_chars:int):
    """
    Sub-Select the right-most number of characters from a string
    """
    
    # Assertions
    assert a.is_str(string)
    assert a.is_int(num_chars)
    
    # Return
    return string[-num_chars:]


def str_left(string:str, num_chars:int):
    """
    Sub-Select the right-most number of characters from a string
    """
    
    # Assertions
    assert a.is_str(string)
    assert a.is_int(num_chars)
    
    # Return
    return string[:num_chars]


def str_is_match(regex_pattern:str, test_text:str):
    # https://stackoverflow.com/questions/9012008/pythons-re-return-true-if-string-contains-regex-pattern#answer-9012176
    
    # Assertions
    assert a.all_str([regex_pattern, test_text])
    
    # Do work
    pattern = re.compile(regex_pattern)
    result = pattern.search(test_text) is not None
    
    # Return
    return result


