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


import sys, os, requests, shutil, gzip
import logging as lg
import pandas as pd
from src.utils import assertions as a, misc as ms



#------------------------------------------------------------------------------#
# File interactions                                                         ####
#------------------------------------------------------------------------------#


def get_filename(full_path:str):
    
    # Begin log
    lg.info(f"BEGIN {ms.get_func_name()}()")
    
    # Assertions
    assert a.is_str(full_path)
    
    # Do work
    filename = os.path.basename(full_path)
    
    # End log
    lg.info(f"END {ms.get_func_name()}()")
            
    # Return
    return filename


def get_filename_no_extension(full_path:str):
    
    # Begin log
    lg.info(f"BEGIN {ms.get_func_name()}()")
    
    # Assertions
    assert a.is_str(full_path)
    
    # Do work
    filename = get_filename(full_path).split(".")[0]
    
    # End log
    lg.info(f"END {ms.get_func_name()}()")
            
    # Return
    return filename


def get_fullname_of_all_files_in_dir(dir:str):
    
    # Begin log
    lg.info(f"BEGIN {ms.get_func_name()}()")
    
    # Assertions
    assert a.is_str(dir)
    assert a.is_path_valid(dir)
    
    # Do work
    fullnames = [os.path.join(dir, file) for file in os.listdir(dir)]
    
    # End log
    lg.info(f"END {ms.get_func_name()}()")
            
    # Return
    return fullnames


def read(path:str):
    
    # Begin log
    lg.info(f"BEGIN {ms.get_func_name()}()")

    # Assertions
    assert a.is_str(path)
    assert a.is_path_valid(path)
    
    # Read
    try:
        f = open(path, "rt")
        data = f.read()
    except:
        e = sys.exc_info()[0]
        lg.error(e)
        raise 
    finally:
        f.close()

    # End log
    lg.info(f"END {ms.get_func_name()}()")
            
    # Return
    return data


def save(object, path:str):

    # Begin log
    lg.info(f"BEGIN {ms.get_func_name()}()")
    
    # Assertions
    assert a.is_str(path)
    assert a.is_path_valid(path)
    
    # Save
    try:
        f = open(path, "wt")
        f.write(object)
    except:
        e = sys.exc_info()[0]
        lg.error(e)
        raise 
    finally:
        f.close()
    
    # End log
    lg.info(f"END {ms.get_func_name()}()")

    # Return
    return None


# read_csv ----
def read_csv(path:str):
    
    # Begin log
    lg.info(f"BEGIN {ms.get_func_name()}()")
    
    # Assertions
    assert a.is_str(path)
    assert a.is_path_valid(path)
    assert a.is_path_csv(path)
    
    # Do work
    try:
        data = pd.read_csv(path)
    except:
        e = sys.exc_info()[0]
        lg.error(e)
        raise 
    
    # End log
    lg.info(f"Data shape: {data.shape}")
    lg.info(f"END {ms.get_func_name()}()")
    
    # Return
    return data


def unzip(from_file:str, to_file:str):

    # Begin log
    lg.info(f"BEGIN {ms.get_func_name()}()")
    
    # Assertions
    assert a.all_str([from_file,to_file])
    assert a.all_path_dir_valid([from_file, to_file])
    assert a.is_path_valid(from_file)
    
    # Do work
    try:
        f = gzip.open(from_file, "rb")
        t = open(to_file, "wb")
        shutil.copyfileobj(f, t)
    except:
        e = sys.exc_info()[0]
        lg.error(e)
        raise 
    finally:
        f.close()
        t.close()

    # End log
    lg.info(f"END {ms.get_func_name()}()")
    
    # Return
    return None


def download_write(url:str
    , path:str
    , filename_prefix:str=None
    ):
    
    # Begin log
    lg.info(f"BEGIN {ms.get_func_name()}()")
    
    # Assertions
    assert a.all_str([url, path, filename_prefix])
    assert a.is_url_valid(url)
    assert a.is_path_valid(path)
    
    # Get filename
    filename = url.split("/")[-1]
    if filename_prefix is not None:
        filename = filename_prefix + "_" + filename
    fullname = os.path.join(path, filename)
    
    # Work
    try:
        f = open(fullname, "wb")
        r = requests.get(url)
        f.write(r.content)
    except:
        e = sys.exc_info()[0]
        lg.error(e)
        raise 
    finally:
        f.close()
        lg.info(f"Successfully downloaded '{filename}' and saved to '{path}'")
        
    # End log
    lg.info(f"END {ms.get_func_name()}()")
    
    # Return
    return fullname


def download_unzip(url:str
    , path:str
    , delete_zip:bool=True
    , filename_prefix:str=None
    ):

    # Begin log
    lg.info(f"BEGIN {ms.get_func_name()}()")
    
    # Assertions
    assert a.all_str([url, path, filename_prefix])
    assert a.is_url_valid(url)
    assert a.is_path_valid(path)
    assert a.is_bool(delete_zip)
    
    # Download
    from_full_name = download_write(url, path, filename_prefix)
    
    # Get paths
    file_path = os.path.dirname(from_full_name)
    from_file_name = os.path.split(from_full_name)[-1]
    to_file_name = ".".join(from_file_name.split(".")[:-1])
    to_full_name = os.path.join(file_path, to_file_name)
    
    # Unzip
    unzip(from_full_name, to_full_name)
    
    # You sure you wanna delete me? Punk?
    if delete_zip:
        os.remove(from_full_name)

    # End log
    lg.info(f"END {ms.get_func_name()}()")
    
    # Return
    return to_full_name


def download_write_loop \
    ( url_format:str
    , dates:list
    , path:str
    , delete_zip:bool=True
    , early_exit:bool=False
    ):
    
    # Begin log
    lg.info(f"BEGIN {ms.get_func_name()}()")

    # Assertions
    assert a.is_list(dates)
    assert a.all_str([url_format, path]+dates)
    assert a.all_bool([delete_zip,early_exit])
    assert "{}" in url_format
    
    # Early exit
    if early_exit:
        lg.warn("Function was exited early. Files were not re-downloaded")
    else:    
        # Loopie loop
        for date in dates:
            url = url_format.format(date)
            download_unzip(url, path, delete_zip, date)
    
    # End log
    lg.info(f"END {ms.get_func_name()}()")
    
    # Return
    return None
