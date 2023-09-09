#------------------------------------------------------------------------------#
#                                                                              #
#    Setup                                                                  ####
#                                                                              #
#------------------------------------------------------------------------------#

import os



#------------------------------------------------------------------------------#
#                                                                              #
#    Overview                                                               ####
#                                                                              #
#------------------------------------------------------------------------------#


#------------------------------------------------------------------------------#
# Intro                                                                     ####
#------------------------------------------------------------------------------#

"""
This file is used to hold all of the keys and secrets.
You must ensure that the 'secret.py' file is added to the .gitignore file.
So that way it is not pushed to the Repo.
"""


#------------------------------------------------------------------------------#
# Methodology                                                               ####
#------------------------------------------------------------------------------#

"""
There are two main methods for handling secrets:
1. Add the secrets as variables in to a single file, then import that file for use in your other modules.
2. Add the secrets to global variables. See: https://able.bio/rhett/how-to-set-and-get-environment-variables-in-python--274rgt5
"""


#------------------------------------------------------------------------------#
# Instructions                                                              ####
#------------------------------------------------------------------------------#

"""
To set up and use this file, do the following:
1. COPY THIS FILE!!
    Do not add your keys directly to this file.
    Copy it and rename your copy to 'secrets.py'.
2. Add your keys.
3. In the module you want to import these keys into, add these lines to the top:
    `from src.secrets import secrets as sc`
    Then, you can call it like this:
    `sc.twitter_api_key`
"""




#------------------------------------------------------------------------------#
#                                                                              #
#    Twitter                                                                ####
#                                                                              #
#------------------------------------------------------------------------------#

"""
For getting & using Twitter Keys, do the following:
1. Create an App on the Twitter API website: https://apps.twitter.com/
    Basically that will give you keys that you need to use the Twitter API.
2. Add your keys to the `secrets.py` file.
"""


#------------------------------------------------------------------------------#
# Variable Method                                                           ####
#------------------------------------------------------------------------------#

twitter_api_key = '<get your own>'
twitter_api_secret = '<get your own>'
twitter_api_bearer_token = '<get your own>'
twitter_access_token = '<get your own>'
twitter_access_token_secret = '<get your own>'
twitter_consumer_key = '<get your own>'
twitter_consumer_secret = '<get your own>'


#------------------------------------------------------------------------------#
# Environment Method                                                        ####
#------------------------------------------------------------------------------#

os.environ["twitter_api_key"] = "<get your own>"
os.environ["twitter_api_secret"] = "<get your own>"
os.environ["twitter_api_bearer_token"] = "<get your own>"
os.environ["twitter_access_token"] = "<get your own>"
os.environ["twitter_access_token_secret"] = "<get your own>"
os.environ["twitter_consumer_key"] = "<get your own>"
os.environ["twitter_consumer_secret"] = "<get your own>"



#------------------------------------------------------------------------------#
#                                                                              #
#    IEX                                                                    ####
#                                                                              #
#------------------------------------------------------------------------------#

"""
1. Check details: https://iexcloud.io/docs/api/
2. Register on: https://iexcloud.io/cloud-login#/register
3. Select Starter plan (the free version)
4. Add your keys to the `secrets.py` file.
"""


#------------------------------------------------------------------------------#
# Variable                                                                  ####
#------------------------------------------------------------------------------#

iex_account_number = "<get your own>"
iex_api_token = "<get your own>"
iex_secret_key = "<get your own>"
iex_publishable_key = "<get your own>"