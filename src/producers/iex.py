#==============================================================================#
#                                                                              #
#    Title: Title                                                              #
#    Sources:                                                                  #
#    - https://iexcloud.io/docs/api/#examples-on-how-to-make-a-signed-request  #
#                                                                              #
#==============================================================================#



#------------------------------------------------------------------------------#
#                                                                              #
#    Setup                                                                  ####
#                                                                              #
#------------------------------------------------------------------------------#


#------------------------------------------------------------------------------#
# Import Globals                                                            ####
#------------------------------------------------------------------------------#

import os, sys, datetime, requests, subprocess, hashlib, hmac#, os, base64


#------------------------------------------------------------------------------#
# Fix local issue                                                           ####
#------------------------------------------------------------------------------#

# Ensure the directory is correct... Every time. ----
for i in range(5):
    if not os.path.basename(os.getcwd()).lower() == "mdsi_bde_aut21_at3":
        os.chdir("..")
    else:
        break

# Ensure the current directory is in the system path. ---
if not os.path.abspath(".") in sys.path: sys.path.append(os.path.abspath("."))


#------------------------------------------------------------------------------#
# Local Imports                                                             ####
#------------------------------------------------------------------------------#

from src.secrets import secrets as sc


#------------------------------------------------------------------------------#
# Main Variables                                                            ####
#------------------------------------------------------------------------------#

method = "GET"
host = "cloud.iexapis.com"

# access_key = os.environ.get("IEX_PUBLIC_KEY")
# secret_key = os.environ.get("IEX_SECRET_KEY")
access_key = sc.iex_public_key
secret_key = sc.iex_secret_key

canonical_querystring = f"token={access_key}"
canonical_uri = "/v1/stock/aapl/company"
endpoint = f"https://{host}{canonical_uri}"


#------------------------------------------------------------------------------#
# Functions                                                                 ####
#------------------------------------------------------------------------------#

def sign(key, msg):
    return hmac.new(key.encode("utf-8"), msg.encode("utf-8"), hashlib.sha256).hexdigest()

def getSignatureKey(key, dateStamp):
    print(type(key.encode("utf-8")).__name__)
    print(type(dateStamp.encode("utf-8")).__name__)
    kDate = sign(key, dateStamp)
    return sign(kDate, "iex_request")

if access_key is None or secret_key is None:
    print("No access key is available.")
    sys.exit()


#------------------------------------------------------------------------------#
# Module Variables                                                          ####
#------------------------------------------------------------------------------#

t = datetime.datetime.utcnow()
iexdate = t.strftime("%Y%m%dT%H%M%SZ")
datestamp = t.strftime("%Y%m%d") # Date w/o time, used in credential scope
canonical_headers = f"host:{host}\nx-iex-date:{iexdate}\n"
signed_headers = "host;x-iex-date"
payload_hash = hashlib.sha256(("").encode("utf-8")).hexdigest()
canonical_request = "{method}\n{canonical_uri}\n{canonical_querystring}\n{canonical_headers}\n{signed_headers}\n{payload_hash}"
algorithm = "IEX-HMAC-SHA256"
credential_scope = f"{datestamp}/iex_request"
string_to_sign = f"{algorithm}\n{iexdate}\n{credential_scope}\n{hashlib.sha256(canonical_request.encode('utf-8')).hexdigest()}"
signing_key = getSignatureKey(secret_key, datestamp)
signature = hmac.new(signing_key.encode("utf-8"), (string_to_sign).encode("utf-8"), hashlib.sha256).hexdigest()
authorization_header = f"{algorithm} Credential={access_key}/{credential_scope}, SignedHeaders={signed_headers}, Signature={signature}"

headers = {"x-iex-date":iexdate, "Authorization":authorization_header}

request_url = endpoint + "?" + canonical_querystring



#------------------------------------------------------------------------------#
#                                                                              #
#    Do Work                                                                ####
#                                                                              #
#------------------------------------------------------------------------------#


if __name__ == "__main__":
    print("\nBEGIN REQUEST++++++++++++++++++++++++++++++++++++")
    print("Request URL = " + request_url)
    r = requests.get(request_url, headers=headers)

    print("\nRESPONSE++++++++++++++++++++++++++++++++++++")
    print("Response code: %d\n" % r.status_code)
    print(r.text)