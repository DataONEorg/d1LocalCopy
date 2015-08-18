# -*- coding: utf-8 -*-

'''Python command line tool to maange a local cache of content fom DataONE.

Output is a folder with structure:

  cache/
    meta.json: Basic metadata about the content in the cache
    index.json: An index to entries in the cache. Downlaods are renamed using a
                hash of the identifier as the identifier is not file system safe
    0/
    .
    .
    f/

Note that this process runs as a single thread and so will take quite a while
to complete.

Note also that the libraries used emit error messages that may be more 
appropriately handled in logic. As a result the output from this script is
quite verbose, though seems to work effecively.

Dependencies:

  pip install -U dataone.libclient 
  # should install downstream dependencies

Use:
  python d1_local_copy.py

'''

import logging
from d1_local_copy.local_copy_manager import LocalCopyManager

if __name__ == "__main__":
  logging.basicConfig(level=logging.INFO)

  # name of a folder that will contain the downloaded content,
  cache_folder="cache"  
  
  # hostname of coordinating node to use
  host="cn.dataone.org"

  # Query to retrieve all METADATA entries that are not obsoleted
  q = "formatType:METADATA AND -obsoletedBy:[* TO *]"
  
  manager = LocalCopyManager(host=host)
  #populate the cache, limiting the total downloads to max_records
  manager.populate(q, max_records=1000)
  
  
  
