# d1LocalCopy
A command line python script that creates a local copy of content from DataONE

Output is a folder with structure:
<pre>
  cache/
    meta.json: Basic metadata about the content in the cache
    index.json: An index to entries in the cache. Downlaods are renamed using a
                hash of the identifier as the identifier is not file system safe
    0/
    .
    .
    f/
</pre>

Note that this process runs as a single thread and so will take quite a while
to complete.

Note also that the libraries used emit error messages that may be more 
appropriately handled in logic. As a result the output from this script is
quite verbose, though seems to work effecively.

Dependencies:
<pre>
  pip install -U dataone.libclient 
  # should install downstream dependencies
</pre>
Use:
<pre>
  python d1_local_copy.py
</pre>
