# -*- coding: utf-8 -*-
"""
Created on Thu Jul 23 15:43:49 2015

@author: vieglais

Manages a local copy of content retrieved from DataONE.

Objects are stored as individual files in the designated folder. Files are 
named with a hash of the PID. Index of PID to filename is persisted as
a dictionary in JSON.

"""

import os
import logging
import hashlib
import json
import time
from d1_client import solr_client
from d1_client import d1client


VERSION="1.0.0"

class LocalCopyManager(object):

  CACHE_NAME="index.json"
  META_NAME="metadata.json"
  PERSIST_FREQUENCY=10

    
  def __init__(self, dest_folder="cache",
                     host="cn.dataone.org"):
    self.host = host                       
    baseurl = "https://" + self.host + "/cn"
    self.log = logging.getLogger(self.__class__.__name__)
    self.folder = dest_folder
    self.cache_name = os.path.join(self.folder, LocalCopyManager.CACHE_NAME)
    self.meta_name = os.path.join(self.folder, LocalCopyManager.META_NAME)
    try:
      if not os.path.exists(self.folder):
        os.makedirs(self.folder)
    except OSError as e:
      self.log.exception("Unable to create destination folder")
      raise(e)
      self.extension = ".xml"
    #catalog is a dict
    self._catalog = None
    self._solr_fl = "id,dateModified,formatId,formatType"
    self.d1cli = d1client.DataONEClient(cnBaseUrl = baseurl)
    self.meta = {"version":VERSION,
                 "created":self._getTStamp(),
                 "modified":self._getTStamp(),
                 "baseurl":baseurl,
                 "query":"",
                 "hits": 0,
                 "retrieved": 0,
                 "metadata":True,
                 "resourcemaps":False,
                 "data":False,
                 "catalog":self.cache_name}    

    
  def _getCatalog(self):
    if self._catalog is None:
      self._catalog = {}
    return self._catalog


  def persistCatalog(self):
    if self._catalog is None:
      return
    if isinstance(self._catalog, dict):
      fdest = open(self.meta['catalog'],"w")
      json.dump(self._catalog, fdest, indent=2)
      fdest.close()
        
      fdest = open(self.meta_name, "w")
      json.dump(self.meta, fdest, indent=2)
      fdest.close()
    

  def _closeCatalog(self):
    if self._catalog is None:
      return
    try:
      self.persistCatalog()
    except Exception as e:
      self.log.exception(e)
    finally:        
      self._catalog = None
    self._catalog = None
    
      
  def _getTStamp(self):
    '''Return a floating point representation of time.
    '''
    return time.time()


  def _PIDtoFName(self, pid):
    '''Return a filesystem friendly name without extension for the 
    provided PID.
    '''
    return hashlib.md5(pid).hexdigest()
  

  def extensionFromType(self, format_id):
    #TODO - expand this when dealing with stuff other than metadata
    return ".xml"

  
  def _downloadObject(self, obj):
    '''Given an identifier for the object, download it to the 
    storage area.
    '''

    def readInChunks(f, chunk_size=4096):
      while True:
        data = f.read(chunk_size)
        if not data:
          break
        yield data
    
    
    pid = obj['id']
    fname = self._PIDtoFName(pid) + self.extensionFromType(obj['formatId'])
    subfolder = os.path.join(self.folder, fname[0])
    if not os.path.exists(subfolder):
      os.makedirs(subfolder)
    fpath = os.path.join(subfolder, fname)
    self.log.debug("Add object {0}".format(fpath))

    self.log.info("Downloading %s  -->  %s", pid, fname)
    entry = {'touched': self._getTStamp(),
             'fname':fname, 
             'fpath':fpath,
             'status':0}

    #performs a resolve and a opens a stream to read the object
    try:
      fsrc = self.d1cli.get(pid)
      self.log.debug("Stream opened for %s", pid)
      entry['status'] = fsrc.status
      fdest = open(fpath, "w")
      for data in readInChunks(fsrc):
        fdest.write(data)
      fdest.close()
    except Exception as e:
      # failed to retreive from any resolved location. Record this as a 404
      # status, and keep chugging along
      self.log.exception(e)
      self.log.error("Unable to retrieve from any source: %s", pid)
      entry['status'] = 404    
    entry['formatid'] = obj['formatId']
    entry['datemodified'] = obj['dateModified']
    self._catalog[pid] = entry
    return entry

      
  def get(self, objects):
    '''Given an iterator that returns dictionaries with at least 'id' 
    populated with the object PID, downloads each object
    '''
    self._getCatalog()
    try:
      for obj in objects:
        logging.debug(obj)
        self.meta['retrieved'] = self.meta['retrieved'] + 1
        try:
          entry = self._catalog[obj['id']]
          entry['touched'] = self._getTStamp()
        except KeyError as e:
          # don't have object in cache
          entry = self._downloadObject(obj)
        if self.meta['retrieved'] % LocalCopyManager.PERSIST_FREQUENCY == 0:
          #Total number of matches. There's a bug (fixed in trunk) that 
          #overwrites the number of hits for the first page.
          self.meta['hits'] = objects._numhits 
          #Persist the catalog to disk
          self.persistCatalog()
          self.log.info("Retrieved %d entries", self.meta['retrieved'])
    except Exception as e:
      self.log.exception(e)
    finally:
      self._closeCatalog()
      
    
  def populate(self, query, max_records=1000):
    '''Given a solr query, populate the cache with records that match.
    '''
    # persistent has to be True, redmine #7299
    cli = solr_client.SolrConnection(host=self.host,
                                     persistent=True)
    # The response from this is an iterator that will send new requests to
    # retrieve further pages as necessary.
    objects = solr_client.SOLRSearchResponseIterator(cli, 
                                                     query, 
                                                     fields=self._solr_fl,
                                                     max_records=max_records)
    self.meta['query'] = query
    self.get(objects)


#==============================================================================

if __name__ == "__main__":
  logging.basicConfig(level=logging.DEBUG)
  host="cn.dataone.org"
  #BUG: escapeQueryTerm should be a module method, not a class method
  # redmine # 7298
  #cli = solr_client.SolrConnection()
  # Retrieve all METADATA with origin node urn:node:ONEShare < 130
  #q = "formatType:METADATA AND datasource:" + cli.escapeQueryTerm("urn:node:ONEShare")

  # Query to retrieve all METADATA entries that are not obsoleted
  q = "formatType:METADATA AND -obsoletedBy:[* TO *]"
  
  manager = LocalCopyManager(host=host)
  #populate the cache, limiting the total downloads to max_records
  manager.populate(q, max_records=100)
  
  