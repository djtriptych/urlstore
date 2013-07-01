#!/usr/bin/env python

"""
  TODO:
    - Store link to response if we were redirected. For instance if requesting
      cnn.com takes us to www.cnn.com, we should hash the response under the
      final url, but store a link from cnn.com to www.cnn.com so it's still
      discoverable.
"""

import hashlib
import json
import logging
import os
import re
import time
import urllib
import urllib2
import urlparse

class CacheEntryError(Exception): pass

class Response:
  def __init__(self, url, headers, data):
    self.url = url
    self.headers = headers
    self.data = data


class UrlLink:

  def __init__(self, cache_dir):
    self.cache_dir = cache_dir
    self.link_dir = os.path.join(cache_dir, '.links')
    if not os.path.exists(self.link_dir):
      os.makedirs(self.link_dir)

  def add(self, orig, final):
    orig = UrlStore.hashurl(orig)
    final = UrlStore.hashurl(final)
    with open(orig, 'wb') as f:
      f.write(final)

  def all(self):
    for root, dirs, files in os.walk(self.link_dir):
      for path in files:
        with open(path) as link:
          orig = os.path.basename(path)
          final = link.read().strip()
          yield orig, final


class UrlStore:

  def __init__(self, cache_dir):
    self.cache_dir = cache_dir
    self.linker = UrlLink(cache_dir)

  def remove(self, url):
    """ remove url from cache """
    # just delete the files
    pass

  def hashurl(self, url):
    return hashlib.md5(url.lower()).hexdigest()

  def url2datapath(self, url):
    hx = self.hashurl(url)
    path = '%s.data' % hx
    dirname = hx[:2]
    return os.path.join(self.cache_dir, dirname, path)

  def url2headerpath(self, url):
    hx = self.hashurl(url)
    path = '%s.header' % hx
    dirname = hx[:2]
    return os.path.join(self.cache_dir, dirname, path)

  def paths(self, url):
    return self.url2datapath(url), self.url2headerpath(url)

  def __contains__(self, url):
    datapath, headerpath = self.paths(url)
    return os.path.exists(datapath) and os.path.exists(headerpath)

  def _response_from_files(self, datapath, headerpath):
    try:
      with open(datapath) as f:
        data = f.read()
      with open(headerpath) as f:
        headers = json.loads(f.read())
    except IOError, e:
      return None
    return Response(url, headers, data)

  def _cache_get(self, url):
    datapath, headerpath = self.paths(url)
    return self._response_from_files(datapath, headerpath)

  def get(self, url):
    """ Get url from data store, downloading if necessary"""

    response = self._cache_get(url)
    if not response:
      print '[mis]', url
      response = self._cache_add(url)
      if not response:
        print '[err]', url
        return None
    else:
      print '[hit]', url
    return response

  def _cache_add(self, url):
    """ Pull url from intarwebs, cache, and return """

    try:
      response = urllib2.urlopen(url)
    except urllib2.HTTPError, e:
      if 500 <= e.code <= 599:
        # Probably a bad URL
        return None

    # Read URL and decorate response with some meta information.
    data = response.read()
    headers = response.info().dict
    headers['x-original-url'] = url
    headers['x-final-url'] = response.url
    headers['x-fetch-date'] = time.time()
    headers['x-fetch-code'] = response.code

    if url != response.url:
      UrlLink(self.cache_dir).add(url, response.url)

    def make_cache_subdir(path):
      D = os.path.dirname(path)
      if not os.path.exists(D):
        print 'making dir', D
        os.makedirs(D)

    datapath, headerpath = self.paths(url)
    map(make_cache_subdir, [datapath, headerpath])

    with open(datapath, 'w') as cache:
      cache.write(data)

    with open(headerpath, 'w') as cache:
      cache.write(json.dumps(response.info().dict))

    return Response(url, headers, data)

  def _gen_responses(self):
    """ Generate all responses in cache_dir. """
    for root, subs, files in os.walk(self.cache_dir):
      for file in files:
        if file.endswith('.header'):
          base, ext = os.path.splitext(file)
          headerpath = file
          datapath = base + '.data'
          if not os.path.exists(datapath):
            raise CacheEntryError('Missing matching header or data')
          return self._response_from_files(datapath, headerpath)

  def gen_index():
    """ Scan every file in cache and compile an index dict.  """
    index = {}
    for response in self._gen_responses():
      url = response.headers['x-final-url']
      index[url] = response
    return index

def bootstrap():
  if not os.path.exists(URL_DIR):
    os.makedirs(URL_DIR)

def test():
  url = 'http://cnn.com'

  if not os.path.exists('.testcache'):
    os.makedirs('.testcache')
  url = UrlStore('.testcache')
  url.get('http://cnn.com')
  url.get('http://cnn.com')

if __name__ == '__main__':
  bootstrap()
