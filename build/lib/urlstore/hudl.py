#!/usr/bin/env python

import hashlib
import json
import logging
import os
import re
import time
import urllib
import urllib2
import urlparse

URL_DIR = '/var/hudl/urls'

class Response:
  def __init__(self, url, headers, data):
    self.url = url
    self.headers = headers
    self.data = data

class UrlStore:

  def __init__(self, cache_dir):
    self.cache_dir = cache_dir

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
    return os.path.join(URL_DIR, dirname, path)

  def url2headerpath(self, url):
    hx = self.hashurl(url)
    path = '%s.header' % hx
    dirname = hx[:2]
    return os.path.join(URL_DIR, dirname, path)

  def paths(self, url):
    return self.url2datapath(url), self.url2headerpath(url)

  def __contains__(self, url):
    datapath, headerpath = self.paths(url)
    return os.path.exists(datapath) and os.path.exists(headerpath)

  def _cache_get(self, url):
    datapath, headerpath = self.paths(url)
    try:
      with open(datapath) as f:
        data = f.read()
      with open(headerpath) as f:
        headers = json.loads(f.read())
      return Response(url, headers, data)
    except IOError:
      return None

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

    data = response.read()
    headers = response.info().dict

    datapath, headerpath = self.paths(url)

    def make_cache_subdir(path):
      D = os.path.dirname(path)
      if not os.path.exists(D):
        print 'making dir', D
        os.makedirs(D)

    map(make_cache_subdir, [datapath, headerpath])

    with open(datapath, 'w') as cache:
      cache.write(data)

    with open(headerpath, 'w') as cache:
      cache.write(json.dumps(response.info().dict))

    return Response(url, headers, data)

def gen_index_urls():
  for letter in 'abcdefghijklmnopqrstuvwxyz':
    yield INDEX_URL_TEMPLATE % letter


def bootstrap():
  if not os.path.exists(URL_DIR):
    os.makedirs(URL_DIR)

class ResponseParser(object):

  """ Take a response, yield new links and data """
  def __init__(self, response):
    self.response = response

  def match(self):
    raise NotImplementedError

  def data(self):
    yield

  def links(self):
    rx = re.compile('href="(.*?)"')
    links = (m.group(1) for m in rx.finditer(self.response.data))
    for link in links:
      yield urlparse.urljoin(self.response.url, link)

class IndexPageParser(ResponseParser):
  def match(self):
    return 'directory/teams' in self.response.url.lower()

def main():
  cache = UrlStore(URL_DIR)

  # Seed with index urls.
  index_urls = gen_index_urls()
  for index_url in index_urls:
    Q.append(index_url)

  # Set up response parsers.
  parsers = [
    IndexPageParser
  ]

  class data:
    urls = 0
    bytes = 0

  while len(Q):

    if not len(Q):
      print '.',
      time.sleep(NAP_TIME)
      continue

    url = Q.pop()
    response = cache.get(url)

    if response:
      data.urls += 1
      data.bytes += len(response.data)
      if data.urls % 100 == 0:
        print 'urls:', data.urls, 'bytes:', data.bytes

      for parser in parsers:
        p = parser(response)
        if p.match():
          for link in p.links():
            if link not in cache:
              Q.append(link)
          for patch in p.data():
            pass

if __name__ == '__main__':
  bootstrap()

  # main()
