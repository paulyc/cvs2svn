# (Be in -*- python -*- mode.)
#
# ====================================================================
# Copyright (c) 2000-2006 CollabNet.  All rights reserved.
#
# This software is licensed as described in the file COPYING, which
# you should have received as part of this distribution.  The terms
# are also available at http://subversion.tigris.org/license-1.html.
# If newer versions of this license are posted there, you may use a
# newer version instead, at your option.
#
# This software consists of voluntary contributions made by many
# individuals.  For exact contribution history, see the revision
# history and logs, available at http://cvs2svn.tigris.org/.
# ====================================================================

"""This module contains a database that can store arbitrary CVSItems."""


from __future__ import generators

import struct
import cPickle

from cvs2svn_lib.boolean import *
from cvs2svn_lib.common import FatalError
from cvs2svn_lib.cvs_item import CVSRevision
from cvs2svn_lib.cvs_item import CVSBranch
from cvs2svn_lib.cvs_item import CVSTag
from cvs2svn_lib.primed_pickle import get_memos
from cvs2svn_lib.primed_pickle import PrimedPickler
from cvs2svn_lib.primed_pickle import PrimedUnpickler
from cvs2svn_lib.record_table import Packer
from cvs2svn_lib.record_table import FileOffsetPacker
from cvs2svn_lib.record_table import RecordTable
from cvs2svn_lib.database import DB_OPEN_NEW
from cvs2svn_lib.database import DB_OPEN_READ
from cvs2svn_lib.database import DB_OPEN_WRITE


class NewCVSItemStore:
  """A file of sequential CVSItems, grouped by CVSFile.

  The file consists of a sequence of pickles.  The zeroth one is an
  'unpickler_memo' as described in the primed_pickle module.
  Subsequent ones are pickled lists of CVSItems, each list containing
  all of the CVSItems for a single file.

  We don't use a single pickler for all items because the memo would
  grow too large."""

  def __init__(self, filename):
    """Initialize an instance, creating the file and writing the primer."""

    self.f = open(filename, 'wb')

    primer = (CVSRevision, CVSBranch, CVSTag,)
    (pickler_memo, unpickler_memo,) = get_memos(primer)
    self.pickler = PrimedPickler(pickler_memo)
    cPickle.dump(unpickler_memo, self.f, -1)

    self.current_file_id = None
    self.current_file_items = []

  def _flush(self):
    """Write the current items to disk."""

    if self.current_file_items:
      self.pickler.dumpf(self.f, self.current_file_items)
      self.current_file_id = None
      self.current_file_items = []

  def add(self, cvs_item):
    """Write cvs_item into the database."""

    if cvs_item.cvs_file.id != self.current_file_id:
      self._flush()
      self.current_file_id = cvs_item.cvs_file.id
    self.current_file_items.append(cvs_item)

  def close(self):
    self._flush()
    self.f.close()


class OldCVSItemStore:
  """Read a file created by NewCVSItemStore.

  The file must be read sequentially, except that it is possible to
  read old CVSItems from the current CVSFile."""

  def __init__(self, filename):
    self.f = open(filename, 'rb')

    # Read the memo from the first pickle:
    unpickler_memo = cPickle.load(self.f)
    self.unpickler = PrimedUnpickler(unpickler_memo)

    self.current_file_items = []
    self.current_file_map = {}

  def _read_file_chunk(self):
    self.current_file_items = self.unpickler.loadf(self.f)
    self.current_file_map = {}
    for item in self.current_file_items:
      self.current_file_map[item.id] = item

  def __iter__(self):
    while True:
      try:
        self._read_file_chunk()
      except EOFError:
        return
      for item in self.current_file_items:
        yield item

  def iter_file_item_maps(self):
    """Iterate through the items, one file at a time.

    Each time yield a map { id : CVSItem } for one CVSFile."""

    while True:
      try:
        self._read_file_chunk()
      except EOFError:
        return
      yield self.current_file_map.copy()

  def __getitem__(self, id):
    try:
      return self.current_file_map[id]
    except KeyError:
      raise FatalError(
          'Key %r not found within items currently accessible.' % (id,))


class IndexedCVSItemStore:
  """A file of CVSItems that is written sequentially and read randomly.

  The file consists of a sequence of pickles.  The zeroth one is a
  tuple (pickler_memo, unpickler_memo) as described in the
  primed_pickle module.  Subsequent ones are pickled CVSItems.  The
  offset of each CVSItem in the file is stored to an index table so
  that the data can later be retrieved randomly (via
  OldIndexedCVSItemStore).

  Items are always stored to the end of the file.  If an item is
  deleted or overwritten, the fact is recorded in the index_table but
  the space in the pickle file is not garbage-collected.  This has the
  advantage that one can create a modified version of a database that
  shares the main pickle file with an old version by copying the index
  file.  But it has the disadvantage that space is wasted whenever
  items are written multiple times."""

  def __init__(self, filename, index_filename, mode):
    """Initialize an instance, creating the files and writing the primer."""

    self.mode = mode
    if self.mode in [DB_OPEN_NEW, DB_OPEN_WRITE]:
      self.f = open(filename, 'wb+')
    else:
      self.f = open(filename, 'rb')

    self.index_table = RecordTable(
        index_filename, self.mode, FileOffsetPacker())

    if self.mode == DB_OPEN_NEW:
      primer = (CVSRevision, CVSBranch, CVSTag,)
      (pickler_memo, unpickler_memo,) = get_memos(primer)
      cPickle.dump((pickler_memo, unpickler_memo,), self.f, -1)
    else:
      # Read the memo from the first pickle:
      (pickler_memo, unpickler_memo,) = cPickle.load(self.f)

    self.pickler = PrimedPickler(pickler_memo)
    self.unpickler = PrimedUnpickler(unpickler_memo)

  def add(self, cvs_item):
    """Write cvs_item into the database."""

    # Make sure we're at the end of the file:
    self.f.seek(0, 2)
    self.index_table[cvs_item.id] = self.f.tell()
    self.pickler.dumpf(self.f, cvs_item)

  def _fetch(self, offset):
    self.f.seek(offset)
    return self.unpickler.loadf(self.f)

  def __iter__(self):
    for offset in self.index_table:
      if offset != 0:
        yield self._fetch(offset)

  def __getitem__(self, id):
    offset = self.index_table[id]
    if offset == 0:
      raise KeyError()
    return self._fetch(offset)

  def close(self):
    self.index_table.close()
    self.f.close()


