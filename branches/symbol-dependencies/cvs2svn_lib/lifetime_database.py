# (Be in -*- python -*- mode.)
#
# ====================================================================
# Copyright (c) 2007 CollabNet.  All rights reserved.
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

"""A database to keep track of the lifetimes of CVSItems."""


from __future__ import generators

import struct

from cvs2svn_lib.boolean import *
from cvs2svn_lib import config
from cvs2svn_lib.common import OP_ADD
from cvs2svn_lib.common import OP_CHANGE
from cvs2svn_lib.common import DB_OPEN_READ
from cvs2svn_lib.common import DB_OPEN_WRITE
from cvs2svn_lib.common import DB_OPEN_NEW
from cvs2svn_lib.common import FatalError
from cvs2svn_lib.context import Ctx
from cvs2svn_lib.artifact_manager import artifact_manager
from cvs2svn_lib.record_table import Packer
from cvs2svn_lib.record_table import RecordTable
from cvs2svn_lib.symbol import BranchSymbol
from cvs2svn_lib.symbol import TagSymbol


class Lifetime:
  """Represents the lifetime of a CVSItem in terms of SVN revisions."""

  def __init__(self, opening=None, closing=None):
    # The svn_revnum in which the CVSItem was created (or None if it
    # hasn't been created yet):
    self.opening = opening

    # The svn_revnum in which the CVSItem was overwritten or deleted,
    # and is therefore no longer in the repository (or None if it
    # hasn't been closed yet):
    self.closing = closing

  def __str__(self):
    return '[%s:%s]' % (self.opening or '', self.closing or '',)


class _LifetimePacker(Packer):
  format = '=2I'

  format_len = struct.calcsize(format)

  def __init__(self):
    Packer.__init__(self, self.format_len, self.pack(Lifetime()))

  def pack(self, v):
    return struct.pack(self.format, v.opening or 0, v.closing or 0)

  def unpack(self, s):
    (opening, closing) = struct.unpack(self.format, s)
    return Lifetime(opening or None, closing or None)


class LifetimeDatabase:
  """A database holding the lifetimes of CVSItems."""

  def __init__(self, mode):
    self.db = RecordTable(
        artifact_manager.get_temp_file(config.LIFETIME_DB), mode,
        _LifetimePacker())

  def close(self):
    self.db.close()

  def set_opening(self, cvs_item_id, svn_revnum):
    lifetime = self.db.get(cvs_item_id) or Lifetime()
    if lifetime.opening is not None:
      raise FatalError(
          '%s was already registered to open in revision %d'
          % (Ctx()._cvs_items_db[cvs_item_id], lifetime.opening,)
          )
    lifetime.opening = svn_revnum
    self.db[cvs_item_id] = lifetime

  def set_closing(self, cvs_item_id, svn_revnum):
    if False:
      # FIXME: We would eventually prefer this version, but it seems
      # to fail now in some OP_DELETE cases:
      try:
        lifetime = self.db[cvs_item_id]
      except KeyError:
        raise FatalError(
            'Closing registered before opening for %s' % (cvs_item_id,)
            )
    else:
      lifetime = self.db.get(cvs_item_id) or Lifetime()

    if lifetime.closing is not None:
      raise FatalError(
          '%s was already registered to close in revision %d'
          % (Ctx()._cvs_items_db[cvs_item_id], lifetime.closing,)
          )
    lifetime.closing = svn_revnum
    self.db[cvs_item_id] = lifetime

  def __getitem__(self, cvs_item_id):
    return self.db.get(cvs_item_id) or Lifetime()

  def get_openings_closings_map(self, svn_symbol_commit, svn_revnum):
    openings_closings_map = {}
    for cvs_symbol in svn_symbol_commit.get_cvs_items():
      cvs_rev = Ctx()._cvs_items_db[cvs_symbol.rev_id]
      if cvs_rev.op in [OP_ADD, OP_CHANGE]:
        value = self[cvs_rev.id]
        if value.opening is not None:
          if value.closing is not None and value.closing > svn_revnum:
            value.closing = None
          openings_closings_map[cvs_rev.get_svn_path()] = value

    return openings_closings_map


