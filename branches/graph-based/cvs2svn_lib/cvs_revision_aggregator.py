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

"""This module contains the CVSRevisionAggregator class."""


from cvs2svn_lib.boolean import *
from cvs2svn_lib.set_support import *
from cvs2svn_lib.common import DB_OPEN_NEW
from cvs2svn_lib.common import DB_OPEN_READ
from cvs2svn_lib import config
from cvs2svn_lib.context import Ctx
from cvs2svn_lib.artifact_manager import artifact_manager
from cvs2svn_lib.line_of_development import Branch
from cvs2svn_lib.database import Database
from cvs2svn_lib.database import SDatabase
from cvs2svn_lib.persistence_manager import PersistenceManager
from cvs2svn_lib.cvs_commit import CVSCommit
from cvs2svn_lib.svn_commit import SVNSymbolCloseCommit


class CVSRevisionAggregator:
  """This class coordinates the committing of changesets and symbols."""

  def __init__(self):
    if not Ctx().trunk_only:
      self.last_revs_db = Database(
          artifact_manager.get_temp_file(config.SYMBOL_LAST_CVS_REVS_DB),
          DB_OPEN_READ)

    # List of CVSCommits that are ready to be committed, but might
    # need to be delayed until a CVSRevision with a later timestamp is
    # read.  (This can happen if the timestamp of the ready CVSCommit
    # had to be adjusted to make it later than its dependencies.)
    self.ready_queue = [ ]

    # A set of symbol ids for which the last source CVSRevision has
    # already been processed but which haven't been closed yet.
    self._pending_symbols = set()

    # A set containing the symbol ids of closed symbols.  That is,
    # we've already encountered the last CVSRevision that is a source
    # for that symbol, the final fill for this symbol has been done,
    # and we never need to fill it again.
    self._done_symbols = set()

    # This variable holds the most recently created primary svn_commit
    # object.  CVSRevisionAggregator maintains this variable merely
    # for its date, so that it can set dates for the SVNCommits
    # created in self._attempt_to_commit_symbols().
    self.latest_primary_svn_commit = None

    Ctx()._persistence_manager = PersistenceManager(DB_OPEN_NEW)

  def _attempt_to_commit_symbols(self):
    """Generate one SVNCommit for each symbol in self._pending_symbols
    that doesn't have an opening CVSRevision in self.ready_queue."""

    # Make a list of tuples (symbol_name, symbol) for all symbols from
    # self._pending_symbols that do not have *source* CVSRevisions in
    # the pending commit queue (self.ready_queue):
    closeable_symbols = []
    pending_commits = self.ready_queue[:]
    for symbol_id in self._pending_symbols:
      for cvs_commit in pending_commits:
        if cvs_commit.opens_symbol(symbol_id):
          break
      else:
        symbol = Ctx()._symbol_db.get_symbol(symbol_id)
        closeable_symbols.append( (symbol.name, symbol,) )

    # Sort the closeable symbols so that we will always process the
    # symbols in the same order, regardless of the order in which the
    # dict hashing algorithm hands them back to us.  We do this so
    # that our tests will get the same results on all platforms.
    closeable_symbols.sort()
    for (symbol_name, symbol,) in closeable_symbols:
      Ctx()._persistence_manager.put_svn_commit(
          SVNSymbolCloseCommit(symbol, self.latest_primary_svn_commit.date))
      self._done_symbols.add(symbol.id)
      self._pending_symbols.remove(symbol.id)

  def _commit_ready_commits(self):
    """Sort the commits from self.ready_queue by time, then process
    them in order."""

    self.ready_queue.sort()
    while self.ready_queue:
      cvs_commit = self.ready_queue.pop(0)
      self.latest_primary_svn_commit = \
          cvs_commit.process_revisions(self._done_symbols)
      self._attempt_to_commit_symbols()

  def process_changeset(self, changeset, timestamp):
    """Process CHANGESET, using TIMESTAMP for all of its entries.

    The changesets must be fed to this function in proper dependency
    order."""

    cvs_revs = list(changeset.get_cvs_items())

    metadata_id = cvs_revs[0].metadata_id

    author, log = Ctx()._metadata_db[metadata_id]
    cvs_commit = CVSCommit(metadata_id, author, log, timestamp)
    self.ready_queue.append(cvs_commit)

    for cvs_rev in cvs_revs:
      if Ctx().trunk_only and isinstance(cvs_rev.lod, Branch):
        continue

      # This is a kludge to force the timestamp for all revisions to
      # be the same:
      cvs_rev.timestamp = timestamp

      cvs_commit.add_revision(cvs_rev)

      # Add to self._pending_symbols any symbols from CVS_REV for
      # which CVS_REV is the last CVSRevision.
      if not Ctx().trunk_only:
        for symbol_id in self.last_revs_db.get('%x' % (cvs_rev.id,), []):
          self._pending_symbols.add(symbol_id)

    self._commit_ready_commits()


