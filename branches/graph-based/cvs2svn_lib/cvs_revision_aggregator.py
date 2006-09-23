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
  """This class groups CVSRevisions into CVSCommits that represent
  at least one SVNCommit."""

  # How it works: CVSCommits are accumulated within an interval by
  # metadata_id (commit log and author).
  #
  # In a previous implementation, we would just close a CVSCommit for
  # further CVSRevisions and open a new CVSCommit if a second
  # CVSRevision with the same (CVS) path arrived within the
  # accumulation window.
  #
  # In the new code, there can be multiple open CVSCommits touching
  # the same files within an accumulation window.  A hash of pending
  # CVSRevisions with associated CVSCommits is maintained.  If a new
  # CVSRevision is found to have a prev_rev in this hash, the
  # corresponding CVSCommit is not eligible for accomodating the
  # revision, but will be added to the dependency list of the commit
  # the revision finally goes into.  When a CVSCommit moves out of its
  # accumulation window it is scheduled for flush immediately.
  # Timestamps are adjusted accordingly - it could happen that a small
  # CVSCommit is commited while a big commit it depends on is still
  # underway in other directories.

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

  def _commit_ready_commits(self, timestamp=None):
    """Sort the commits from self.ready_queue by time, then process
    them in order.  If TIMESTAMP is specified, only process commits
    that have timestamp previous to TIMESTAMP."""

    self.ready_queue.sort()
    while self.ready_queue and \
              (timestamp is None
               or self.ready_queue[0].time_range.t_max < timestamp):
      cvs_commit = self.ready_queue.pop(0)
      self.latest_primary_svn_commit = \
          cvs_commit.process_revisions(self._done_symbols)
      self._attempt_to_commit_symbols()

  def process_changeset(self, changeset, timestamp):
    """Process CHANGESET, using TIMESTAMP for all of its entries."""

    cvs_revs = list(changeset.get_cvs_items())

    metadata_id = cvs_revs[0].metadata_id

    author, log = Ctx()._metadata_db[metadata_id]
    cvs_commit = CVSCommit(metadata_id, author, log)
    self.ready_queue.append(cvs_commit)

    for cvs_rev in cvs_revs:
      if Ctx().trunk_only and isinstance(cvs_rev.lod, Branch):
        continue

      # This is a kludge to force aggregator to use the changesets
      # in the form that we feed it: @@@
      cvs_rev.timestamp = timestamp

      cvs_commit.add_revision(cvs_rev)

      # If there are any elements in the ready_queue at this point, they
      # need to be processed, because this latest rev couldn't possibly
      # be part of any of them.  Limit the timestamp of commits to be
      # processed, because re-stamping according to a commit's
      # dependencies can alter the commit's timestamp.
      self._commit_ready_commits(cvs_rev.timestamp)

      # Add to self._pending_symbols any symbols from CVS_REV for
      # which CVS_REV is the last CVSRevision.
      if not Ctx().trunk_only:
        for symbol_id in self.last_revs_db.get('%x' % (cvs_rev.id,), []):
          self._pending_symbols.add(symbol_id)

  def flush(self):
    """Commit anything left in self.cvs_commits.  Then inform the
    SymbolingsLogger that all commits are done."""

    self._commit_ready_commits()

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


