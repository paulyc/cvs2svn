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

"""This module contains class PersistenceManager."""


from cvs2svn_lib.boolean import *
from cvs2svn_lib import config
from cvs2svn_lib.common import DB_OPEN_NEW
from cvs2svn_lib.common import DB_OPEN_READ
from cvs2svn_lib.common import SVN_INVALID_REVNUM
from cvs2svn_lib.log import Log
from cvs2svn_lib.context import Ctx
from cvs2svn_lib.artifact_manager import artifact_manager
from cvs2svn_lib.record_table import SignedIntegerPacker
from cvs2svn_lib.record_table import RecordTable
from cvs2svn_lib.database import PrimedPDatabase
from cvs2svn_lib.svn_commit import SVNRevisionCommit
from cvs2svn_lib.svn_commit import SVNInitialProjectCommit
from cvs2svn_lib.svn_commit import SVNPrimaryCommit
from cvs2svn_lib.svn_commit import SVNSymbolCommit
from cvs2svn_lib.svn_commit import SVNPostCommit


class PersistenceManager:
  """The PersistenceManager allows us to effectively store SVNCommits
  to disk and retrieve them later using only their subversion revision
  number as the key.  It also returns the subversion revision number
  for a given CVSRevision's unique key.

  All information pertinent to each SVNCommit is stored in a series of
  on-disk databases so that SVNCommits can be retrieved on-demand.

  MODE is one of the constants DB_OPEN_NEW or DB_OPEN_READ.
  In 'new' mode, PersistenceManager will initialize a new set of on-disk
  databases and be fully-featured.
  In 'read' mode, PersistenceManager will open existing on-disk databases
  and the set_* methods will be unavailable."""

  def __init__(self, mode):
    self.mode = mode
    if mode not in (DB_OPEN_NEW, DB_OPEN_READ):
      raise RuntimeError, "Invalid 'mode' argument to PersistenceManager"
    self.svn_commit_db = PrimedPDatabase(
        artifact_manager.get_temp_file(config.SVN_COMMITS_DB), mode,
        (SVNInitialProjectCommit, SVNPrimaryCommit, SVNSymbolCommit,
         SVNPostCommit,))
    self.cvs2svn_db = RecordTable(
        artifact_manager.get_temp_file(config.CVS_REVS_TO_SVN_REVNUMS),
        mode, SignedIntegerPacker(SVN_INVALID_REVNUM))

    # Symbol -> svn_revnum in which branch symbol was last filled.
    # This is used by SVNCommitCreator._pre_commit, to prevent
    # creating a fill revision which would have nothing to do.
    self._last_filled = {}

  def get_svn_revnum(self, cvs_rev_id):
    """Return the Subversion revision number in which CVS_REV_ID was
    committed, or SVN_INVALID_REVNUM if there is no mapping for
    CVS_REV_ID."""

    return self.cvs2svn_db.get(cvs_rev_id, SVN_INVALID_REVNUM)

  def get_svn_commit(self, svn_revnum):
    """Return an SVNCommit that corresponds to SVN_REVNUM.

    If no SVNCommit exists for revnum SVN_REVNUM, then return None.

    This method can throw SVNCommitInternalInconsistencyError."""

    return self.svn_commit_db.get('%x' % svn_revnum, None)

  def put_svn_commit(self, svn_commit):
    """Record the bidirectional mapping between SVN_REVNUM and
    CVS_REVS and record associated attributes."""

    Log().normal("Creating Subversion r%d (%s)"
                 % (svn_commit.revnum, svn_commit.description))

    if self.mode == DB_OPEN_READ:
      raise RuntimeError, \
          'Write operation attempted on read-only PersistenceManager'

    self.svn_commit_db['%x' % svn_commit.revnum] = svn_commit

    if isinstance(svn_commit, SVNRevisionCommit):
      for cvs_rev in svn_commit.cvs_revs:
        Log().verbose(' %s %s' % (cvs_rev.cvs_path, cvs_rev.rev,))
        self.cvs2svn_db[cvs_rev.id] = svn_commit.revnum

    # If it is a symbol commit, then record _last_filled.
    if isinstance(svn_commit, SVNSymbolCommit):
      self._last_filled[svn_commit.symbol] = svn_commit.revnum

  def filled(self, lod):
    """Return True iff LOD has ever been filled."""

    return lod.symbol in self._last_filled

  def filled_since(self, lod, svn_revnum):
    """Return True iff LOD has been filled since SVN_REVNUM."""

    return self._last_filled.get(lod.symbol, 0) >= svn_revnum
