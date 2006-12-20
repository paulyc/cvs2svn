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

"""This module contains the SVNCommitCreator class."""


import time

from cvs2svn_lib.boolean import *
from cvs2svn_lib.set_support import *
from cvs2svn_lib import config
from cvs2svn_lib.common import warning_prefix
from cvs2svn_lib.common import DB_OPEN_NEW
from cvs2svn_lib.common import DB_OPEN_READ
from cvs2svn_lib.common import OP_ADD
from cvs2svn_lib.common import OP_CHANGE
from cvs2svn_lib.common import OP_DELETE
from cvs2svn_lib.log import Log
from cvs2svn_lib.context import Ctx
from cvs2svn_lib.artifact_manager import artifact_manager
from cvs2svn_lib.line_of_development import Branch
from cvs2svn_lib.database import Database
from cvs2svn_lib.changeset import OrderedChangeset
from cvs2svn_lib.svn_commit import SVNCommit
from cvs2svn_lib.svn_commit import SVNPrimaryCommit
from cvs2svn_lib.svn_commit import SVNPreCommit
from cvs2svn_lib.svn_commit import SVNPostCommit
from cvs2svn_lib.svn_commit import SVNSymbolCloseCommit


class SVNCommitCreator:
  """This class coordinates the committing of changesets and symbols."""

  def __init__(self):
    if not Ctx().trunk_only:
      self._last_changesets_db = Database(
          artifact_manager.get_temp_file(config.SYMBOL_LAST_CHANGESETS_DB),
          DB_OPEN_READ)

    # A set containing the closed symbols.  That is, we've already
    # encountered the last CVSRevision that is a source for that
    # symbol, the final fill for this symbol has been done, and we
    # never need to fill it again.
    self._done_symbols = set()

  def _commit_symbols(self, changeset_id, timestamp):
    """Generate SVNCommits for any symbols that are closed in all files.

    Generate a SVNSymbolCloseCommit for any symbols for which the
    changeset with id CHANGESET_ID is the last one that affects the
    symbol."""

    symbol_ids = self._last_changesets_db.get('%x' % (changeset_id,), [])
    symbols = set([
        Ctx()._symbol_db.get_symbol(symbol_id)
        for symbol_id in symbol_ids
        ])

    # Sort the closeable symbols so that we will always process the
    # symbols in the same order, regardless of the order in which the
    # dict hashing algorithm hands them back to us.  We do this so
    # that our tests will get the same results on all platforms.
    symbols = list(symbols)
    symbols.sort(lambda a, b: cmp(a.name, b.name))

    for symbol in symbols:
      Ctx()._persistence_manager.put_svn_commit(
          SVNSymbolCloseCommit(symbol, timestamp))
      self._done_symbols.add(symbol)

  def _fill_needed(cvs_rev):
    """Return True iff CVS_REV forces the branch to be filled.

    Return True if CVS_REV is the first commit on a new branch (for this
    file) and it requires that the branch be filled.  See comments below
    for the detailed rules."""

    if cvs_rev.first_on_branch_id is None:
      # Only commits that are the first on their branch can force fills:
      return False

    pm = Ctx()._persistence_manager

    # It should be the case that when we have a file F that is added on
    # branch B (thus, F on trunk is in state 'dead'), we generate an
    # SVNCommit to fill B iff the branch has never been filled before.
    if cvs_rev.op == OP_ADD:
      # Fill the branch only if it has never been filled before:
      return not pm.filled(cvs_rev.lod)
    elif cvs_rev.op == OP_CHANGE:
      # We need to fill only if the last commit affecting the file has
      # not been filled yet:
      return not pm.filled_since(
          cvs_rev.lod, pm.get_svn_revnum(cvs_rev.prev_id))
    elif cvs_rev.op == OP_DELETE:
      # If the previous revision was also a delete, we don't need to
      # fill it - and there's nothing to copy to the branch, so we can't
      # anyway.  No one seems to know how to get CVS to produce the
      # double delete case, but it's been observed.
      if Ctx()._cvs_items_db[cvs_rev.prev_id].op == OP_DELETE:
        return False
      # Other deletes need fills only if the last commit affecting the
      # file has not been filled yet:
      return not pm.filled_since(
          cvs_rev.lod, pm.get_svn_revnum(cvs_rev.prev_id))

  _fill_needed = staticmethod(_fill_needed)

  def _pre_commit(self, cvs_revs):
    """Generate any SVNCommits that must exist before the main commit."""

    # There may be multiple cvs_revs in this commit that would cause
    # branch B to be filled, but we only want to fill B once.  On the
    # other hand, there might be multiple branches committed on in this
    # commit.  Whatever the case, we should count exactly one commit per
    # branch, because we only fill a branch once per primary SVNCommit.
    # This list tracks which symbols we've already counted.
    accounted_for_symbols = set()
    secondary_commits = []

    for cvs_rev in cvs_revs:
      # If a commit is on a branch, we must ensure that the branch path
      # being committed exists (in HEAD of the Subversion repository).
      # If it doesn't exist, we will need to fill the branch.  After the
      # fill, the path on which we're committing will exist.
      if isinstance(cvs_rev.lod, Branch) \
          and cvs_rev.lod.symbol not in accounted_for_symbols \
          and cvs_rev.lod.symbol not in self._done_symbols \
          and self._fill_needed(cvs_rev):
        symbol = cvs_rev.lod.symbol
        secondary_commits.append(SVNPreCommit(symbol))
        accounted_for_symbols.add(symbol)

    return secondary_commits

  def _delete_needed(cvs_rev):
    """Return True iff the specified delete CVS_REV is really needed.

    When a file is added on a branch, CVS not only adds the file on the
    branch, but generates a trunk revision (typically 1.1) for that file
    in state 'dead'.  We only want to add this revision if the log
    message is not the standard cvs fabricated log message."""

    if cvs_rev.prev_id is not None:
      return True

    # cvs_rev.branch_ids may be empty if the originating branch has been
    # excluded.
    if not cvs_rev.branch_ids:
      return False
    # FIXME: This message will not match if the RCS file was renamed
    # manually after it was created.
    cvs_generated_msg = 'file %s was initially added on branch %s.\n' % (
        cvs_rev.cvs_file.basename,
        Ctx()._cvs_items_db[cvs_rev.branch_ids[0]].symbol.name,)
    author, log_msg = Ctx()._metadata_db[cvs_rev.metadata_id]
    return log_msg != cvs_generated_msg

  _delete_needed = staticmethod(_delete_needed)

  def _commit(self, timestamp, changes, deletes):
    """Generates the primary SVNCommit for a set of CVSRevisions.

    CHANGES and DELETES are the CVSRevisions to be included.  Use
    TIMESTAMP as the time of the commit (do not use the timestamps
    stored in the CVSRevisions)."""

    # Generate an SVNCommit unconditionally.  Even if the only change in
    # this group of CVSRevisions is a deletion of an already-deleted
    # file (that is, a CVS revision in state 'dead' whose predecessor
    # was also in state 'dead'), the conversion will still generate a
    # Subversion revision containing the log message for the second dead
    # revision, because we don't want to lose that information.
    needed_deletes = [
        cvs_rev
        for cvs_rev in deletes
        if self._delete_needed(cvs_rev)
        ]
    svn_commit = SVNPrimaryCommit(changes + needed_deletes, timestamp)

    default_branch_cvs_revisions = []
    for cvs_rev in changes:
      # Only make a change if we need to:
      if cvs_rev.rev == "1.1.1.1" and not cvs_rev.deltatext_exists:
        # When 1.1.1.1 has an empty deltatext, the explanation is almost
        # always that we're looking at an imported file whose 1.1 and
        # 1.1.1.1 are identical.  On such imports, CVS creates an RCS
        # file where 1.1 has the content, and 1.1.1.1 has an empty
        # deltatext, i.e, the same content as 1.1.  There's no reason to
        # reflect this non-change in the repository, so we want to do
        # nothing in this case.  (If we were really paranoid, we could
        # make sure 1.1's log message is the CVS-generated "Initial
        # revision\n", but I think the conditions above are strict
        # enough.)
        pass
      else:
        if cvs_rev.default_branch_revision:
          default_branch_cvs_revisions.append(cvs_rev)

    for cvs_rev in needed_deletes:
      if cvs_rev.default_branch_revision:
        default_branch_cvs_revisions.append(cvs_rev)

    # There is a slight chance that we didn't actually register any
    # CVSRevisions with our SVNCommit (see loop over deletes above), so
    # if we have no CVSRevisions, we don't flush the svn_commit to disk
    # and roll back our revnum.
    if svn_commit.cvs_revs:
      Ctx()._persistence_manager.put_svn_commit(svn_commit)
    else:
      # We will not be flushing this SVNCommit, so rollback the
      # SVNCommit revision counter.
      SVNCommit.revnum -= 1

    if not Ctx().trunk_only:
      for cvs_rev in changes + deletes:
        Ctx()._symbolings_logger.log_revision(cvs_rev, svn_commit.revnum)

    return svn_commit, default_branch_cvs_revisions

  def _post_commit(self, cvs_revs, motivating_revnum):
    """Generates any SVNCommits that we can perform following CVS_REVS.

    That is, handle non-trunk default branches.  Sometimes an RCS file
    has a non-trunk default branch, so a commit on that default branch
    would be visible in a default CVS checkout of HEAD.  If we don't
    copy that commit over to Subversion's trunk, then there will be no
    Subversion tree which corresponds to that CVS checkout.  Of course,
    in order to copy the path over, we may first need to delete the
    existing trunk there."""

    # Only generate a commit if we have default branch revs
    if cvs_revs:
      # Generate an SVNCommit for all of our default branch cvs_revs.
      svn_commit = SVNPostCommit(motivating_revnum, cvs_revs)
      for cvs_rev in cvs_revs:
        Ctx()._symbolings_logger.log_default_branch_closing(
            cvs_rev, svn_commit.revnum)
      return [svn_commit]
    else:
      return []

  def _process_revision_changeset(self, changeset, timestamp):
    """Process CHANGESET, using TIMESTAMP for all of its entries.

    Creating one or more SVNCommits in the process, and store them to
    the persistence manager.  CHANGESET must be an OrderedChangeset."""

    if not changeset.cvs_item_ids:
      Log().warn('Changeset has no items: %r' % changeset)
      return

    Log().verbose('-' * 60)
    Log().verbose('CVS Revision grouping:')
    Log().verbose('  Time: %s' % time.ctime(timestamp))

    cvs_revs = list(changeset.get_cvs_items())

    if Ctx().trunk_only:
      # Filter out non-trunk revisions:
      cvs_revs = [
          cvs_rev
          for cvs_rev in cvs_revs
          if not isinstance(cvs_rev.lod, Branch)]

    # Lists of CVSRevisions
    changes = []
    deletes = []

    for cvs_rev in cvs_revs:
      if cvs_rev.op == OP_DELETE:
        deletes.append(cvs_rev)
      else:
        # OP_CHANGE or OP_ADD
        changes.append(cvs_rev)

    if Ctx().trunk_only:
      # When trunk-only, only do the primary commit:
      self._commit(timestamp, changes, deletes)
    else:
      # This is a list of all non-primary SVNCommits motivated by the
      # main commit.  We gather these so that we can set their dates to
      # the same date as the primary commit.
      secondary_commits = []

      secondary_commits.extend(self._pre_commit(changes + deletes))

      # If some of the commits in this txn happened on a non-trunk
      # default branch, then those files will have to be copied into
      # trunk manually after being changed on the branch (because the
      # RCS "default branch" appears as head, i.e., trunk, in practice).
      # Unfortunately, Subversion doesn't support copies with sources in
      # the current txn.  All copies must be based in committed
      # revisions.  Therefore, we generate the copies in a new revision.
      #
      # default_branch_cvs_revisions is a list of cvs_revs for each
      # default branch commit that will need to be copied to trunk (or
      # deleted from trunk) in some generated revision following the
      # "regular" revision.
      motivating_commit, default_branch_cvs_revisions = self._commit(
          timestamp, changes, deletes)

      secondary_commits.extend(
          self._post_commit(
              default_branch_cvs_revisions, motivating_commit.revnum))

      for svn_commit in secondary_commits:
        svn_commit.date = timestamp
        Ctx()._persistence_manager.put_svn_commit(svn_commit)

    if not Ctx().trunk_only:
      self._commit_symbols(changeset.id, timestamp)

  def process_changeset(self, changeset, timestamp):
    """Process CHANGESET, using TIMESTAMP for all of its entries.

    The changesets must be fed to this function in proper dependency
    order."""

    if isinstance(changeset, OrderedChangeset):
      self._process_revision_changeset(changeset, timestamp)


