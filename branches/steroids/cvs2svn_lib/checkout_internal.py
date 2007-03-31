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

"""This module contains classes that implement the --use-internal-co option.

The idea is to patch up the revisions' contents incrementally, thus avoiding
the O(n^2) overhead of "co" and "cvs".

InternalRevisionRecorder saves the RCS deltas and RCS revision trees to
databases.  Notably, deltas from the trunk need to be reversed, as CVS
stores them so they apply from HEAD backwards.

InternalRevisionExcluder copies the revision trees to a new database, but
omits excluded branches.

InternalRevisionReader does the actual checking out of the revisions'
contents. The current content of each line of development (LOD) which still
has commits pending is kept in a database.  When the next revision is
requested, the current state is fetched and the delta is applied. This is
very fast compared to "co" which is invoked each time and checks out each
revision from scratch starting at HEAD.  It is important that each revision
recorded in the revision tree is requested exactly once, as otherwise the
reference counting will never dispose the ignored revisions' content copy.
So InternalRevisionRecorder skips deleted revisions at the ends of LODs,
InternalRevisionExcluder skips excluded branches and InternalRevisionReader
provides the skip_content method to skip unused 1.1.1.1 revisions."""

from __future__ import generators

import cStringIO
import re
import types

from cvs2svn_lib import config
from cvs2svn_lib.common import DB_OPEN_NEW
from cvs2svn_lib.common import DB_OPEN_READ
from cvs2svn_lib.common import warning_prefix
from cvs2svn_lib.artifact_manager import artifact_manager
from cvs2svn_lib.collect_data import is_trunk_revision
from cvs2svn_lib.context import Ctx
from cvs2svn_lib.database import SDatabase
from cvs2svn_lib.database import IndexedDatabase
from cvs2svn_lib.log import Log
from cvs2svn_lib.rcs_stream import RCSStream
from cvs2svn_lib.revision_recorder import RevisionRecorder
from cvs2svn_lib.revision_excluder import RevisionExcluder
from cvs2svn_lib.revision_reader import RevisionReader
from cvs2svn_lib.serializer import PrimedPickleSerializer

class InternalRevisionRecorder(RevisionRecorder):
  """A RevisionRecorder that reconstructs the full text internally."""

  def register_artifacts(self, which_pass):
    which_pass._register_temp_file(config.RCS_DELTAS_INDEX_TABLE)
    which_pass._register_temp_file(config.RCS_DELTAS_STORE)
    which_pass._register_temp_file(config.RCS_TREES_INDEX_TABLE)
    which_pass._register_temp_file(config.RCS_TREES_STORE)

  def start(self):
    self._rcs_deltas = IndexedDatabase(
        artifact_manager.get_temp_file(config.RCS_DELTAS_STORE),
        artifact_manager.get_temp_file(config.RCS_DELTAS_INDEX_TABLE),
        DB_OPEN_NEW, PrimedPickleSerializer(None))
    self._rcs_trees = IndexedDatabase(
        artifact_manager.get_temp_file(config.RCS_TREES_STORE),
        artifact_manager.get_temp_file(config.RCS_TREES_INDEX_TABLE),
        DB_OPEN_NEW, PrimedPickleSerializer(None))

  def start_file(self, cvs_file):
    self._cvs_file = cvs_file

  def record_text(self, revisions_data, revision, log, text):
    revision_data = revisions_data[revision]
    if is_trunk_revision(revision):
      # On trunk, deltas are inverted.
      if revision_data.child is None: # HEAD has no children.
        # HEAD is the first revision to be delivered - as full text.
        self._stream = RCSStream(text)
      else:
        # Any other trunk revision is a backward delta.
        self._writeout(
            revisions_data[revision_data.child],
            self._stream.invert_diff(text))
      if revision_data.parent is None:
        self._writeout(revision_data, self._stream.get_text())
        # There will be no further trunk revisions delivered.
        del self._stream
    elif not Ctx().trunk_only:
      # On branches, we have forward deltas.
      self._writeout(revision_data, text)

    return None

  def _writeout(self, revision_data, text):
    self._rcs_deltas[revision_data.cvs_rev_id] = text

  def finish_file(self, revisions_data, root_rev):
    self._rcs_trees[self._cvs_file.id] = list(
        self._get_lods(revisions_data, root_rev, not Ctx().trunk_only))
    del self._cvs_file

  def _get_lods(self, revs_data, revision, do_branch):
    """Generate an efficient representation of the revision tree of a
    LOD and its subbranches.

    REVS_DATA is a map { rev : _RevisionData }, REVISION the first
    revision number on a LOD, and DO_BRANCH a flag indicating whether
    subbranches should be entered recursively.

    Yield the LODs under REVISION, one LOD at a time, from leaf
    towards trunk.  Each LOD is returned as a list of cvs_revision_ids
    of revisions on the LOD, in reverse chronological order.
    Revisions that represent deletions at the end of an LOD are
    omitted.  For non-trunk LODs, the last item in the list is the cvs
    revision id of the revision from which the LOD sprouted."""

    # The last CVSItem on the current LOD from which live branches sprout.
    last_used_rev = None
    # List of CVSItems on current LOD.
    lod_revs_data = []

    while revision is not None:
      rev_data = revs_data[revision]
      lod_revs_data.append(rev_data)
      if do_branch:
        for branch in rev_data.branches_revs_data:
          for sub_lod in self._get_lods(revs_data, branch, True):
            yield sub_lod
            last_used_rev = rev_data
      revision = rev_data.child

    # Pop revisions that will never be fetched off the branch ends as
    # otherwise they would fill up the checkout.
    while lod_revs_data and lod_revs_data[-1].state == 'dead' \
        and lod_revs_data[-1] is not last_used_rev:
      del lod_revs_data[-1]

    if lod_revs_data:
      lod_rev_ids = [rev_data.cvs_rev_id for rev_data in lod_revs_data]
      lod_rev_ids.reverse()
      if lod_revs_data[0].parent is not None:
        lod_rev_ids.append(revs_data[lod_revs_data[0].parent].cvs_rev_id)
      yield lod_rev_ids

  def finish(self):
    self._rcs_deltas.close()
    self._rcs_trees.close()


class InternalRevisionExcluder(RevisionExcluder):
  """The RevisionExcluder used by InternalRevisionReader."""

  def register_artifacts(self, which_pass):
    which_pass._register_temp_file_needed(config.RCS_TREES_STORE)
    which_pass._register_temp_file_needed(config.RCS_TREES_INDEX_TABLE)
    which_pass._register_temp_file(config.RCS_TREES_FILTERED_STORE)
    which_pass._register_temp_file(config.RCS_TREES_FILTERED_INDEX_TABLE)

  def start(self):
    self._tree_db = IndexedDatabase(
        artifact_manager.get_temp_file(config.RCS_TREES_STORE),
        artifact_manager.get_temp_file(config.RCS_TREES_INDEX_TABLE),
        DB_OPEN_READ)
    self._new_tree_db = IndexedDatabase(
        artifact_manager.get_temp_file(config.RCS_TREES_FILTERED_STORE),
        artifact_manager.get_temp_file(config.RCS_TREES_FILTERED_INDEX_TABLE),
        DB_OPEN_NEW, PrimedPickleSerializer(None))

  def start_file(self, cvs_file):
    self._id = cvs_file.id
    self._lods = {}
    for lod in self._tree_db[self._id]:
      self._lods[lod[0]] = lod

  def exclude_tag(self, cvs_tag):
    pass

  def exclude_branch(self, cvs_branch, cvs_revisions):
    for i in range(len(cvs_revisions) - 1, -1, -1):
      r = cvs_revisions[i].id
      if self._lods.has_key(r):
        del self._lods[r]
        return

  def finish_file(self):
    self._new_tree_db[self._id] = self._lods.values()

  def skip_file(self, cvs_file):
    self._new_tree_db[cvs_file.id] = self._tree_db[cvs_file.id]

  def finish(self):
    self._tree_db.close()
    self._new_tree_db.close()


class InternalRevisionReader(RevisionReader):
  """A RevisionReader that reads the contents from an own delta store."""

  _kw_re = re.compile(
      r'\$(' +
      r'Author|Date|Header|Id|Name|Locker|Log|RCSfile|Revision|Source|State' +
      r'):[^$\n]*\$')

  class _Rev: pass

  def __init__(self):
    pass

  def register_artifacts(self, which_pass):
    which_pass._register_temp_file(config.CVS_CHECKOUT_DB)
    which_pass._register_temp_file_needed(config.RCS_DELTAS_STORE)
    which_pass._register_temp_file_needed(config.RCS_DELTAS_INDEX_TABLE)
    which_pass._register_temp_file_needed(config.RCS_TREES_FILTERED_STORE)
    which_pass._register_temp_file_needed(
        config.RCS_TREES_FILTERED_INDEX_TABLE)

  def get_revision_recorder(self):
    return InternalRevisionRecorder()

  def get_revision_excluder(self):
    return InternalRevisionExcluder()

  def start(self):
    self._delta_db = IndexedDatabase(
        artifact_manager.get_temp_file(config.RCS_DELTAS_STORE),
        artifact_manager.get_temp_file(config.RCS_DELTAS_INDEX_TABLE),
        DB_OPEN_READ)
    self._tree_db = IndexedDatabase(
        artifact_manager.get_temp_file(config.RCS_TREES_FILTERED_STORE),
        artifact_manager.get_temp_file(config.RCS_TREES_FILTERED_INDEX_TABLE),
        DB_OPEN_READ)
    self._co_db = SDatabase(
          artifact_manager.get_temp_file(config.CVS_CHECKOUT_DB), DB_OPEN_NEW)
    self._files = {}

  def _init_file(self, id):
    """Read in the revision tree of the CVSFile with the id ID."""

    revs = {}
    for line in self._tree_db[id]:
      prv = None
      for r in line:
        rev = revs.get(r, None)
        if rev is None:
          rev = InternalRevisionReader._Rev()
          rev.ref = 0
          rev.prev = None
          revs[r] = rev
        if prv:
          revs[prv].prev = r
          rev.ref += 1
        prv = r
    return revs

  def _checkout_rev(self, revs, r, deref):
    """Workhorse of the checkout process. Recurses if a revision was skipped.
    """

    rev = revs[r]
    if rev.prev:
      # This is not the root revision so we need an ancestor.
      prev = revs[rev.prev]
      try:
        text = self._co_db[str(rev.prev)]
      except KeyError:
        # The previous revision was skipped. Fetch it now.
        co = self._checkout_rev(revs, rev.prev, 1)
      else:
        # The previous revision was already checked out.
        co = RCSStream(text)
        prev.ref -= 1
        if not prev.ref:
          # The previous revision will not be needed any more.
          del revs[rev.prev]
          del self._co_db[str(rev.prev)]
      co.apply_diff(self._delta_db[r])
    else:
      # Root revision - initialize checkout.
      co = RCSStream(self._delta_db[r])
    rev.ref -= deref
    if rev.ref:
      # Revision has descendants.
      text = co.get_text()
      self._co_db[str(r)] = text
      if not deref:
        return text
    else:
      # Revision is branch head.
      del revs[r]
      if not deref:
        return co.get_text()
    return co

  def _do_checkout(self, c_rev, revs, suppress_keyword_substitution):
    rv = self._checkout_rev(revs, c_rev.id, 0)
    if suppress_keyword_substitution:
      return re.sub(self._kw_re, r'$\1$', rv)
    return rv

  def _checkout(self, c_rev, suppress_keyword_substitution):
    """Check out the revision C_REV from the repository.

    If SUPPRESS_KEYWORD_SUBSTITUTION is True, any RCS keywords will be
    _un_expanded prior to returning the file content.
    Note that $Log$ never actually generates a log (makes test 68 fail).

    Revisions must be requested in the order they appear on the branches.
    Revisions except the last one on a branch may be skipped.
    Each revision may be requested only once."""

    try:
      revs = self._files[c_rev.cvs_file.id]
      # The file is already active ...
      rv = self._do_checkout(c_rev, revs, suppress_keyword_substitution)
      if not revs:
        # ... and will not be needed any more.
        del self._files[c_rev.cvs_file.id]
    except KeyError:
      # The file is not active yet ...
      revs = self._init_file(c_rev.cvs_file.id)
      rv = self._do_checkout(c_rev, revs, suppress_keyword_substitution)
      if revs:
        # ... and will be needed again.
        self._files[c_rev.cvs_file.id] = revs
    return rv

  def get_content_stream(self, cvs_rev, suppress_keyword_substitution=False):
    return cStringIO.StringIO(
        self._checkout(cvs_rev, suppress_keyword_substitution))

  def skip_content(self, cvs_rev):
    # A dedicated .skip() function doesn't seem worth it
    self._checkout(cvs_rev, False)

  def finish(self):
    if self._files:
      Log().warn(
         "%s: internal problem: leftover revisions in the checkout cache:" % \
         warning_prefix)
      for file in self._files:
        msg = Ctx()._cvs_file_db.get_file(file).cvs_path + ':'
        for r in self._files[file]:
          # This does not work, as we have only the filtered item database
          # at hand.  The non-filtered one is long gone and is not indexed
          # anyway.
          #msg += " %s" % Ctx()._cvs_items_db[r].rev
          msg += " %d" % r
        Log().warn(msg)

    self._delta_db.close()
    self._tree_db.close()
    self._co_db.close()

