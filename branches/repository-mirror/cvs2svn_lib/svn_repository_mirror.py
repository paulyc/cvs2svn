# (Be in -*- python -*- mode.)
#
# ====================================================================
# Copyright (c) 2000-2007 CollabNet.  All rights reserved.
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

"""This module contains the SVNRepositoryMirror class."""


import sys
import bisect

from cvs2svn_lib.boolean import *
from cvs2svn_lib import config
from cvs2svn_lib.common import InternalError
from cvs2svn_lib.context import Ctx
from cvs2svn_lib.cvs_file import CVSDirectory
from cvs2svn_lib.cvs_file import CVSFile
from cvs2svn_lib.symbol import Trunk
from cvs2svn_lib.svn_commit_item import SVNCommitItem
from cvs2svn_lib.repository_mirror import RepositoryMirror
from cvs2svn_lib.repository_mirror import LODExistsError
from cvs2svn_lib.repository_mirror import PathExistsError
from cvs2svn_lib.repository_mirror import CurrentMirrorSubdirectory


class SVNRepositoryMirror:
  """Mirror a Subversion repository and its history."""

  class ParentMissingError(Exception):
    """The parent of a path is missing.

    Exception raised if an attempt is made to add a path to the
    repository mirror but the parent's path doesn't exist in the
    youngest revision of the repository."""

    pass

  class ExpectedDirectoryError(Exception):
    """A file was found where a directory was expected."""

    pass

  def __init__(self):
    self._mirror = RepositoryMirror()

  def register_artifacts(self, which_pass):
    """Register the artifacts that will be needed for this object."""

    self._mirror.register_artifacts(which_pass)

  def open(self):
    """Set up the SVNRepositoryMirror and prepare it for SVNCommits."""

    self._mirror.open()
    self._delegates = []

  def start_commit(self, revnum, revprops):
    """Start a new commit."""

    self._mirror.start_commit(revnum)
    self._invoke_delegates('start_commit', revnum, revprops)

  def end_commit(self):
    """Called at the end of each commit.

    This method copies the newly created nodes to the on-disk nodes
    db."""

    self._mirror.end_commit()
    self._invoke_delegates('end_commit')

  def delete_lod(self, lod):
    """Delete the main path for LOD from the tree.

    The path must currently exist.  Silently refuse to delete trunk
    paths."""

    if isinstance(lod, Trunk):
      # Never delete a Trunk path.
      return

    self._mirror.get_current_lod_directory(lod).delete()
    self._invoke_delegates('delete_lod', lod)

  def delete_path(self, cvs_path, lod, should_prune=False):
    """Delete CVS_PATH from LOD."""

    if cvs_path.parent_directory is None:
      self.delete_lod(lod)
      return

    parent_node = self._mirror.get_current_path(
        cvs_path.parent_directory, lod
        )
    del parent_node[cvs_path]
    self._invoke_delegates('delete_path', lod, cvs_path)

    if should_prune:
      while parent_node is not None and len(parent_node) == 0:
        # A drawback of this code is that we issue a delete for each
        # path and not just a single delete for the topmost directory
        # pruned.
        node = parent_node
        cvs_path = node.cvs_path
        if cvs_path.parent_directory is None:
          parent_node = None
          self.delete_lod(lod)
        else:
          parent_node = node.parent_mirror_dir
          node.delete()
          self._invoke_delegates('delete_path', lod, cvs_path)

  def initialize_project(self, project):
    """Create the basic structure for PROJECT."""

    self._invoke_delegates('initialize_project', project)

    # Don't invoke delegates.
    self._mirror.add_lod(project.get_trunk())

  def change_path(self, cvs_rev):
    """Register a change in self._youngest for the CVS_REV's svn_path."""

    # We do not have to update the nodes because our mirror is only
    # concerned with the presence or absence of paths, and a file
    # content change does not cause any path changes.
    self._invoke_delegates('change_path', SVNCommitItem(cvs_rev, False))

  def _mkdir_p(self, cvs_directory, lod):
    """Make sure that CVS_DIRECTORY exists in LOD.

    If not, create it, calling delegates.  Return the node for
    CVS_DIRECTORY."""

    try:
      node = self._mirror.get_current_lod_directory(lod)
    except KeyError:
      node = self._mirror.add_lod(lod)
      self._invoke_delegates('initialize_lod', lod)

    for sub_path in cvs_directory.get_ancestry()[1:]:
      try:
        node = node[sub_path]
      except KeyError:
        node = node.mkdir(sub_path)
        self._invoke_delegates('mkdir', lod, sub_path)
      if node is None:
        raise self.ExpectedDirectoryError(
            'File found at \'%s\' where directory was expected.' % (sub_path,)
            )

    return node

  def add_path(self, cvs_rev):
    """Add the CVS_REV's svn_path to the repository mirror.

    Create any missing intermediate paths."""

    cvs_file = cvs_rev.cvs_file
    parent_path = cvs_file.parent_directory
    lod = cvs_rev.lod
    parent_node = self._mkdir_p(parent_path, lod)
    parent_node.add_file(cvs_file)
    self._invoke_delegates('add_path', SVNCommitItem(cvs_rev, True))

  def copy_lod(self, src_lod, dest_lod, src_revnum):
    """Copy all of SRC_LOD at SRC_REVNUM to DST_LOD.

    In the youngest revision of the repository, the destination LOD
    *must not* already exist.

    Return the new node at DEST_LOD.  Note that this node is not
    necessarily writable, though its parent node necessarily is."""

    node = self._mirror.copy_lod(src_lod, dest_lod, src_revnum)
    self._invoke_delegates('copy_lod', src_lod, dest_lod, src_revnum)
    return node

  def copy_path(
        self, cvs_path, src_lod, dest_lod, src_revnum, create_parent=False
        ):
    """Copy CVS_PATH from SRC_LOD at SRC_REVNUM to DST_LOD.

    In the youngest revision of the repository, the destination's
    parent *must* exist unless CREATE_PARENT is specified.  But the
    destination itself *must not* exist.

    Return the new node at (CVS_PATH, DEST_LOD), as a
    CurrentMirrorDirectory."""

    if cvs_path.parent_directory is None:
      return self.copy_lod(src_lod, dest_lod, src_revnum)

    # Get the node of our source, or None if it is a file:
    src_node = self._mirror.get_old_path(cvs_path, src_lod, src_revnum)

    # Get the parent path of the destination:
    if create_parent:
      dest_parent_node = self._mkdir_p(cvs_path.parent_directory, dest_lod)
    else:
      try:
        dest_parent_node = self._mirror.get_current_path(
            cvs_path.parent_directory, dest_lod
            )
      except KeyError:
        raise self.ParentMissingError(
            'Attempt to add path \'%s\' to repository mirror, '
            'but its parent directory doesn\'t exist in the mirror.'
            % (dest_lod.get_path(cvs_path.cvs_path),)
            )

    if cvs_path in dest_parent_node:
      raise PathExistsError(
          'Attempt to add path \'%s\' to repository mirror '
          'when it already exists in the mirror.'
          % (dest_lod.get_path(cvs_path.cvs_path),)
          )

    dest_parent_node[cvs_path] = src_node
    self._invoke_delegates(
        'copy_path',
        src_lod.get_path(cvs_path.cvs_path),
        dest_lod.get_path(cvs_path.cvs_path),
        src_revnum
        )

    return dest_parent_node[cvs_path]

  def fill_symbol(self, svn_symbol_commit, fill_source):
    """Perform all copies for the CVSSymbols in SVN_SYMBOL_COMMIT.

    The symbolic name is guaranteed to exist in the Subversion
    repository by the end of this call, even if there are no paths
    under it."""

    symbol = svn_symbol_commit.symbol

    try:
      dest_node = self._mirror.get_current_lod_directory(symbol)
    except KeyError:
      self._fill_directory(symbol, None, fill_source, None)
    else:
      self._fill_directory(symbol, dest_node, fill_source, None)

  def _fill_directory(self, symbol, dest_node, fill_source, parent_source):
    """Fill the tag or branch SYMBOL at the path indicated by FILL_SOURCE.

    Use items from FILL_SOURCE, and recurse into the child items.

    Fill SYMBOL starting at the path FILL_SOURCE.cvs_path.  DEST_NODE
    is the node of this destination path, or None if the destination
    does not yet exist.  All directories above this path have already
    been filled.  FILL_SOURCE is a FillSource instance describing the
    items within a subtree of the repository that still need to be
    copied to the destination.

    PARENT_SOURCE is the SVNRevisionRange that was used to copy the
    parent directory, if it was copied in this commit.  We prefer to
    copy from the same source as was used for the parent, since it
    typically requires less touching-up.  If PARENT_SOURCE is None,
    then the parent directory was not copied in this commit, so no
    revision is preferable to any other."""

    copy_source = fill_source.compute_best_source(parent_source)

    # Figure out if we shall copy to this destination and delete any
    # destination path that is in the way.
    if dest_node is None:
      # The destination does not exist at all, so it definitely has to
      # be copied:
      dest_node = self.copy_path(
          fill_source.cvs_path, copy_source.source_lod,
          symbol, copy_source.opening_revnum
          )
    elif (parent_source is not None) and (
          copy_source.source_lod != parent_source.source_lod
          or copy_source.opening_revnum != parent_source.opening_revnum
          ):
      # The parent path was copied from a different source than we
      # need to use, so we have to delete the version that was copied
      # with the parent then re-copy from the correct source:
      self.delete_path(fill_source.cvs_path, symbol)
      dest_node = self.copy_path(
          fill_source.cvs_path, copy_source.source_lod,
          symbol, copy_source.opening_revnum
          )
    else:
      copy_source = parent_source

    # The map {CVSPath : FillSource} of entries within this directory
    # that need filling:
    src_entries = fill_source.get_subsource_map()

    if copy_source is not None:
      self._prune_extra_entries(
          fill_source.cvs_path, symbol, dest_node, src_entries
          )

    return self._cleanup_filled_directory(
        symbol, dest_node, src_entries, copy_source
        )

  def _cleanup_filled_directory(
        self, symbol, dest_node, src_entries, copy_source
        ):
    """The directory at DEST_NODE has been filled and pruned; recurse.

    Recurse into the SRC_ENTRIES, in alphabetical order.  If DEST_NODE
    was copied in this revision, COPY_SOURCE should indicate where it
    was copied from; otherwise, COPY_SOURCE should be None."""

    cvs_paths = src_entries.keys()
    cvs_paths.sort()
    for cvs_path in cvs_paths:
      if isinstance(cvs_path, CVSDirectory):
        # Path is a CVSDirectory:
        try:
          dest_subnode = dest_node[cvs_path]
        except KeyError:
          # Path doesn't exist yet; it has to be created:
          dest_node = self._fill_directory(
              symbol, None, src_entries[cvs_path], None
              ).parent_mirror_dir
        else:
          # Path already exists, but might have to be cleaned up:
          dest_node = self._fill_directory(
              symbol, dest_subnode, src_entries[cvs_path], copy_source
              ).parent_mirror_dir
      else:
        # Path is a CVSFile:
        self._fill_file(
            symbol, cvs_path in dest_node, src_entries[cvs_path], copy_source
            )
        # Reread dest_node since the call to _fill_file() might have
        # made it writable:
        dest_node = self._mirror.get_current_path(
            dest_node.cvs_path, dest_node.lod
            )

    return dest_node

  def _fill_file(self, symbol, dest_existed, fill_source, parent_source):
    """Fill the tag or branch SYMBOL at the path indicated by FILL_SOURCE.

    Use items from FILL_SOURCE.

    Fill SYMBOL at path FILL_SOURCE.cvs_path.  DEST_NODE is the node
    of this destination path, or None if the destination does not yet
    exist.  All directories above this path have already been filled
    as needed.  FILL_SOURCE is a FillSource instance describing the
    item that needs to be copied to the destination.

    PARENT_SOURCE is the source from which the parent directory was
    copied, or None if the parent directory was not copied during this
    commit.  We prefer to copy from PARENT_SOURCE, since it typically
    requires less touching-up.  If PARENT_SOURCE is None, then the
    parent directory was not copied in this commit, so no revision is
    preferable to any other."""

    copy_source = fill_source.compute_best_source(parent_source)

    # Figure out if we shall copy to this destination and delete any
    # destination path that is in the way.
    if not dest_existed:
      # The destination does not exist at all, so it definitely has to
      # be copied:
      self.copy_path(
          fill_source.cvs_path, copy_source.source_lod,
          symbol, copy_source.opening_revnum
          )
    elif (parent_source is not None) and (
          copy_source.source_lod != parent_source.source_lod
          or copy_source.opening_revnum != parent_source.opening_revnum
          ):
      # The parent path was copied from a different source than we
      # need to use, so we have to delete the version that was copied
      # with the parent and then re-copy from the correct source:
      self.delete_path(fill_source.cvs_path, symbol)
      self.copy_path(
          fill_source.cvs_path, copy_source.source_lod,
          symbol, copy_source.opening_revnum
          )

  def _prune_extra_entries(
        self, dest_cvs_path, symbol, dest_node, src_entries
        ):
    """Delete any entries in DEST_NODE that are not in SRC_ENTRIES."""

    delete_list = [
        cvs_path
        for cvs_path in dest_node
        if cvs_path not in src_entries
        ]

    # Sort the delete list so that the output is in a consistent
    # order:
    delete_list.sort()
    for cvs_path in delete_list:
      del dest_node[cvs_path]
      self._invoke_delegates('delete_path', symbol, cvs_path)

  def add_delegate(self, delegate):
    """Adds DELEGATE to self._delegates.

    For every delegate you add, as soon as SVNRepositoryMirror
    performs a repository action method, SVNRepositoryMirror will call
    the delegate's corresponding repository action method.  Multiple
    delegates will be called in the order that they are added.  See
    SVNRepositoryMirrorDelegate for more information."""

    self._delegates.append(delegate)

  def _invoke_delegates(self, method, *args):
    """Invoke a method on each delegate.

    Iterate through each of our delegates, in the order that they were
    added, and call the delegate's method named METHOD with the
    arguments in ARGS."""

    for delegate in self._delegates:
      getattr(delegate, method)(*args)

  def close(self):
    """Call the delegate finish methods and close databases."""

    self._invoke_delegates('finish')
    self._mirror.close()
    self._mirror = None


class SVNRepositoryMirrorDelegate:
  """Abstract superclass for any delegate to SVNRepositoryMirror.

  Subclasses must implement all of the methods below.

  For each method, a subclass implements, in its own way, the
  Subversion operation implied by the method's name.  For example, for
  the add_path method, the DumpfileDelegate would write out a
  'Node-add:' command to a Subversion dumpfile, the StdoutDelegate
  would merely print that the path is being added to the repository,
  and the RepositoryDelegate would actually cause the path to be added
  to the Subversion repository that it is creating."""

  def start_commit(self, revnum, revprops):
    """An SVN commit is starting.

    Perform any actions needed to start an SVN commit with revision
    number REVNUM and revision properties REVPROPS."""

    raise NotImplementedError()

  def end_commit(self):
    """An SVN commit is ending."""

    raise NotImplementedError()

  def initialize_project(self, project):
    """Initialize PROJECT.

    For Subversion, this means to create the trunk, branches, and tags
    directories for PROJECT."""

    raise NotImplementedError()

  def initialize_lod(self, lod):
    """Initialize LOD with no contents.

    LOD is an instance of LineOfDevelopment.  It is also possible for
    an LOD to be created by copying from another LOD; such events are
    indicated via the copy_lod() callback."""

    raise NotImplementedError()

  def mkdir(self, lod, cvs_directory):
    """Create CVS_DIRECTORY within LOD.

    LOD is a LineOfDevelopment; CVS_DIRECTORY is a CVSDirectory."""

    raise NotImplementedError()

  def add_path(self, s_item):
    """Add the path corresponding to S_ITEM to the repository.

    S_ITEM is an SVNCommitItem."""

    raise NotImplementedError()

  def change_path(self, s_item):
    """Change the path corresponding to S_ITEM in the repository.

    S_ITEM is an SVNCommitItem."""

    raise NotImplementedError()

  def delete_lod(self, lod):
    """Delete LOD from the repository.

    LOD is a LineOfDevelopment instance."""

    raise NotImplementedError()

  def delete_path(self, lod, cvs_path):
    """Delete CVS_PATH from LOD.

    LOD is a LineOfDevelopment; CVS_PATH is a CVSPath."""

    raise NotImplementedError()

  def copy_lod(self, src_lod, dest_lod, src_revnum):
    """Copy SRC_LOD in SRC_REVNUM to DEST_LOD.

    SRC_LOD and DEST_LOD are both LODs, and SRC_REVNUM is a subversion
    revision number (int)."""

    raise NotImplementedError()

  def copy_path(self, src_path, dest_path, src_revnum):
    """Copy SRC_PATH in SRC_REVNUM to DEST_PATH.

    SRC_PATH and DEST_PATH are both SVN paths, and SRC_REVNUM is a
    subversion revision number (int)."""

    raise NotImplementedError()

  def finish(self):
    """All SVN revisions have been committed.

    Perform any necessary cleanup."""

    raise NotImplementedError()


