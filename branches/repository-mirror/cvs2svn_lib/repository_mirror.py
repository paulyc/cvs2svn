# (Be in -*- python -*- mode.)
#
# ====================================================================
# Copyright (c) 2000-2008 CollabNet.  All rights reserved.
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

"""This module contains the RepositoryMirror class and supporting classes.

RepositoryMirror represents the skeleton of a versioned file tree with
multiple lines of development ('LODs').  It records the presence of
absence of files and directories, but not their contents.  Given three
values (revnum, lod, cvs_path), it can tell you whether the specified
CVSPath existed on the specified LOD in the given revision number.
The file trees corresponding to the most recent revision can be
modified.

The individual file trees are stored using immutable tree structures.
Each directory node is represented as a MirrorDirectory instance,
which is basically a map {cvs_path : node_id}, where cvs_path is a
CVSPath within the directory, and node_id is an integer ID that
uniquely identifies another directory node if that node is a
CVSDirectory, or None if that node is a CVSFile.  If a directory node
is to be modified, then first a new node is created with a copy of the
original node's contents, then the copy is modified.  A reference to
the copy also has to be stored in the parent node, meaning that the
parent node needs to be modified, and so on recursively to the root
node of the file tree.  This data structure allows cheap deep copies,
which is useful for tagging and branching.

The class must also be able to find the root directory node
corresponding to a particular (revnum, lod).  This is done by keeping
an LODHistory instance for each LOD, which can determine the root
directory node ID for that LOD for any revnum.  It does so by
recording changes to the root directory node ID only for revisions in
which it changed.  Thus it stores two arrays, revnums (a list of the
revision numbers when the ID changed), and ids (a list of the
corresponding IDs).  To find the ID for a particular revnum, first a
binary search is done in the revnums array to find the index of the
last change preceding revnum, then the corresponding ID is read from
the ids array.  Since most revisions change only one LOD, this allows
storage of the history of potentially tens of thousands of LODs over
hundreds of thousands of revisions in an amount of space that scales
as O(numberOfLODs + numberOfRevisions), rather than O(numberOfLODs *
numberOfRevisions) as would be needed if the information were stored
in the equivalent of a 2D array."""


import sys
import bisect

from cvs2svn_lib.boolean import *
from cvs2svn_lib import config
from cvs2svn_lib.common import InternalError
from cvs2svn_lib.common import DB_OPEN_NEW
from cvs2svn_lib.context import Ctx
from cvs2svn_lib.key_generator import KeyGenerator
from cvs2svn_lib.artifact_manager import artifact_manager
from cvs2svn_lib.serializer import MarshalSerializer
from cvs2svn_lib.database import IndexedDatabase
from cvs2svn_lib.cvs_file import CVSDirectory
from cvs2svn_lib.symbol import Trunk
from cvs2svn_lib.svn_commit_item import SVNCommitItem


class MirrorDirectory(object):
  """Represent a node within the RepositoryMirror.

  Instances of this class act like a map {CVSPath : MirrorDirectory},
  where CVSPath is an item within this directory (i.e., a file or
  subdirectory within this directory).  The value is either another
  MirrorDirectory instance (for directories) or None (for files).

  There is a bewildering variety of MirrorDirectory classes.  The most
  important distinction is between OldMirrorDirectories and
  CurrentMirrorDirectories:

      OldMirrorDirectory -- a MirrorDirectory that was looked up for
          an old revision.  These instances are immutable, as only the
          current revision is allowed to be modified.

      CurrentMirrorDirectory -- a MirrorDirectory that was looked up
          for the current revision.  These instances might represent a
          node that has already been copied during this revision, or
          they might represent a node that was carried over from an
          old revision.  If the latter, then the node copies itself
          (and bubbles up the change) before allowing itself to be
          modified.

  """

  def __init__(self, repo, id, entries):
    # The RepositoryMirror containing this directory:
    self.repo = repo

    # The id of this node:
    self.id = id

    # The entries within this directory, stored as a map {CVSPath :
    # node_id}.  The node_ids are integers for CVSDirectories, None
    # for CVSFiles:
    self.entries = entries

  def __getitem__(self, cvs_path):
    """Return the MirrorDirectory associated with the specified subnode.

    Return a MirrorDirectory instance if the subnode is a
    CVSDirectory; None if it is a CVSFile.  Raise KeyError if the
    specified subnode does not exist."""

    raise NotImplementedError()

  def __len__(self):
    """Return the number of CVSPaths within this node."""

    return len(self.entries)

  def __contains__(self, cvs_path):
    """Return True iff CVS_PATH is contained in this node."""

    return cvs_path in self.entries

  def __iter__(self):
    """Iterate over the CVSPaths within this node."""

    return self.entries.__iter__()


class OldMirrorDirectory(MirrorDirectory):
  """Represent a historical directory within the RepositoryMirror."""

  def __getitem__(self, cvs_path):
    id = self.entries[cvs_path]
    if id is None:
      # This represents a leaf node.
      return None
    else:
      return OldMirrorDirectory(self, id, self.repo._nodes_db[id])


class CurrentMirrorDirectory(MirrorDirectory):
  """Represent a directory that currently exists in the RepositoryMirror."""

  def __init__(self, repo, id, lod, cvs_path, entries):
    MirrorDirectory.__init__(self, repo, id, entries)
    self.lod = lod
    self.cvs_path = cvs_path

  def __getitem__(self, cvs_path):
    id = self.entries[cvs_path]
    if id is None:
      # This represents a leaf node.
      return None
    else:
      try:
        return self.repo._new_nodes[id]
      except KeyError:
        return _CurrentMirrorReadOnlySubdirectory(
            self.repo, id, self.lod, self.cvs_path, self,
            self.repo._nodes_db[id]
            )


class _WritableMirrorDirectoryMixin:
  """Mixin for MirrorDirectories that are already writable.

  A MirrorDirectory is writable if it has already been recreated
  during the current revision."""

  def __setitem__(self, cvs_path, node):
    """Create or overwrite a subnode of this node.

    CVS_PATH is the path of the subnode.  NODE will be the new value
    of the node; for CVSDirectories it should be a MirrorDirectory
    instance; for CVSFiles it should be None."""

    if node is None:
      self.entries[cvs_path] = None
    else:
      self.entries[cvs_path] = node.id

  def __delitem__(self, cvs_path):
    """Remove the subnode of this node at CVS_PATH.

    If the node does not exist, then raise a KeyError."""

    del self.entries[cvs_path]

  def mkdir(self, cvs_directory):
    """Create an empty subdirectory of this node at CVS_PATH.

    Return the CurrentDirectory that was created."""

    if cvs_directory in self:
      raise self.PathExistsError(
          'Attempt to create directory \'%s\' in %s in repository mirror '
          'when it already exists.'
          % (cvs_directory, self.lod,)
          )

    new_node = _CurrentMirrorWritableSubdirectory(
        self.repo, self.repo.key_generator.gen_id(), self.lod, cvs_directory,
        self, {}
        )
    self[cvs_directory] = new_node
    self.repo._new_nodes[new_node.id] = new_node
    return new_node

  def add_file(self, cvs_file):
    """Create a file within this node at CVS_FILE."""

    if cvs_file in self:
      raise self.PathExistsError(
          'Attempt to create file \'%s\' in %s in repository mirror '
          'when it already exists.'
          % (cvs_file, self.lod,)
          )

    self[cvs_file] = None


class _ReadOnlyMirrorDirectoryMixin:
  """Mixin for a CurrentMirrorDirectory that hasn't yet been made writable."""

  def _make_writable(self):
    raise NotImplementedError()

  def __setitem__(self, cvs_path, node):
    self._make_writable()
    self[cvs_path] = node

  def __delitem__(self, cvs_path):
    self._make_writable()
    del self[cvs_path]

  def mkdir(self, cvs_directory):
    self._make_writable()
    self.mkdir(cvs_directory)

  def add_file(self, cvs_file):
    self._make_writable()
    self.add_file(cvs_file)


class CurrentMirrorLODDirectory(CurrentMirrorDirectory):
  """Represent an LOD's main directory in the mirror's current version."""

  def __init__(self, repo, id, lod, entries):
    CurrentMirrorDirectory.__init__(
        self, repo, id, lod, lod.project.get_root_cvs_directory(), entries
        )

  def rmdir(self):
    """Remove the directory represented by this object."""

    self.repo._get_lod_history(self.lod).update(self.repo._youngest, None)
    # Vandalize this object to prevent its being used again:
    self.__dict__.clear()


class _CurrentMirrorReadOnlyLODDirectory(
          CurrentMirrorLODDirectory, _ReadOnlyMirrorDirectoryMixin
          ):
  """Represent an LOD's main directory in the mirror's current version."""

  def _make_writable(self):
    self.__class__ = _CurrentMirrorWritableLODDirectory
    # Create a new ID:
    self.id = self.repo._key_generator.gen_id()
    self.repo._new_nodes[self.id] = self
    self.repo._get_lod_history(self.lod).update(self.repo._youngest, self.id)


class _CurrentMirrorWritableLODDirectory(
          CurrentMirrorLODDirectory, _WritableMirrorDirectoryMixin
          ):
  pass


class CurrentMirrorSubdirectory(CurrentMirrorDirectory):
  """Represent a subdirectory in the mirror's current version."""

  def __init__(self, repo, id, lod, cvs_path, parent_mirror_dir, entries):
    CurrentMirrorDirectory.__init__(self, repo, id, lod, cvs_path, entries)
    self.parent_mirror_dir = parent_mirror_dir

  def rmdir(self):
    """Remove the directory represented by this object."""

    del self.parent_mirror_dir[self.cvs_path]
    # Vandalize this object to prevent its being used again:
    self.__dict__.clear()


class _CurrentMirrorReadOnlySubdirectory(
          CurrentMirrorSubdirectory, _ReadOnlyMirrorDirectoryMixin
          ):
  """Represent a subdirectory in the mirror's current version."""

  def _make_writable(self):
    self.__class__ = _CurrentMirrorWritableSubdirectory
    # Create a new ID:
    self.id = self.repo._key_generator.gen_id()
    self.repo._new_nodes[self.id] = self
    self.parent_mirror_dir[self.cvs_path] = self.id


class _CurrentMirrorWritableSubdirectory(
          CurrentMirrorSubdirectory, _WritableMirrorDirectoryMixin
          ):
  pass


class LODHistory(object):
  """The history of root nodes for a line of development.

  Members:

    revnums -- (list of int) the SVN revision numbers in which the id
        changed, in numerical order.

    ids -- (list of (int or None)) the ID of the node describing the
        root of this LOD starting at the corresponding SVN revision
        number, or None if the LOD did not exist in that revision.

  To find the root id for a given SVN revision number, a binary search
  is done within REVNUMS to find the index of the most recent revision
  at the time of REVNUM, then that index is used to read the id out of
  IDS.

  A sentry is written at the zeroth index of both arrays to describe
  the initial situation, namely, that the LOD doesn't exist in SVN
  revision r0.

  """

  __slots__ = ['revnums', 'ids']

  def __init__(self):
    self.revnums = [0]
    self.ids = [None]

  def get_id(self, revnum=sys.maxint):
    """Get the ID of the root path for this LOD in REVNUM.

    Raise KeyError if this LOD didn't exist in REVNUM."""

    index = bisect.bisect_right(self.revnums, revnum) - 1
    id = self.ids[index]

    if id is None:
      raise KeyError()

    return id

  def get_current_id(self):
    """Get the ID of the root path for this LOD in the current revision.

    Raise KeyError if this LOD doesn't currently exist."""

    id = self.ids[-1]

    if id is None:
      raise KeyError()

    return id

  def exists(self):
    """Return True iff LOD exists at the end of history."""

    return self.ids[-1] is not None

  def update(self, revnum, id):
    """Indicate that the root node of this LOD changed to ID at REVNUM.

    REVNUM is a revision number that must be the same as that of the
    previous recorded change (in which case the previous change is
    overwritten) or later (in which the new change is appended).

    ID can be a node ID, or it can be None to indicate that this LOD
    ceased to exist in REVNUM."""

    if revnum < self.revnums[-1]:
      raise KeyError()
    elif revnum == self.revnums[-1]:
      # Overwrite old entry (which was presumably read-only):
      self.ids[-1] = id
    else:
      self.revnums.append(revnum)
      self.ids.append(id)


class _NodeSerializer(MarshalSerializer):
  def __init__(self):
    self.cvs_file_db = Ctx()._cvs_file_db

  def _dump(self, node):
    return [
        (cvs_path.id, value)
        for (cvs_path, value) in node.iteritems()
        ]

  def dumpf(self, f, node):
    MarshalSerializer.dumpf(self, f, self._dump(node))

  def dumps(self, node):
    return MarshalSerializer.dumps(self, self._dump(node))

  def _load(self, items):
    retval = {}
    for (id, value) in items:
      retval[self.cvs_file_db.get_file(id)] = value
    return retval

  def loadf(self, f):
    return self._load(MarshalSerializer.loadf(self, f))

  def loads(self, s):
    return self._load(MarshalSerializer.loads(self, s))


class RepositoryMirror:
  """Mirror a Subversion repository and its history.

  Mirror a Subversion repository as it is constructed, one SVNCommit
  at a time.  For each LineOfDevelopment we store a skeleton of the
  directory structure within that LOD for each SVN revision number in
  which it changed.

  For each LOD that has been seen so far, an LODHistory instance is
  stored in self._lod_histories.  An LODHistory keeps track of each
  SVNRevision in which files were added to or deleted from that LOD,
  as well as the node id of the node tree describing the LOD contents
  at that SVN revision.

  The LOD trees themselves are stored in the _nodes_db database, which
  maps node ids to nodes.  A node is a map from CVSPath.id to ids of
  the corresponding subnodes.  The _nodes_db is stored on disk and
  each access is expensive.

  The _nodes_db database only holds the nodes for old revisions.  The
  revision that is being constructed is kept in memory in the
  _new_nodes map, which is cheap to access.

  You must invoke start_commit() before each SVNCommit and
  end_commit() afterwards.

  *** WARNING *** Path arguments to methods in this class MUST NOT
      have leading or trailing slashes."""

  class ParentMissingError(Exception):
    """The parent of a path is missing.

    Exception raised if an attempt is made to add a path to the
    repository mirror but the parent's path doesn't exist in the
    youngest revision of the repository."""

    pass

  class PathExistsError(Exception):
    """The path already exists in the repository.

    Exception raised if an attempt is made to add a path to the
    repository mirror and that path already exists in the youngest
    revision of the repository."""

    pass

  def register_artifacts(self, which_pass):
    """Register the artifacts that will be needed for this object."""

    artifact_manager.register_temp_file(
        config.SVN_MIRROR_NODES_INDEX_TABLE, which_pass
        )
    artifact_manager.register_temp_file(
        config.SVN_MIRROR_NODES_STORE, which_pass
        )

  def open(self):
    """Set up the RepositoryMirror and prepare it for SVNCommits."""

    self._key_generator = KeyGenerator()

    # A map from LOD to LODHistory instance for all LODs that have
    # been defines so far:
    self._lod_histories = {}

    # This corresponds to the 'nodes' table in a Subversion fs.  (We
    # don't need a 'representations' or 'strings' table because we
    # only track metadata, not file contents.)
    self._nodes_db = IndexedDatabase(
        artifact_manager.get_temp_file(config.SVN_MIRROR_NODES_STORE),
        artifact_manager.get_temp_file(config.SVN_MIRROR_NODES_INDEX_TABLE),
        DB_OPEN_NEW, serializer=_NodeSerializer()
        )

    # Start at revision 0 without a root node.  It will be created
    # by _open_writable_lod_node().
    self._youngest = 0

  def start_commit(self, revnum):
    """Start a new commit."""

    self._youngest = revnum

    # A map {node_id : CurrentMirrorDirectory}.
    self._new_nodes = {}

  def end_commit(self):
    """Called at the end of each commit.

    This method copies the newly created nodes to the on-disk nodes
    db."""

    # Copy the new nodes to the _nodes_db
    for node in self._new_nodes.values():
      if isinstance(node, _WritableMirrorDirectoryMixin):
        self._nodes_db[node.id] = node.entries

    del self._new_nodes

  def _get_lod_history(self, lod):
    """Return the LODHistory instance describing LOD.

    Create a new (empty) LODHistory if it doesn't yet exist."""

    try:
      return self._lod_histories[lod]
    except KeyError:
      lod_history = LODHistory()
      self._lod_histories[lod] = lod_history
      return lod_history

  def get_old_lod_directory(self, lod, revnum):
    """Return the directory for the root path of LOD at revision REVNUM.

    Return an instance of MirrorDirectory if the path exists;
    otherwise, raise KeyError."""

    lod_history = self._get_lod_history(lod)
    id = lod_history.get_id(revnum)
    return OldMirrorDirectory(self, id, self._nodes_db[id])

  def get_old_directory(self, cvs_path, lod, revnum):
    """Return the directory for CVS_PATH from LOD at REVNUM.

    If cvs_path refers to a leaf node, return None.

    Raise KeyError if the node does not exist."""

    if cvs_path.parent_directory is None:
      return self.get_old_lod_directory(lod, revnum)
    else:
      return self.get_old_directory(
          cvs_path.parent_directory, lod, revnum
          )[cvs_path]

  def get_current_lod_directory(self, lod):
    """Return the directory for the root path of LOD in the current revision.

    Return an instance of CurrentMirrorDirectory.  Raise KeyError if
    the path doesn't already exist."""

    lod_history = self._get_lod_history(lod)
    id = lod_history.get_current_id()
    try:
      return self._new_nodes[id]
    except KeyError:
      return _CurrentMirrorReadOnlyLODDirectory(
          self, id, lod, self._nodes_db[id]
          )

  def get_current_directory(self, cvs_directory, lod):
    """Return the directory for CVS_DIRECTORY in LOD in the current revision.

    Return an instance of CurrentMirrorDirectory.  Raise KeyError if
    CVS_DIRECTORY doesn't exist."""

    if cvs_directory.parent_directory is None:
      return self.get_current_lod_directory(lod)

    return self.get_current_directory(
        cvs_directory.parent_directory, lod
        )[cvs_directory]

  def delete_lod(self, lod):
    """Delete the main path for LOD from the tree.

    The path must currently exist."""

    lod_history = self._get_lod_history(lod)
    if not lod_history.exists():
      raise KeyError()
    lod_history.update(self._youngest, None)

  def copy_lod(self, src_lod, dest_lod, src_revnum):
    """Copy all of SRC_LOD at SRC_REVNUM to DST_LOD.

    In the youngest revision of the repository, the destination LOD
    *must not* already exist.

    Return the new node at DEST_LOD.  Note that this node is not
    necessarily writable, though its parent node necessarily is."""

    # Get the node of our src_path
    src_node = self.get_old_lod_directory(src_lod, src_revnum)

    dest_lod_history = self._get_lod_history(dest_lod)
    if dest_lod_history.exists():
      raise self.PathExistsError(
          "Attempt to add path '%s' to repository mirror "
          "when it already exists in the mirror." % (dest_lod.get_path(),)
          )

    dest_lod_history.update(self._youngest, src_node.id)

    # This is a cheap copy, so src_node has the same contents as the
    # new destination node.
    return src_node

  def copy_path(self, cvs_path, src_lod, dest_lod, src_revnum):
    """Copy CVS_PATH from SRC_LOD at SRC_REVNUM to DST_LOD.

    In the youngest revision of the repository, the destination's
    parent *must* exist.  But the destination itself *must not* exist.

    Return the new node at (CVS_PATH, DEST_LOD)."""

    if cvs_path.parent_directory is None:
      return self.copy_lod(src_lod, dest_lod, src_revnum)

    # Get the node of our source, or None if it is a file:
    src_node = self.get_old_directory(cvs_path, src_lod, src_revnum)

    # Get the parent path of the destination:
    try:
      dest_parent_node = self.get_current_directory(
          cvs_path.parent_directory, dest_lod
          )
    except KeyError:
      raise self.ParentMissingError(
          'Attempt to add path \'%s\' to repository mirror, '
          'but its parent directory doesn\'t exist in the mirror.'
          % (dest_lod.get_path(cvs_path.cvs_path),)
          )

    if cvs_path in dest_parent_node:
      raise self.PathExistsError(
          'Attempt to add path \'%s\' to repository mirror '
          'when it already exists in the mirror.'
          % (dest_lod.get_path(cvs_path.cvs_path),)
          )

    dest_parent_node[cvs_path] = src_node

    # This is a cheap copy, so src_node has the same contents as the
    # new destination node.
    return src_node

  def close(self):
    """Free resources and close databases."""

    self._lod_histories = None
    self._nodes_db.close()
    self._nodes_db = None


