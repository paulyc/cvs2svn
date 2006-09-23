# (Be in -*- python -*- mode.)
#
# ====================================================================
# Copyright (c) 2006 CollabNet.  All rights reserved.
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

"""A node in the changeset dependency graph."""


from __future__ import generators

from cvs2svn_lib.boolean import *
from cvs2svn_lib.set_support import *


class ChangesetGraph(object):
  """A graph of changesets and their dependencies."""

  def __init__(self):
    # A map { id : ChangesetGraphNode }
    self.nodes = {}

  def add(self, node):
    self.nodes[node.id] = node

  def __getitem__(self, id):
    return self.nodes[id]

  def get(self, id):
    return self.nodes.get(id)

  def remove(self, node):
    del self.nodes[node.id]

  def __iter__(self):
    return self.nodes.itervalues()

  def get_nodes(self):
    return self.nodes.values()

  def extract(self, node):
    """Remove NODE and remove references to it from other nodes.

    This method does not change NODE.pred_ids or NODE.succ_ids."""

    for succ_id in node.succ_ids:
      succ = self[succ_id]
      succ.pred_ids.remove(node.id)

    for pred_id in node.pred_ids:
      pred = self[pred_id]
      pred.succ_ids.remove(node.id)

    del self.nodes[node.id]

  def remove_nopred_nodes(self):
    """Remove and yield any nodes that do not have predecessors.

    This can be continued until there are no more nopred nodes.  The
    graph should not be altered while this generator is running."""

    # Find a list of nodes with no predecessors:
    nopred_nodes = [
        node
        for node in self.nodes.itervalues()
        if not node.pred_ids]
    while nopred_nodes:
      node = nopred_nodes.pop()
      self.extract(node)
      # See if any successors are now ready for extraction:
      for succ_id in node.succ_ids:
        succ = self[succ_id]
        if not succ.pred_ids:
          nopred_nodes.append(succ)
      yield node

  def find_cycle(self):
    """Return a cycle in this graph as lists of nodes.

    The cycle is left in the graph.  This method destroys the graph:
    it extracts and discards nodes that have no predecessors.

    If there are no cycles left in the graph, return None.  By the
    time this can happen, all of the nodes in the graph will have been
    removed."""

    for node in self.remove_nopred_nodes():
      pass

    if not self.nodes:
      return None
    # Now all nodes in the graph are involved in a cycle.  Pick an
    # arbitrary node and follow it backwards until a node is seen a
    # second time, then we have our cycle.
    node = self.nodes.itervalues().next()
    seen_nodes = [node]
    while True:
      node_id = node.pred_ids.__iter__().next()
      node = self[node_id]
      try:
        i = seen_nodes.index(node)
      except ValueError:
        seen_nodes.append(node)
      else:
        seen_nodes = seen_nodes[i:]
        seen_nodes.reverse()
        return seen_nodes

  def __repr__(self):
    """For convenience only.  The format is subject to change at any time."""

    if self.nodes:
      return 'ChangesetGraph:\n%s' \
             % ''.join(['  %r\n' % node for node in self])
    else:
      return 'ChangesetGraph:\n  EMPTY\n'


class ChangesetGraphNode(object):
  """A node in the changeset dependency graph."""

  def __init__(self, id, pred_ids, succ_ids):
    self.id = id
    self.pred_ids = set(pred_ids)
    self.succ_ids = set(succ_ids)

  def __repr__(self):
    """For convenience only.  The format is subject to change at any time."""

    return '%x; pred=[%s]; succ=[%s]' % (
        self.id,
        ','.join(['%x' % id for id in self.pred_ids]),
        ','.join(['%x' % id for id in self.succ_ids]),
        )


