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
from cvs2svn_lib.time_range import TimeRange


class ChangesetGraphNode(object):
  """A node in the changeset dependency graph."""

  def __init__(self, id):
    self.id = id
    self.time_range = TimeRange()
    self.pred_ids = set()
    self.succ_ids = set()

  def __repr__(self):
    """For convenience only.  The format is subject to change at any time."""

    return '%x; pred=[%s]; succ=[%s]' % (
        self.id,
        ','.join(['%x' % id for id in self.pred_ids]),
        ','.join(['%x' % id for id in self.succ_ids]),
        )


