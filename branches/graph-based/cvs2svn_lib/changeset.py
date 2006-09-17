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

"""Manage change sets."""


from cvs2svn_lib.boolean import *
from cvs2svn_lib.set_support import *


class Changeset(object):
  """A set of cvs_items that might potentially form a single change set."""

  def __init__(self, id, cvs_items):
    self.id = id
    self.cvs_items = set(cvs_items)

  def __str__(self):
    return 'Changeset<%d>' % (self.id,)

  def __repr__(self):
    lines = ['%s\n' % self]
    for cvs_item in self.cvs_items:
      lines.append('  %s\n' % cvs_item)
    return ''.join(lines)


