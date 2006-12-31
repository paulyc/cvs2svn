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

"""This module contains the FillSource class."""


from cvs2svn_lib.boolean import *


class FillSource:
  """Representation of a fill source.

  A fill source is a directory (either trunk or a branches
  subdirectory) that can be used as a source for a symbol.  A fill
  source is initialized without a score, but can be scored later and
  the score stored into it via the set_score() method.  Scored fill
  sources can also be compared; the comparison is such that it sorts
  FillSources in descending order by score (higher score implies
  smaller).

  These objects are used by the symbol filler in SVNRepositoryMirror."""

  def __init__(self, project, prefix, node):
    """Create an unscored fill source with a prefix and a key."""

    # The Project to which this source belongs:
    self.project = project

    # The svn path that is the base of this source:
    self.prefix = prefix

    # The node in the SymbolFillingGuide corresponding to the prefix
    # path:
    self.node = node

    # The score of this source, or None if it hasn't been computed yet:
    self.score = None

    # The revision number with the best score for this source, or None
    # if it hasn't been computed yet:
    self.revnum = None

  def set_score(self, score, revnum):
    """Set the SCORE and REVNUM."""

    self.score = score
    self.revnum = revnum

  def __cmp__(self, other):
    """Comparison operator that sorts FillSources in descending score order.

    If the scores are the same, prefer trunk, or alphabetical order by
    path - these cases are mostly useful to stabilize testsuite
    results."""

    if self.score is None or other.score is None:
      raise TypeError('Tried to compare unscored FillSource')

    return cmp(other.score, self.score) \
           or cmp(other.prefix == self.project.trunk_path,
                  self.prefix == self.project.trunk_path) \
           or cmp(self.prefix, other.prefix)


