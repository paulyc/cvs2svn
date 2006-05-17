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

"""This module contains database facilities used by cvs2svn."""


from boolean import *
import cvs_revision
import database


class CVSRevisionDatabase:
  """A Database to store CVSRevision objects and retrieve them by their
  unique_key()."""

  def __init__(self, cvs_file_db, filename, mode):
    """Initialize an instance, opening database in MODE (like the MODE
    argument to Database or anydbm.open()).  Use CVS_FILE_DB to look
    up CVSFiles."""

    self.cvs_file_db = cvs_file_db
    self.db = database.PDatabase(filename, mode)

  def log_revision(self, c_rev):
    """Add C_REV, a CVSRevision, to the database."""

    args = list(c_rev.__getinitargs__())
    args[1] = args[1].id
    self.db[c_rev.unique_key()] = args

  def get_revision(self, unique_key):
    """Return the CVSRevision stored under UNIQUE_KEY."""

    args = self.db[unique_key]
    args[1] = self.cvs_file_db.get_file(args[1])
    return cvs_revision.CVSRevision(*args)


