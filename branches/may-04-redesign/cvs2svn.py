#!/usr/bin/env python
#
# cvs2svn: ...
#
# $LastChangedRevision$
#
# ====================================================================
# Copyright (c) 2000-2004 CollabNet.  All rights reserved.
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

import rcsparse
import os
import sys
import sha
import re
import time
import fileinput
import string
import getopt
import stat
import string
import md5
import marshal

# Warnings and errors start with these strings.  They are typically
# followed by a colon and a space, as in "%s: " ==> "Warning: ".
warning_prefix = "Warning"
error_prefix = "Error"

# Make sure this Python is recent enough.
if sys.hexversion < 0x2000000:
  sys.stderr.write("'%s: Python 2.0 or higher required, "
                   "see www.python.org.\n" % error_prefix)
  sys.exit(1)

# DBM module selection

# 1. If we have bsddb3, it is probably newer than bsddb. Fake bsddb = bsddb3,
#    so that the dbhash module used by anydbm will use bsddb3.
try:
  import bsddb3
  sys.modules['bsddb'] = sys.modules['bsddb3']
except ImportError:
  pass

# 2. These DBM modules are not good for cvs2svn.
import anydbm
if (anydbm._defaultmod.__name__ == 'dumbdbm'
    or anydbm._defaultmod.__name__ == 'dbm'):
  print 'ERROR: your installation of Python does not contain a suitable'
  print '  DBM module. This script cannot continue.'
  print '  to solve: see http://python.org/doc/current/lib/module-anydbm.html'
  print '  for details.'
  sys.exit(1)

# 3. If we are using the old bsddb185 module, then try prefer gdbm instead.
#    Unfortunately, gdbm appears not to be trouble free, either.
if hasattr(anydbm._defaultmod, 'bsddb') \
    and not hasattr(anydbm._defaultmod.bsddb, '__version__'):
  try:
    gdbm = __import__('gdbm')
  except ImportError:
    sys.stderr.write(warning_prefix +
        ': The version of the bsddb module found '
        'on your computer has been reported to malfunction on some datasets, '
        'causing KeyError exceptions. You may wish to upgrade your Python to '
        'version 2.3 or later.\n')
  else:
    anydbm._defaultmod = gdbm

trunk_rev = re.compile('^[0-9]+\\.[0-9]+$')
branch_tag = re.compile('^[0-9.]+\\.0\\.[0-9]+$')
vendor_tag = re.compile('^[0-9]+\\.[0-9]+\\.[0-9]+$')

# This really only matches standard '1.1.1.*'-style vendor revisions.
# One could conceivably have a file whose default branch is 1.1.3 or
# whatever, or was that at some point in time, with vendor revisions
# 1.1.3.1, 1.1.3.2, etc.  But with the default branch gone now (which 
# is the only time this regexp gets used), we'd have no basis for
# assuming that the non-standard vendor branch had ever been the
# default branch anyway, so we don't want this to match them anyway.
vendor_revision = re.compile('^(1\\.1\\.1)\\.([0-9])+$')

DATAFILE = 'cvs2svn-data'
DUMPFILE = 'cvs2svn-dump'  # The "dumpfile" we create to load into the repos

SYMBOL_OPENINGS_CLOSINGS = 'cvs2svn-symbolic-names.txt'
SYMBOL_OPENINGS_CLOSINGS_SORTED = 'cvs2svn-symbolic-names-s.txt'

# This file is a temporary file for storing symbolic_name -> closing
# CVSRevision until the end of our pass where we can look up the
# corresponding SVNRevNum for the closing revs and write these out to
# the SYMBOL_OPENINGS_CLOSINGS.
SYMBOL_CLOSINGS_TMP = 'cvs2svn-symbolic-names-closings-tmp.txt'

# Skeleton version of an svn filesystem.
# See class RepositoryMirror for how these work.
SVN_REVISIONS_DB = 'cvs2svn-revisions.db'
NODES_DB = 'cvs2svn-nodes.db'

# Skeleton version of an svn filesystem.
# (These supersede and will eventually replace the two above.)
# See class SVNRepositoryMirror for how these work.
SVN_MIRROR_REVISIONS_DB = 'cvs2svn-svn-revisions.db'
SVN_MIRROR_NODES_DB = 'cvs2svn-svn-nodes.db'

# Offsets pointing to the beginning of each SYMBOLIC_NAME in
# SYMBOL_OPENINGS_CLOSINGS_SORTED
SYMBOL_OFFSETS_DB = 'cvs2svn-symbolic-name-offsets.db'


# Maps CVSRevision.unique_key()s to lists of symbolic names, where
# the CVSRevision is the last such that is a source for those symbolic
# names.  For example, if branch B's number is 1.3.0.2 in this CVS
# file, and this file's 1.3 is the latest (by date) revision among
# *all* CVS files that is a source for branch B, then the
# CVSRevision.unique_key() corresponding to this file at 1.3 would
# list at least B in its list. ###TODO rewrite example to use s-revs?
SYMBOL_LAST_CVS_REVS_DB = 'cvs2svn-symbol-last-cvs-revs.db'

# Maps CVSRevision.unique_key() to corresponding line in s-revs.
###TODO Or, we could map to an offset into s-revs, instead of dup'ing
### the s-revs data in this database.
CVS_REVS_DB = 'cvs2svn-cvs-revs.db'

# Lists all symbolic names that are tags.  Keys are strings (symbolic
# names), values are ignorable.
TAGS_DB = 'cvs2svn-tags.db'

# A path-based representation of the current head of the Subversion
# repository.  Keys are Subversion paths, values are ignorable.
# Intermediate directories are not stored, only leaf paths.
SVN_REPOSITORY_HEAD_DB = 'cvs2svn-repository-head.db'

# These two databases provide a bidirectional mapping between
# CVSRevision.unique_key()s and Subversion revision numbers.
#
# The first maps CVSRevision.unique_key() to a number; the values are
# not unique.
#
# The second maps a number to a list of CVSRevision.unique_key()s.
CVS_REVS_TO_SVN_REVNUMS = 'cvs2svn-cvs-revs-to-svn-revnums.db'
SVN_REVNUMS_TO_CVS_REVS = 'cvs2svn-svn-revnums-to-cvs-revs.db'

# This database maps svn_revnums to the corresponding symbolic name
# that is filled in the SVNCommit that corresponds to that svn_revnum.
SVN_COMMIT_NAMES = 'cvs2svn-svn-commit-names.db'

# This database maps svn_revnums of a default branch synchronization
# commit to the svn_revnum of the primary SVNCommit that motivated it.
#
# This mapping is used when generating the log message for the commit
# that synchronizes the default branch with trunk.
MOTIVATING_REVNUMS = 'cvs2svn-svn-motivating-commit-revnums.db'

# os.popen() on Windows seems to require an access-mode string of 'rb'
# in cases where the process will output binary information to stdout.
# Without the 'b' we get IOErrors upon closing the pipe.  Unfortunately
# 'rb' isn't accepted in the Linux version of os.popen().  As a purely
# practical matter, we compensate by switching on os.name.
if os.name == 'nt':
  PIPE_READ_MODE = 'rb'
  PIPE_WRITE_MODE = 'wb'
else:
  PIPE_READ_MODE = 'r'
  PIPE_WRITE_MODE = 'w'

# Record the default RCS branches, if any, for CVS filepaths.
#
# The keys are CVS filepaths, relative to the top of the repository
# and with the ",v" stripped off, so they match the cvs paths used in
# Commit.commit().  The values are vendor branch revisions, such as
# '1.1.1.1', or '1.1.1.2', or '1.1.1.96'.  The vendor branch revision
# represents the highest vendor branch revision thought to have ever
# been head of the default branch.
#
# The reason we record a specific vendor revision, rather than a
# default branch number, is that there are two cases to handle:
#
# One case is simple.  The RCS file lists a default branch explicitly
# in its header, such as '1.1.1'.  In this case, we know that every
# revision on the vendor branch is to be treated as head of trunk at
# that point in time.
#
# But there's also a degenerate case.  The RCS file does not currently
# have a default branch, yet we can deduce that for some period in the
# past it probably *did* have one.  For example, the file has vendor
# revisions 1.1.1.1 -> 1.1.1.96, all of which are dated before 1.2,
# and then it has 1.1.1.97 -> 1.1.1.100 dated after 1.2.  In this
# case, we should record 1.1.1.96 as the last vendor revision to have
# been the head of the default branch.
DEFAULT_BRANCHES_DB = 'cvs2svn-default-branches.db'

# Records the origin ranges for branches and tags.
# See class RepositoryMirror for how this works.
SYMBOLIC_NAME_ROOTS_DB = 'cvs2svn-symroots.db'

# See class SymbolicNameTracker for details.
SYMBOLIC_NAMES_DB = "cvs2svn-sym-names.db"

# Records the author and log message for each changeset.
# The keys are author+log digests, the same kind used to identify
# unique revisions in the .revs, etc files.  Each value is a tuple
# of two elements: '(author logmessage)'.
METADATA_DB = "cvs2svn-metadata.db"

REVS_SUFFIX = '.revs'
CLEAN_REVS_SUFFIX = '.c-revs'
SORTED_REVS_SUFFIX = '.s-revs'
RESYNC_SUFFIX = '.resync'

ATTIC = os.sep + 'Attic'

SVN_INVALID_REVNUM = -1

COMMIT_THRESHOLD = 5 * 60	# flush a commit if a 5 minute gap occurs

# Things that can happen to a file.
OP_NOOP   = '-'
OP_ADD    = 'A'
OP_DELETE = 'D'
OP_CHANGE = 'C'

# A deltatext either does or doesn't represent some change.
DELTATEXT_NONEMPTY = 'N'
DELTATEXT_EMPTY    = 'E'

DIGEST_END_IDX = 9 + (sha.digestsize * 2)

# Constants used in SYMBOL_OPENINGS_CLOSINGS
OPENING = '0'
CLOSING = 'C'

# Officially, CVS symbolic names must use a fairly restricted set of
# characters.  Unofficially, CVS 1.10 allows any character but [$,.:;@]
# We don't care if some repositories out there use characters outside the
# official set, as long as their tags start with a letter.
# Since the unofficial set also includes [/\] we need to translate those
# into ones that don't conflict with Subversion limitations.
symbolic_name_re = re.compile('^[a-zA-Z].*$')

def _clean_symbolic_name(name):
  """Return symbolic name NAME, translating characters that Subversion
  does not allow in a pathname."""
  name = name.replace('/',',')
  name = name.replace('\\',';')
  return name

class Singleton(object):
  """If you wish to have a class that you can only instantiate once,
  then this is your superclass."""
  def __new__(cls, *args, **kwds):
    singleton = cls.__dict__.get("__singleton__")
    if singleton is not None:
      return singleton
    cls.__singleton__ = singleton = object.__new__(cls)
    singleton.init(*args, **kwds)
    return singleton


###TODO Note that when we cleanup a db file owned by a singleton, we
#should Probably zap the singleton itself (or at least the ref to the
#db file).
class Cleanup(Singleton):
  """This singleton class manages any files created by cvs2svn. When
  you first create a file, call Cleanup.register, passing the
  filename, and the last pass that you need the file.  After the end
  of that pass, your file will be cleaned up after running an optional
  callback."""

  # We're a singleton, so we use init, not __init__
  def init(self):
    self._log = {}
    self._callbacks = {}

  def register(self, file, which_pass, callback=None):
    """Register FILE for cleanup at the end of WHICH_PASS, running
    function CALLBACK prior to removal.  Registering a given FILE is
    idempotent; you may register as many times as you wish, but it
    will only be cleaned up once."""
    ###TODO We support idempotency for files, but not callbacks.  Fix.
    if not self._log.has_key(which_pass):
      self._log[which_pass] = {}
    self._log[which_pass][file] = 1
    if callback:
      self._callbacks[file] = callback

  def cleanup(self, which_pass):
    """Clean up all files, and invoke callbacks, for pass WHICH_PASS."""
    if not self._log.has_key(which_pass):
      return
    for file in self._log[which_pass].keys():
      print "Cleaning up", file
      if self._callbacks.has_key(file):
        self._callbacks[file]()
      os.unlink(file)


# A wrapper for anydbm that uses the marshal module to store items as
# strings.
class Database:
  def __init__(self, filename, mode):
    self.db = anydbm.open(filename, mode)

  def has_key(self, key):
    return self.db.has_key(key)

  def __getitem__(self, key):
    return marshal.loads(self.db[key])

  def __setitem__(self, key, value):
    self.db[key] = marshal.dumps(value)

  def __delitem__(self, key):
    del self.db[key]

  def get(self, key, default):
    if self.has_key(key):
      return self.__getitem__(key)
    return default
   

class StringWriter:
  """Provide a convenient method to accumulate string data from
  methods that want to write to a stream."""
  def __init__(self):
    self.vals = []

  def write(self, val):
    self.vals.append(val)

  def stringValue(self):
    return "".join(self.vals)


class CVSRevision:
  def __init__(self, ctx, *args):
    """Initialize a new CVSRevision with context CTX and ARGS.
    If there is one argument in ARGS, it is a string, in the format of
    a line from a revs file.  If there are multiple ARGS, there must be
    11 of them, comprising a parsed revs line:

       timestamp       -->  (int) date stamp for this cvs revision
       digest          -->  (string) digest of author+logmsg
       op              -->  (char) 'C' for change, 'D' for delete
       prev_rev        -->  (string) previous CVS Rev, e.g., "1.2"
       rev             -->  (string) this CVS rev, e.g., "1.3"
       next_rev        -->  (string) next CVS Rev, e.g., "1.4"
       deltatext_code  -->  (char) 'N' if non-empty deltatext, else 'E'
       fname           -->  (string) relative path of file in CVS repos
       mode            -->  (string) "kkv", "kb", etc, or None.
       branch_name     -->  (string) branch on which this rev occurred
       tags            -->  (list of strings) all tags on this revision
       branches        -->  (list of strings) all branches rooted in this rev

    The two forms of initialization are equivalent.  You can construct
    essentially identical instances, one from a single string argument
    and the other from multiple arguments, as long as the string
    represents the same data as the multiple arguments."""
    self._ctx = ctx
    if len(args) == 12:
      (self.timestamp, self.digest, self.op, self.prev_rev, self.rev, 
       self.next_rev, self.deltatext_code, self.fname, 
       self.mode, self.branch_name, self.tags, self.branches) = args
    elif len(args) == 1:
      data = args[0].split(' ', 10)
      self.timestamp = int(data[0], 16)
      self.digest = data[1]
      self.op = data[2]
      self.prev_rev = data[3]
      if self.prev_rev == "*":
        self.prev_rev = None
      self.rev = data[4]
      self.next_rev = data[5]
      if self.next_rev == "*":
        self.next_rev = None
      self.deltatext_code = data[6]
      self.mode = data[7]
      if self.mode == "*":
        self.mode = None
      self.branch_name = data[8]
      if self.branch_name == "*":
        self.branch_name = None
      ntags = int(data[9])
      tags = data[10].split(' ', ntags + 1)
      nbranches = int(tags[ntags])
      branches = tags[ntags + 1].split(' ', nbranches)
      self.fname = branches[nbranches][:-1]  # strip \n
      self.tags = tags[:ntags]
      self.branches = branches[:nbranches]
    else:
      raise TypeError, 'CVSRevision() takes 2 or 12 arguments (%d given)' % \
          (len(args) + 1)
    self.cvs_path = relative_name(self._ctx.cvsroot, self.fname[:-2])
    self.svn_path = make_path(self._ctx, self.cvs_path, self.branch_name)
    self.svn_trunk_path = make_path(self._ctx, self.cvs_path)

  # The 'primary key' of a CVS Revision is the revision number + the
  # filename.  To provide a unique key (say, for a dict), we just glom
  # them together in a string.  By passing in self.prev_rev or
  # self.next_rev, you can get the unique key for their respective
  # CVSRevisions.
  def unique_key(self, revnum=None):
    if revnum is None:
      revnum = self.rev
    return revnum + "/" + self.fname

  def write_revs_line(self, output):
    if not self.prev_rev:
      self.prev_rev = '*'
    if not self.next_rev:
      self.next_rev = '*'
    output.write('%08lx %s %s %s %s %s %s ' % \
                 (self.timestamp, self.digest, self.op,
                  self.prev_rev, self.rev, self.next_rev,
                  self.deltatext_code))
    output.write('%s ' % (self.mode or "*"))
    output.write('%s ' % (self.branch_name or "*"))
    output.write('%d ' % (len(self.tags)))
    for tag in self.tags:
      output.write('%s ' % (tag))
    output.write('%d ' % (len(self.branches)))
    for branch in self.branches:
      output.write('%s ' % (branch))
    output.write('%s\n' % self.fname)

  def symbolic_names(self):
    return self.tags + self.branches

  # Returns true if this CVSRevision is the opening CVSRevision for
  # NAME (for this RCS file).
  def opens_symbolic_name(self, name):
    if name in self.tags:
      return 1
    if name in self.branches:
      return 1
    return 0

  ###TODO This needs to be deleted when the redesign is done.
  def contains_symbolic_name(self, name):
    if name in self.tags:
      return 1
    if name in self.branches:
      return 1
    ###TODO REMOVING THIS IS NOT CORRECT because if we do, the
    ###symbolicnametracker blows away the branch in the
    ###symbolicnamesdb before the last commits on that branch.
    if self.branch_name == name:
      return 1
    return 0

  def is_default_branch_revision(self):
    """Return 1 if SELF.rev of SELF.cvs_path is a default branch
    revision according to DEFAULT_BRANCHES_DB (see the conditions
    documented there), else return None."""
    if self._ctx.default_branches_db.has_key(self.cvs_path):
      val = self._ctx.default_branches_db[self.cvs_path]
      val_last_dot = val.rindex(".")
      our_last_dot = self.rev.rindex(".")
      default_branch = val[:val_last_dot]
      our_branch = self.rev[:our_last_dot]
      default_rev_component = int(val[val_last_dot + 1:])
      our_rev_component = int(self.rev[our_last_dot + 1:])
      if (default_branch == our_branch
          and our_rev_component <= default_rev_component):
        return 1
    # else
    return None


class CollectData(rcsparse.Sink):
  def __init__(self, log_fname_base, ctx):
    self._ctx = ctx
    ### TODO self.ctx.* can be accessed through self._ctx.
    self.cvsroot = ctx.cvsroot
    self.revs = open(log_fname_base + REVS_SUFFIX, 'w')
    Cleanup().register(log_fname_base + REVS_SUFFIX, pass2)
    self.resync = open(log_fname_base + RESYNC_SUFFIX, 'w')
    Cleanup().register(log_fname_base + RESYNC_SUFFIX, pass2)
    self.default_branches_db = ctx.default_branches_db
    self.metadata_db = Database(METADATA_DB, 'n')
    Cleanup().register(METADATA_DB, pass8)
    self.fatal_errors = []
    self.next_faked_branch_num = 999999
    
    # Branch and tag label types.
    self.BRANCH_LABEL = 0
    self.VENDOR_BRANCH_LABEL = 1
    self.TAG_LABEL = 2
    # A label type to string conversion list
    self.LABEL_TYPES = [ 'branch', 'vendor branch', 'tag' ]
    # A dict mapping label names to types
    self.label_type = { }

    # A list of labels that are to be treated as branches even if they
    # are defined as tags in CVS.
    self.forced_branches = ctx.forced_branches
    # A list of labels that are to be treated as tags even if they are
    # defined as branches in CVS.
    self.forced_tags = ctx.forced_tags
    # A list of branches that may not contain any commits because
    # it is forced to be treated as a tag by the user.
    self.forced_tag_branches = { }

    # See set_fname() for initializations of other variables.

  def set_fname(self, fname):
    "Prepare to receive data for a new file."
    self.fname = fname

    # revision -> [timestamp, author, operation, old-timestamp]
    self.rev_data = { }

    # Maps revision number (key) to the revision number of the
    # previous revision along this line of development.
    #
    # For the first revision R on a branch, we consider the revision
    # from which R sprouted to be the 'previous'.
    #
    # Note that this revision can't be determined arithmetically (due
    # to cvsadmin -o, which is why this is necessary).
    self.prev_rev = { }

    # This dict is essentially self.prev_rev with the values mapped in
    # the other direction, so following key -> value will yield you
    # the next revision number
    self.next_rev = { }

    # Track the state of each revision so that in set_revision_info,
    # we can determine if our op is an add/change/delete.  We can do
    # this because in set_revision_info, we'll have all of the
    # revisions for a file at our fingertips, and we need to examine
    # the state of our prev_rev to determine if we're an add or a
    # change--without the state of the prev_rev, we are unable to
    # distinguish between an add and a change.
    self.rev_state = { }

    # Hash mapping branch numbers, like '1.7.2', to branch names,
    # like 'Release_1_0_dev'.
    self.branch_names = { }

    # RCS flags (used for keyword expansion).
    self.mode = None
    
    # Hash mapping revision numbers, like '1.7', to lists of names
    # indicating which branches sprout from that revision, like
    # ['Release_1_0_dev', 'experimental_driver', ...].
    self.branchlist = { }

    # Like self.branchlist, but the values are lists of tag names that
    # apply to the key revision.
    self.taglist = { }

    # This is always a number -- rcsparse calls this the "principal
    # branch", but CVS and RCS refer to it as the "default branch",
    # so that's what we call it, even though the rcsparse API setter
    # method is still 'set_principal_branch'.
    self.default_branch = None

    # If the RCS file doesn't have a default branch anymore, but does
    # have vendor revisions, then we make an educated guess that those
    # revisions *were* the head of the default branch up until the
    # commit of 1.2, at which point the file's default branch became
    # trunk.  This records the date at which 1.2 was committed.
    self.first_non_vendor_revision_date = None

    # A list of branches for which we have already shown an error for
    # the current file.
    self.forced_tag_error_branches = [ ]

  def set_principal_branch(self, branch):
    self.default_branch = branch

  def set_expansion(self, mode):
    self.mode = mode
    
  def set_branch_name(self, branch_number, name):
    """Record that BRANCH_NUMBER is the branch number for branch NAME,
    and that NAME sprouts from BRANCH_NUMBER .
    BRANCH_NUMBER is an RCS branch number with an odd number of components,
    for example '1.7.2' (never '1.7.0.2')."""
    if not self.branch_names.has_key(branch_number):
      self.branch_names[branch_number] = name
      # The branchlist is keyed on the revision number from which the
      # branch sprouts, so strip off the odd final component.
      sprout_rev = branch_number[:branch_number.rfind(".")]
      if not self.branchlist.has_key(sprout_rev):
        self.branchlist[sprout_rev] = []
      self.branchlist[sprout_rev].append(name)
    else:
      sys.stderr.write("%s: in '%s':\n"
                       "   branch '%s' already has name '%s',\n"
                       "   cannot also have name '%s', ignoring the latter\n"
                       % (warning_prefix, self.fname, branch_number,
                          self.branch_names[branch_number], name))

  def rev_to_branch_name(self, revision):
    """Return the name of the branch on which REVISION lies.
    REVISION is a non-branch revision number with an even number of,
    components, for example '1.7.2.1' (never '1.7.2' nor '1.7.0.2').
    For the convenience of callers, REVISION can also be a trunk
    revision such as '1.2', in which case just return None."""
    if trunk_rev.match(revision):
      return None
    return self.branch_names.get(revision[:revision.rindex(".")])

  def add_cvs_branch(self, revision, branch_name):
    """Record the root revision and branch revision for BRANCH_NAME,
    based on REVISION.  REVISION is a CVS branch number having an even
    number of components where the second-to-last is '0'.  For
    example, if it's '1.7.0.2', then record that BRANCH_NAME sprouts
    from 1.7 and has branch number 1.7.2."""
    last_dot = revision.rfind(".")
    branch_rev = revision[:last_dot]
    last2_dot = branch_rev.rfind(".")
    branch_rev = branch_rev[:last2_dot] + revision[last_dot:]
    self.set_branch_name(branch_rev, branch_name)

  def get_tags(self, revision):
    """Return a list of all tag names attached to REVISION.
    REVISION is a regular revision number like '1.7', and the result
    never includes branch names, only plain tags."""
    return self.taglist.get(revision, [])

  def get_branches(self, revision):
    """Return a list of all branch names that sprout from REVISION.
    REVISION is a regular revision number like '1.7'."""
    return self.branchlist.get(revision, [])

  def define_tag(self, name, revision):
    """Record a bidirectional mapping between symbolic NAME and REVISION.
    REVISION is an unprocessed revision number from the RCS file's
    header, for example: '1.7', '1.7.0.2', or '1.1.1' or '1.1.1.1'.
    This function will determine what kind of symbolic name it is by
    inspection, and record it in the right places."""
    if not symbolic_name_re.match(name):
      sys.stderr.write("%s: in '%s':\n"
                       "   '%s' is not a valid tag or branch name, ignoring\n"
                       % (warning_prefix, self.fname, name))
      return

    if branch_tag.match(revision):
      label_type = self.BRANCH_LABEL
    elif vendor_tag.match(revision):
      label_type = self.VENDOR_BRANCH_LABEL
    else:
      label_type = self.TAG_LABEL

    if name in self.forced_branches:
      if label_type == self.VENDOR_BRANCH_LABEL:
        sys.exit(error_prefix + "Error: --force-branch does not work "
                 "with vendor branches.")
      elif label_type == self.TAG_LABEL:
        # Append a faked branch suffix to the revision to simulate
        # an empty branch.
        revision += ".0." + str(self.next_faked_branch_num)
        self.next_faked_branch_num += 1
        label_type = self.BRANCH_LABEL
    elif name in self.forced_tags:
      if label_type == self.VENDOR_BRANCH_LABEL:
        sys.exit(error_prefix + "Error: --force-tag does not work "
                 "with vendor branches.")
      elif label_type == self.BRANCH_LABEL:
        # Remove the .0.N suffix, where N is the branch number.  Store
        # the branch revision prefix in forced_tag_branches so that we
        # can detect if the branch is not empty.
        rev_parts = revision.split(".")
        branch_num = rev_parts[-1]
        revision = string.join(rev_parts[:-2], ".")
        self.forced_tag_branches[revision + "." + branch_num] = name
        label_type = self.TAG_LABEL

    if label_type == self.BRANCH_LABEL:
      self.add_cvs_branch(revision, name)
    elif label_type == self.VENDOR_BRANCH_LABEL:
      self.set_branch_name(revision, name)
    else:
      if not self.taglist.has_key(revision):
        self.taglist[revision] = []
      self.taglist[revision].append(name)

    try:
      # if label_types are different and at least one is a tag (We
      # don't want to error on branch/vendor branch mismatches)
      if (self.label_type[name] != label_type
          and(self.label_type[name] == self.TAG_LABEL
              or label_type == self.TAG_LABEL)):
        err = ("%s: in '%s' (BRANCH/TAG MISMATCH):\n   '%s' "
               " is defined as %s here, but as a %s elsewhere"
               % (error_prefix, self.fname, name,
                  self.LABEL_TYPES[label_type],
                  self.LABEL_TYPES[self.label_type[name]]))
        sys.stderr.write(err + '\n')
        self.fatal_errors.append(err)
    except KeyError:
      self.label_type[name] = label_type

  def define_revision(self, revision, timestamp, author, state,
                      branches, next):

    # Record the state of our revision for later calculations
    self.rev_state[revision] = state

    # Check that this revision is not on a branch that has been forced
    # to be a tag.
    branch_rev = revision[:revision.rfind('.')]
    if (self.forced_tag_branches.has_key(branch_rev)
        and branch_rev not in self.forced_tag_error_branches):
      err = ("%s: in '%s':\n   "
             "'%s' cannot be forced to be a tag because it contains commits" %
             (error_prefix, self.fname, self.forced_tag_branches[branch_rev]))
      sys.stderr.write(err + '\n')
      self.fatal_errors.append(err)
      self.forced_tag_error_branches.append(branch_rev)

    # store the rev_data as a list in case we have to jigger the timestamp
    self.rev_data[revision] = [int(timestamp), author, None]

    # When on trunk, the RCS 'next' revision number points to what
    # humans might consider to be the 'previous' revision number.  For
    # example, 1.3's RCS 'next' is 1.2.
    #
    # However, on a branch, the RCS 'next' revision number really does
    # point to what humans would consider to be the 'next' revision
    # number.  For example, 1.1.2.1's RCS 'next' would be 1.1.2.2.
    #
    # In other words, in RCS, 'next' always means "where to find the next
    # deltatext that you need this revision to retrieve.
    #
    # That said, we don't *want* RCS's behavior here, so we determine
    # whether we're on trunk or a branch and set self.prev_rev
    # accordingly.
    #
    # One last thing.  Note that if REVISION is a branch revision,
    # instead of mapping REVISION to NEXT, we instead map NEXT to
    # REVISION.  Since we loop over all revisions in the file before
    # doing anything with the data we gather here, this 'reverse
    # assignment' effectively does the following:
    #
    # 1. Gives us no 'prev' value for REVISION (in this
    # iteration... it may have been set in a previous iteration)
    #
    # 2. Sets the 'prev' value for the revision with number NEXT to
    # REVISION.  So when we come around to the branch revision whose
    # revision value is NEXT, its 'prev' and 'prev_rev' are already
    # set.
    if trunk_rev.match(revision):
      self.prev_rev[revision] = next
      self.next_rev[next] = revision
    elif next:
      self.prev_rev[next] = revision
      self.next_rev[revision] = next

    for b in branches:
      self.prev_rev[b] = revision

    # Ratchet up the highest vendor head revision, if necessary.
    if self.default_branch:
      if revision.find(self.default_branch) == 0:
        # This revision is on the default branch, so record that it is
        # the new highest vendor head revision.
        rel_name = relative_name(self.cvsroot, self.fname)[:-2]
        self.default_branches_db[rel_name] = revision
    else:
      # No default branch, so make an educated guess.
      if revision == '1.2':
        # This is probably the time when the file stopped having a
        # default branch, so make a note of it.
        self.first_non_vendor_revision_date = timestamp
      else:
        m = vendor_revision.match(revision)
        if m and ((not self.first_non_vendor_revision_date)
                  or (timestamp < self.first_non_vendor_revision_date)):
          # We're looking at a vendor revision, and it wasn't
          # committed after this file lost its default branch, so bump
          # the maximum trunk vendor revision in the permanent record.
          rel_name = relative_name(self.cvsroot, self.fname)[:-2]
          self.default_branches_db[rel_name] = revision

    # Check for unlabeled branches, record them.  We tried to collect
    # all branch names when we parsed the symbolic name header
    # earlier, of course, but that didn't catch unlabeled branches.
    # If a branch is unlabeled, this is our first encounter with it,
    # so we have to record its data now.
    if not trunk_rev.match(revision):
      branch_number = revision[:revision.rindex(".")]
      branch_name = "unlabeled-" + branch_number
      if not self.branch_names.has_key(branch_number):
        self.set_branch_name(branch_number, branch_name)

  def tree_completed(self):
    "The revision tree has been parsed. Analyze it for consistency."

    # Our algorithm depends upon the timestamps on the revisions occuring
    # monotonically over time. That is, we want to see rev 1.34 occur in
    # time before rev 1.35. If we inserted 1.35 *first* (due to the time-
    # sorting), and then tried to insert 1.34, we'd be screwed.

    # to perform the analysis, we'll simply visit all of the 'previous'
    # links that we have recorded and validate that the timestamp on the
    # previous revision is before the specified revision

    # if we have to resync some nodes, then we restart the scan. just keep
    # looping as long as we need to restart.
    while 1:
      for current, prev in self.prev_rev.items():
        if not prev:
          # no previous revision exists (i.e. the initial revision)
          continue
        t_c = self.rev_data[current][0]
        t_p = self.rev_data[prev][0]
        if t_p >= t_c:
          # the previous revision occurred later than the current revision.
          # shove the previous revision back in time (and any before it that
          # may need to shift).
          while t_p >= t_c:
            self.rev_data[prev][0] = t_c - 1	# new timestamp
            self.rev_data[prev][2] = t_p	# old timestamp

            #print "RESYNC: '%s' (%s) : old time='%s' new time='%s'" \
            #      % (relative_name(self.cvsroot, self.fname),
            #         prev, time.ctime(t_p), time.ctime(t_c - 1))

            current = prev
            prev = self.prev_rev[current]
            if not prev:
              break
            t_c = t_c - 1		# self.rev_data[current][0]
            t_p = self.rev_data[prev][0]

          # break from the for-loop
          break
      else:
        # finished the for-loop (no resyncing was performed)
        return

  def set_revision_info(self, revision, log, text):
    timestamp, author, old_ts = self.rev_data[revision]
    digest = sha.new(log + '\0' + author).hexdigest()
    if old_ts:
      # the timestamp on this revision was changed. log it for later
      # resynchronization of other files's revisions that occurred
      # for this time and log message.
      self.resync.write('%08lx %s %08lx\n' % (old_ts, digest, timestamp))

    # "...Give back one kadam to honor the Hebrew God whose Ark this is."
    #       -- Imam to Indy and Sallah, in 'Raiders of the Lost Ark'
    #
    # If revision 1.1 appears to have been created via 'cvs add'
    # instead of 'cvs import', then this file probably never had a
    # default branch, so retroactively remove its record in the
    # default branches db.  The test is that the log message CVS uses
    # for 1.1 in imports is "Initial revision\n" with no period.
    if revision == '1.1' and log != 'Initial revision\n':
      rel_name = relative_name(self.cvsroot, self.fname)[:-2]
      if self.default_branches_db.has_key(rel_name):
        del self.default_branches_db[rel_name]

    # How to tell if a CVSRevision is an add, a change, or a deletion:
    #
    # It's a delete if RCS state is 'dead'
    #
    # It's an add if RCS state is 'Exp.' and 
    #      - we either have no previous revision
    #        or 
    #      - we have a previous revision whose state is 'dead'
    # 
    # Anything else is a change.
    if self.rev_state[revision] == 'dead':
      op = OP_DELETE
    elif ((self.prev_rev.get(revision, None) is None)
          or (self.rev_state[self.prev_rev[revision]] == 'dead')):
      op = OP_ADD
    else:
      op = OP_CHANGE

    if text:
      deltatext_code = DELTATEXT_NONEMPTY
    else:
      deltatext_code = DELTATEXT_EMPTY

    c_rev = CVSRevision(self._ctx, timestamp, digest, op,
                        self.prev_rev.get(revision, '*'), revision,
                        self.next_rev.get(revision, '*'),
                        deltatext_code, self.fname,
                        self.mode, self.rev_to_branch_name(revision),
                        self.get_tags(revision),
                        self.get_branches(revision))
    c_rev.write_revs_line(self.revs)

    if not self.metadata_db.has_key(digest):
      self.metadata_db[digest] = (author, log)

def run_command(command):
  if os.system(command):
    sys.exit('Command failed: "%s"' % command)

def make_path(ctx, path, branch_name = None, tag_name = None):
  """Return the trunk path, branch path, or tag path for PATH.
  CTX holds the name of the branches or tags directory, which is
  prepended to PATH when constructing a branch or tag path.

  If PATH is empty or None, return the root trunk|branch|tag path.

  It is an error to pass both a BRANCH_NAME and a TAG_NAME."""

  # For a while, we treated each top-level subdir of the CVS
  # repository as a "project root" and interpolated the appropriate
  # genealogy (trunk|tag|branch) in according to the official
  # recommended layout.  For example, the path '/foo/bar/baz.c' on
  # branch 'Rel2' would become
  #
  #   /foo/branches/Rel2/bar/baz.c
  #
  # and on trunk it would become
  #
  #   /foo/trunk/bar/baz.c
  #
  # However, we went back to the older and simpler method of just
  # prepending the genealogy to the front, instead of interpolating.
  # So now we produce:
  #
  #   /branches/Rel2/foo/bar/baz.c
  #   /trunk/foo/bar/baz.c
  #
  # Why?  Well, Jack Repenning pointed out that this way is much
  # friendlier to "anonymously rooted subtrees" (that's a tree where
  # the name of the top level dir doesn't matter, the point is that if
  # you cd into it and, say, run 'make', something good will happen).
  # By interpolating, we made it impossible to point cvs2svn at some
  # subdir in the CVS repository and convert it as a project, because
  # we'd treat every subdir underneath it as an independent project
  # root, which is probably not what the user wanted.
  #
  # Also, see Blair Zajac's post
  #
  #    http://subversion.tigris.org/servlets/ReadMsg?list=dev&msgNo=38965
  #
  # and the surrounding thread, for why what people really want is a
  # way of specifying an in-repository prefix path, not interpolation.

  # Check caller sanity.
  if branch_name and tag_name:
    sys.stderr.write("%s: make_path() miscalled: both branch and tag given.\n"
                     % error_prefix)
    sys.exit(1)

  if branch_name:
    branch_name = _clean_symbolic_name(branch_name)
    if path:
      return ctx.branches_base + '/' + branch_name + '/' + path
    else:
      return ctx.branches_base + '/' + branch_name
  elif tag_name:
    tag_name = _clean_symbolic_name(tag_name)
    if path:
      return ctx.tags_base + '/' + tag_name + '/' + path
    else:
      return ctx.tags_base + '/' + tag_name
  else:
    if path:
      return ctx.trunk_base + '/' + path
    else:
      return ctx.trunk_base


def relative_name(cvsroot, fname):
  l = len(cvsroot)
  if fname[:l] == cvsroot:
    if fname[l] == os.sep:
      return string.replace(fname[l+1:], os.sep, '/')
    return string.replace(fname[l:], os.sep, '/')
  sys.stderr.write("%s: relative_path('%s', '%s'): fname is not a sub-path of"
                   " cvsroot\n" % (error_prefix, cvsroot, fname))
  sys.exit(1)


def visit_file(arg, dirname, files):
  cd, p, stats = arg
  for fname in files:
    if fname[-2:] != ',v':
      continue
    pathname = os.path.join(dirname, fname)
    if dirname[-6:] == ATTIC:
      # drop the 'Attic' portion from the pathname
      ### we should record this so we can easily insert it back in
      cd.set_fname(os.path.join(dirname[:-6], fname))
    else:
      cd.set_fname(pathname)
    print pathname
    try:
      p.parse(open(pathname, 'rb'), cd)
      stats[0] = stats[0] + 1
    except (rcsparse.common.RCSParseError, ValueError, RuntimeError):
      err = "%s: '%s' is not a valid ,v file" \
            % (error_prefix, pathname)
      sys.stderr.write(err + '\n')
      cd.fatal_errors.append(err)
    except:
      print "Exception occurred while parsing %s" % pathname
      raise


# Return a string that has not been returned by gen_key() before.
gen_key_base = 0L
def gen_key():
  global gen_key_base
  key = '%x' % gen_key_base
  gen_key_base = gen_key_base + 1
  return key


class Change:
  """Class for recording what actually happened when a change is made,
  because not all of the result is guessable by the caller.
  See RepositoryMirror.change_path() for more.

  The fields are

    op:
       OP_ADD path was added, OP_CHANGE if changed, or OP_NOOP if no
       action.

    closed_tags:
       List of tags that this path can no longer be the source of,
       that is, tags which could be rooted in the path before the
       change, but not after.

    closed_branches:
       Like closed_tags, but for branches.

    deleted_entries:
       The list of entries deleted from the destination after
       copying a directory, or None.

    copyfrom_rev:
       The actual revision from which the path was copied, which
       may be one less than the requested revision when the path
       was deleted in the requested revision, or None."""
  def __init__(self, op, closed_tags, closed_branches,
               deleted_entries=None, copyfrom_rev=None):
    self.op = op
    self.closed_tags = closed_tags
    self.closed_branches = closed_branches
    self.deleted_entries = deleted_entries
    self.copyfrom_rev = copyfrom_rev


class RepositoryMirror:
  def __init__(self):
    # This corresponds to the 'revisions' table in a Subversion fs.
    self.revs_db_file = SVN_REVISIONS_DB
    self.revs_db = Database(self.revs_db_file, 'n')
    Cleanup().register(self.revs_db_file, pass8)

    # This corresponds to the 'nodes' table in a Subversion fs.  (We
    # don't need a 'representations' or 'strings' table because we
    # only track metadata, not file contents.)
    self.nodes_db_file = NODES_DB
    self.nodes_db = Database(self.nodes_db_file, 'n')
    Cleanup().register(self.nodes_db_file, pass8)

    # This tracks which symbolic names the current "head" of a given
    # filepath could be the origin node for.  When the next commit on
    # that path comes along, we can tell which symbolic names
    # originated in the previous version, and signal back to the
    # caller that the file can no longer be the origin for those names.
    #
    # The values are tuples, (tags, branches), where each value is a
    # list.
    self.symroots_db_file = SYMBOLIC_NAME_ROOTS_DB
    self.symroots_db = Database(self.symroots_db_file, 'n')
    Cleanup().register(self.symroots_db_file, pass8)

    # When copying a directory (say, to create part of a branch), we
    # pass change_path() a list of expected entries, so it can remove
    # any that are in the source but don't belong on the branch.
    # However, because creating a given region of a branch can involve
    # copying from several sources, we don't want later copy
    # operations to delete entries that were legitimately created by
    # earlier copy ops.  So after a copy, the directory records
    # legitimate entries under this key, in a dictionary (the keys are
    # entry names, the values can be ignored).
    self.approved_entries = "/approved-entries"

    # Set to a true value on a directory that's mutable in the
    # revision currently being constructed. (Yes, this is exactly
    # analogous to the Subversion filesystem code's concept of
    # mutability.)
    # Is also overloaded with a second piece of information.
    # If the value of the flag is 2, then in addition to the node
    # being mutable, the node and all subnodes were created by a copy
    # operation in the current revision. In this and only this
    # circumstance, it is valid for pruning to occur.
    self.mutable_flag = "/mutable"
    # This could represent a new mutable directory or file.
    self.empty_mutable_thang = { self.mutable_flag : 1 }

    # Init a root directory with no entries at revision 0.
    self.youngest = 0
    youngest_key = gen_key()
    self.revs_db[str(self.youngest)] = youngest_key
    self.nodes_db[youngest_key] = {}

  def new_revision(self):
    """Stabilize the current revision, then start the next one.
    (Increments youngest.)"""
    self.stabilize_youngest()
    self.revs_db[str(self.youngest + 1)] \
                                      = self.revs_db[str(self.youngest)]
    self.youngest = self.youngest + 1

  def _stabilize_directory(self, key):
    """Close the directory whose node key is KEY."""
    dir = self.nodes_db[key]
    if dir.has_key(self.mutable_flag):
      del dir[self.mutable_flag]
      if dir.has_key(self.approved_entries):
        del dir[self.approved_entries]
      for entry_key in dir.keys():
        if not entry_key[0] == '/':
          self._stabilize_directory(dir[entry_key])
      self.nodes_db[key] = dir

  def stabilize_youngest(self):
    """Stabilize the current revision by removing mutable flags."""
    root_key = self.revs_db[str(self.youngest)]
    self._stabilize_directory(root_key)

  def probe_path(self, path, revision=-1, debugging=None):
    """If PATH exists in REVISION of the svn repository mirror,
    return its leaf value, else return None.
    If DEBUGGING is true, then print trace output to stdout.
    REVISION defaults to youngest, and PATH must not start with '/'."""
    components = string.split(path, '/')
    if revision == -1:
      revision = self.youngest

    if debugging:
      print "PROBING path: '%s' in %d" % (path, revision)

    parent_key = self.revs_db[str(revision)]
    parent = self.nodes_db[parent_key]
    previous_component = "/"

    i = 1
    for component in components:

      if debugging:
        print "  " * i,
        print "'%s' key: %s, val:" % (previous_component, parent_key), parent

      if not parent.has_key(component):
        if debugging:
          print "  PROBE ABANDONED: '%s' does not contain '%s'" \
                % (previous_component, component)
        return None

      this_entry_key = parent[component]
      this_entry_val = self.nodes_db[this_entry_key]
      parent_key = this_entry_key
      parent = this_entry_val
      previous_component = component
      i = i + 1
  
    if debugging:
      print "  " * i,
      print "parent_key: %s, val:" % parent_key, parent

    # It's not actually a parent at this point, it's the leaf node.
    return parent

  def change_path(self, path, tags, branches,
                  intermediate_dir_func=None,
                  copyfrom_path=None, copyfrom_rev=None,
                  expected_entries=None, only_if_already_exists=None):
    """Record a change to PATH.  PATH may not have a leading slash.
    Return a Change instance representing the result of the
    change.

    TAGS are any tags that sprout from this revision of PATH, BRANCHES
    are any branches that sprout from this revision of PATH.

    If INTERMEDIATE_DIR_FUNC is not None, then invoke it once on
    each full path to each missing intermediate directory in PATH, in
    order from shortest to longest.

    If COPYFROM_REV and COPYFROM_PATH are not None, then they are a
    revision and path to record as the copyfrom sources of this node.
    Since this implies an add (OP_ADD), it would be reasonable to
    error and exit if the copyfrom args are present but the node also
    already exists.  Reasonable -- but not what we do :-).  The most
    useful behavior for callers is instead to report that nothing was
    done, by returning OP_NOOP for Change.op, so that's what we do.

    It is an error for only one copyfrom argument to be present.

    If EXPECTED_ENTRIES is not None, then it holds entries expected
    to be in the dst after the copy.  Any entries in the new dst but
    not in EXPECTED_ENTRIES are removed (ignoring keys beginning with
    '/'), and the removed entries returned in Change.deleted_entries,
    which are otherwise None.

    No action is taken for keys in EXPECTED_ENTRIES but not in the
    dst; it is assumed that the caller will compensate for these by
    calling change_path again with other arguments.
    
    If ONLY_IF_ALREADY_EXISTS is set, then do a no-op, rather than an add,
    if the path does not exist. This is to allow pruning using EXPECTED_ENTRIES
    without risking erroneously adding a path."""

    # Check caller sanity.
    if ((copyfrom_rev and not copyfrom_path) or
        (copyfrom_path and not copyfrom_rev)):
      sys.stderr.write("%s: change_path() called with one copyfrom "
                       "argument but not the other.\n" % error_prefix)
      sys.exit(1)

    components = string.split(path, '/')
    path_so_far = None

    deletions = []
    in_pruneable_subtree = None

    parent_key = self.revs_db[str(self.youngest)]
    parent = self.nodes_db[parent_key]
    if not parent.has_key(self.mutable_flag):
      parent_key = gen_key()
      parent[self.mutable_flag] = 1
      self.nodes_db[parent_key] = parent
      self.revs_db[str(self.youngest)] = parent_key

    for component in components[:-1]:
      # parent is always mutable at the top of the loop

      if path_so_far:
        path_so_far = path_so_far + '/' + component
      else:
        path_so_far = component

      # Ensure that the parent has an entry for this component.
      if not parent.has_key(component):
        if only_if_already_exists:
          return Change(OP_NOOP, [], [], deletions)
        # else
        new_child_key = gen_key()
        parent[component] = new_child_key
        self.nodes_db[new_child_key] = self.empty_mutable_thang
        self.nodes_db[parent_key] = parent
        if intermediate_dir_func:
          intermediate_dir_func(path_so_far)

      # One way or another, parent dir now has an entry for component,
      # so grab it, see if it's mutable, and DTRT if it's not.  (Note
      # it's important to reread the entry value from the db, even
      # though we might have just written it -- if we tweak existing
      # data structures, we could modify self.empty_mutable_thang,
      # which must not happen.)
      this_entry_key = parent[component]
      this_entry_val = self.nodes_db[this_entry_key]
      mutable = this_entry_val.get(self.mutable_flag)
      if not mutable:
        this_entry_val[self.mutable_flag] = 1
        this_entry_key = gen_key()
        parent[component] = this_entry_key
        self.nodes_db[this_entry_key] = this_entry_val
        self.nodes_db[parent_key] = parent
      elif mutable == 2:
        in_pruneable_subtree = 1

      parent_key = this_entry_key
      parent = this_entry_val

    # Now change the last node, the versioned file.  Just like at the
    # top of the above loop, parent is already mutable.
    op = OP_ADD
    if self.symroots_db.has_key(path):
      old_names = self.symroots_db[path]
    else:
      old_names = [], []
    last_component = components[-1]
    new_val = { }
    if parent.has_key(last_component):
      # The contract for copying over existing nodes is to do nothing
      # and return:
      if copyfrom_path:
        return Change(OP_NOOP, old_names[0], old_names[1], deletions)
      # else
      op = OP_CHANGE
      new_val = self.nodes_db[parent[last_component]]
    elif only_if_already_exists:
      return Change(OP_NOOP, [], [], deletions)

    leaf_key = gen_key()
    if copyfrom_path:
      new_val = self.probe_path(copyfrom_path, copyfrom_rev)
      if new_val is None:
        # Sometimes a branch is rooted in a revision that RCS has
        # marked as 'dead'. There is no reason to assume that the
        # current path shares any history with any older live parent
        # of the dead revision, so we do nothing and return.
        return Change(OP_NOOP, [], [], deletions)
      # Special value of mutable flag indicates that this subtree was created
      # by copying in this revision. Iff this is true, then it is valid to
      # use expected_entries to prune items.
      new_val[self.mutable_flag] = 2
      in_pruneable_subtree = 1
    else:
      new_val[self.mutable_flag] = 1
    if expected_entries is not None:
      # If it is not None, then even if it is an empty list/tuple,
      # we need to approve this item in its parent's approved entries list.
      approved_entries = parent.get(self.approved_entries) or {}
      approved_entries[last_component] = 1
      parent[self.approved_entries] = approved_entries
    if expected_entries:
      approved_entries = new_val.get(self.approved_entries) or { }
      new_approved_entries = { }
      for ent in new_val.keys():
        if (ent[0] != '/'):
          if (not expected_entries.has_key(ent)
              and not approved_entries.has_key(ent)):
            if in_pruneable_subtree:
              del new_val[ent]
              deletions.append(ent)
          else:
            new_approved_entries[ent] = 1
      new_val[self.approved_entries] = new_approved_entries
    parent[last_component] = leaf_key
    self.nodes_db[parent_key] = parent
    self.symroots_db[path] = (tags, branches)
    self.nodes_db[leaf_key] = new_val

    return Change(op, old_names[0], old_names[1], deletions, copyfrom_rev)

  def delete_path(self, path, prune=None):
    """Delete PATH from the tree.  PATH may not have a leading slash.

    Return a tuple (path_deleted, closed_tags, closed_branches), where
    path_deleted is the path actually deleted or None if PATH did not
    exist, and closed_tags and closed_branches are lists of symbolic
    names closed off by this deletion -- that is, tags or branches
    which could be rooted in the previous revision of PATH, but not in
    this revision, because this rev changes PATH.  If path_deleted is
    None, then closed_tags and closed_branches will both be empty.

    If PRUNE is not None, then delete the highest possible directory,
    which means the returned path may differ from PATH.  In other
    words, if PATH was the last entry in its parent, then delete
    PATH's parent, unless it too is the last entry in *its* parent, in
    which case delete that parent, and so on up the chain, until a
    directory is encountered that has an entry which is not a member
    of the parent stack of the original target.

    NOTE: This function does *not* allow you delete top-level entries
    (like /trunk, /branches, /tags), not does it prune upwards beyond
    those entries.

    PRUNE is like the -P option to 'cvs checkout'."""

    components = string.split(path, '/')
    path_so_far = None

    parent_key = self.revs_db[str(self.youngest)]
    parent = self.nodes_db[parent_key]

    # As we walk down to find the dest, we remember each parent
    # directory's name and db key, in reverse order: push each new key
    # onto the front of the list, so that by the time we reach the
    # destination node, the zeroth item in the list is the parent of
    # that destination.
    #
    # Then if we actually do the deletion, we walk the list from left
    # to right, replacing as appropriate.
    #
    # The root directory has name None.
    parent_chain = [ ]
    parent_chain.insert(0, (None, parent_key))

    def is_prunable(dir):
      """Return true if DIR, a dictionary representing a directory,
      has just zero or one non-special entry, else return false.
      (In a pure world, we'd just ask len(DIR) > 1; it's only
      because the directory might have mutable flags and other special
      entries that we need this function at all.)"""
      num_items = len(dir)
      if num_items > 3:
        return None
      if num_items == 3 or num_items == 2:
        real_entries = 0
        for key in dir.keys():
          if not key[0] == '/': real_entries = real_entries + 1
        if real_entries > 1:
          return None
        else:
          return 1
      else:
        return 1

    # We never prune our top-level directories (/trunk, /tags, /branches)
    if len(components) < 2:
      return None, [], []
    
    for component in components[:-1]:
      if path_so_far:
        path_so_far = path_so_far + '/' + component
      else:
        path_so_far = component

      # If we can't reach the dest, then we don't need to do anything.
      if not parent.has_key(component):
        return None, [], []

      # Otherwise continue downward, dropping breadcrumbs.
      this_entry_key = parent[component]
      this_entry_val = self.nodes_db[this_entry_key]
      parent_key = this_entry_key
      parent = this_entry_val
      parent_chain.insert(0, (component, parent_key))

    # If the target is not present in its parent, then we're done.
    last_component = components[-1]
    old_names = [], []
    if not parent.has_key(last_component):
      return None, [], []
    elif self.symroots_db.has_key(path):
      old_names = self.symroots_db[path]
      del self.symroots_db[path]

    # The target is present, so remove it and bubble up, making a new
    # mutable path and/or pruning as necessary.
    pruned_count = 0
    prev_entry_name = last_component
    new_key = None
    for parent_item in parent_chain:
      pkey = parent_item[1]
      pval = self.nodes_db[pkey]

      # If we're pruning at all, and we're looking at a prunable thing
      # (and that thing isn't one of our top-level directories --
      # trunk, tags, branches) ...
      if prune and (new_key is None) and is_prunable(pval) \
         and parent_item != parent_chain[-2]:
        # ... then up our count of pruned items, and do nothing more.
        # All the action takes place when we hit a non-prunable
        # parent.
        pruned_count = pruned_count + 1
      else:
        # Else, we've hit a non-prunable, or aren't pruning, so bubble
        # up the new gospel.
        pval[self.mutable_flag] = 1
        if new_key is None:
          del pval[prev_entry_name]
        else:
          pval[prev_entry_name] = new_key
        new_key = gen_key()

      prev_entry_name = parent_item[0]
      if new_key:
        self.nodes_db[new_key] = pval

    if new_key is None:
      new_key = gen_key()
      self.nodes_db[new_key] = self.empty_mutable_thang

    # Install the new root entry.
    self.revs_db[str(self.youngest)] = new_key

    # Sanity check -- this should be a "can't happen".
    if pruned_count > len(components):
      sys.stderr.write("%s: deleting '%s' tried to prune %d components.\n"
                       % (error_prefix, path, pruned_count))
      sys.exit(1)

    if pruned_count:
      if pruned_count == len(components):
        # We never prune away the root directory, so back up one component.
        pruned_count = pruned_count - 1
      retpath = string.join(components[:0 - pruned_count], '/')
    else:
      retpath = path

    return retpath, old_names[0], old_names[1]

  def close(self):
    # Just stabilize the last revision.  This may or may not affect
    # anything, but if we end up using the mirror for anything after
    # this, it's nice to know the '/mutable' entries are gone.
    self.stabilize_youngest()

if sys.platform == "win32":
  def escape_shell_arg(str):
    return '"' + string.replace(str, '"', '"^""') + '"'
else:
  def escape_shell_arg(str):
    return "'" + string.replace(str, "'", "'\\''") + "'"

class Dumper:
  def __init__(self, ctx):
    'Open DUMPFILE_PATH, and initialize revision to REVISION.'
    self.dumpfile_path = ctx.dumpfile
    self.revision = 0
    self.repos_mirror = RepositoryMirror()
    self.svnadmin = ctx.svnadmin
    self.target = ctx.target
    self.dump_only = ctx.dump_only
    self.dumpfile = None
    self.path_encoding = ctx.encoding
    self.loader_pipe = None
    
    # If all we're doing here is dumping, we can go ahead and
    # initialize our single dumpfile.  Else, if we're suppose to
    # create the repository, do so.
    if self.dump_only:
      self.init_dumpfile()
      self.write_dumpfile_header(self.dumpfile)
    else:
      if not ctx.existing_svnrepos:
        print "creating repos '%s'" % (self.target)
        run_command('%s create %s %s' % (self.svnadmin, ctx.bdb_txn_nosync
          and "--bdb-txn-nosync" or "", self.target))
      self.loader_pipe = os.popen('%s load -q %s' %
          (self.svnadmin, self.target), PIPE_WRITE_MODE)
      self.write_dumpfile_header(self.loader_pipe)

    
  def init_dumpfile(self):
    # Open the dumpfile for binary-mode write.
    self.dumpfile = open(self.dumpfile_path, 'wb')


  def write_dumpfile_header(self, fileobj):
    # Initialize the dumpfile with the standard headers:
    #
    # The CVS repository doesn't have a UUID, and the Subversion
    # repository will be created with one anyway.  So when we load
    # the dumpfile, we don't specify a UUID.
    fileobj.write('SVN-fs-dump-format-version: 2\n\n')

  def flush_and_remove_dumpfile(self):
    if self.dumpfile is None:
      return
    self.dumpfile.close()
    print "piping revision %d into '%s' loader" % (self.revision, self.target)
    dumpfile = open(self.dumpfile_path, 'rb')
    while 1:
      data = dumpfile.read(1024*1024) # Choice of 1MB chunks is arbitrary
      if not len(data): break
      self.loader_pipe.write(data)
    dumpfile.close()  

    os.remove(self.dumpfile_path)
  
  def start_revision(self, props):
    """Write the next revision, with properties, to the dumpfile.
    Return the newly started revision."""

    # If this is not a --dump-only, we need to flush (load into the
    # repository) any dumpfile data we have already written and the
    # init a new dumpfile before starting this revision.
    
    if not self.dump_only:
      if self.revision > 0:
        self.flush_and_remove_dumpfile()
      self.init_dumpfile()
      
    self.revision = self.revision + 1

    # A revision typically looks like this:
    # 
    #   Revision-number: 1
    #   Prop-content-length: 129
    #   Content-length: 129
    #   
    #   K 7
    #   svn:log
    #   V 27
    #   Log message for revision 1.
    #   K 10
    #   svn:author
    #   V 7
    #   jrandom
    #   K 8
    #   svn:date
    #   V 27
    #   2003-04-22T22:57:58.132837Z
    #   PROPS-END
    #
    # Notice that the length headers count everything -- not just the
    # length of the data but also the lengths of the lengths, including
    # the 'K ' or 'V ' prefixes.
    #
    # The reason there are both Prop-content-length and Content-length
    # is that the former includes just props, while the latter includes
    # everything.  That's the generic header form for any entity in a
    # dumpfile.  But since revisions only have props, the two lengths
    # are always the same for revisions.
    
    # Calculate the total length of the props section.
    total_len = 10  # len('PROPS-END\n')
    for propname in props.keys():
      klen = len(propname)
      klen_len = len('K %d' % klen)
      vlen = len(props[propname])
      vlen_len = len('V %d' % vlen)
      # + 4 for the four newlines within a given property's section
      total_len = total_len + klen + klen_len + vlen + vlen_len + 4
        
    # Print the revision header and props
    self.dumpfile.write('Revision-number: %d\n'
                        'Prop-content-length: %d\n'
                        'Content-length: %d\n'
                        '\n'
                        % (self.revision, total_len, total_len))

    for propname in props.keys():
      self.dumpfile.write('K %d\n' 
                          '%s\n' 
                          'V %d\n' 
                          '%s\n' % (len(propname),
                                    propname,
                                    len(props[propname]),
                                    props[propname]))

    self.dumpfile.write('PROPS-END\n')
    self.dumpfile.write('\n')

    self.repos_mirror.new_revision()
    return self.revision

  def add_dir(self, path):
    self.dumpfile.write("Node-path: %s\n" 
                        "Node-kind: dir\n"
                        "Node-action: add\n"
                        "Prop-content-length: 10\n"
                        "Content-length: 10\n"
                        "\n"
                        "PROPS-END\n"
                        "\n"
                        "\n" % self.utf8_path(path))

  def utf8_path(self, path):
    """Return UTF-8 encoded 'path' based on ctx.path_encoding."""
    try:
      ### Log messages can be converted with 'replace' strategy.
      ### We can't afford that here.
      unicode_path = unicode(path, self.path_encoding, 'strict')
      return unicode_path.encode('utf-8')
    
    except UnicodeError:
      print "Unable to convert a path '%s' to internal encoding." % path
      print "Consider rerunning with (for example) '--encoding=latin1'"
      sys.exit(1)


  def probe_path(self, path):
    """Return true if PATH exists in the youngest tree of the svn
    repository, else return None.  PATH does not start with '/'."""
    if self.repos_mirror.probe_path(path) is None:
      return None
    else:
      return 1

  def copy_path(self, svn_src_path, svn_src_rev, svn_dst_path, entries=None):
    """If it wouldn't be redundant to do so, emit a copy of SVN_SRC_PATH at
    SVN_SRC_REV to SVN_DST_PATH.

    Return 1 if the copy was done, None otherwise.

    If ENTRIES is not None, it is a dictionary whose keys are the full
    set of entries the new copy is expected to have -- and therefore
    any entries in the new dst but not in ENTRIES will be removed.
    (Keys in ENTRIES beginning with '/' are ignored.)

    No action is taken for keys in ENTRIES but not in the dst; it is
    assumed that the caller will compensate for these by calling
    copy_path again with other arguments."""
    change = self.repos_mirror.change_path(svn_dst_path,
                                           [], [],
                                           self.add_dir,
                                           svn_src_path, svn_src_rev,
                                           entries)
    if change.op == OP_ADD:
      if change.copyfrom_rev >= self.revision:
        sys.stderr.write("%s: invalid copyfrom revision %d used while\n"
                         "creating revision %d in dumpfile.\n"
                         % (error_prefix, change.copyfrom_rev, self.revision))
        sys.exit(1)
        
      # We don't need to include "Node-kind:" for copies; the loader
      # ignores it anyway and just uses the source kind instead.
      self.dumpfile.write('Node-path: %s\n'
                          'Node-action: add\n'
                          'Node-copyfrom-rev: %d\n'
                          'Node-copyfrom-path: /%s\n'
                          '\n'
                          % (self.utf8_path(svn_dst_path),
                             change.copyfrom_rev,
                             self.utf8_path(svn_src_path)))

      for ent in change.deleted_entries:
        self.dumpfile.write('Node-path: %s\n'
                            'Node-action: delete\n'
                            '\n' % (self.utf8_path(svn_dst_path + '/' + ent)))
      return 1
    return None
    
  def prune_entries(self, path, expected):
    """Delete any entries in PATH that are not in list EXPECTED.
    PATH need not be a directory, but of course nothing will happen if
    it's a file.  Entries beginning with '/' are ignored as usual."""
    change = self.repos_mirror.change_path(path,
                                           [], [],
                                           self.add_dir,
                                           None, None,
                                           expected, 1)
    for ent in change.deleted_entries:
      self.dumpfile.write('Node-path: %s\n'
                          'Node-action: delete\n'
                          '\n' % (self.utf8_path(path + '/' + ent)))

  def add_or_change_path(self, ctx, c_rev):
    # figure out the real file path for "co"
    try:
      rcs_file = c_rev.fname
      f_st = os.stat(rcs_file)
    except os.error:
      dirname, fname = os.path.split(rcs_file)
      rcs_file = os.path.join(dirname, 'Attic', fname)
      f_st = os.stat(rcs_file)

    # We begin with only a "CVS revision" property.
    if ctx.cvs_revnums:
      prop_contents = 'K 15\ncvs2svn:cvs-rev\nV %d\n%s\n' \
                      % (len(c_rev.rev), c_rev.rev)
    else:
      prop_contents = ''
    
    # Check for executable-ness.
    if f_st[0] & stat.S_IXUSR:
      prop_contents = prop_contents + 'K 14\nsvn:executable\nV 1\n*\n'

    mime_type = None
    eol_style = None
    
    # If the file is marked as binary, it gets a default MIME type of
    # "application/octet-stream".  Otherwise, it gets a default EOL
    # style of "native".
    if c_rev.mode == 'b':
      mime_type = 'application/octet-stream'
    else:
      eol_style = 'native'

    ### TODO: Deal with other expansion modes.  What do we do about
    ### the various keyword expansion styles (name only, value only,
    ### both name and value, old value, etc.)?  Beats me.
    
    # If using the MIME mapper, possibly override the default MIME
    # type and EOL style.
    if ctx.mime_mapper:
      mtype = ctx.mime_mapper.get_type_from_filename(c_rev.cvs_path)
      if mtype:
        mime_type = mtype
        if not mime_type.startswith("text/"):
          eol_style = None

    # Possibly set the svn:mime-type and svn:eol-style properties.
    if mime_type:
      prop_contents = prop_contents + ('K 13\nsvn:mime-type\nV %d\n%s\n' % \
                                       (len(mime_type), mime_type))
    if ctx.set_eol_style and eol_style:
      prop_contents = prop_contents + 'K 13\nsvn:eol-style\nV 6\nnative\n'
                                         
    # Calculate the property length (+10 for "PROPS-END\n")
    props_len = len(prop_contents) + 10
    
    ### FIXME: We ought to notice the -kb flag set on the RCS file and
    ### use it to set svn:mime-type.
    ### (How this will interact with the mime-mapper code
    ### has yet to be decided.)

    basename = os.path.basename(rcs_file[:-2])
    pipe_cmd = 'co -q -x,v -p%s %s' % (c_rev.rev, escape_shell_arg(rcs_file))
    pipe = os.popen(pipe_cmd, PIPE_READ_MODE)

    # You might think we could just test
    #
    #   if c_rev.rev[-2:] == '.1':
    #
    # to determine if this path exists in head yet.  But that wouldn't
    # be perfectly reliable, both because of 'cvs commit -r', and also
    # the possibility of file resurrection.
    change = self.repos_mirror.change_path(c_rev.svn_path, c_rev.tags,
                                           c_rev.branches, self.add_dir)

    if change.op == OP_ADD:
      action = 'add'
    else:
      action = 'change'

    self.dumpfile.write('Node-path: %s\n'
                        'Node-kind: file\n'
                        'Node-action: %s\n'
                        'Prop-content-length: %d\n'
                        'Text-content-length: '
                        % (self.utf8_path(c_rev.svn_path),
                           action, props_len))

    pos = self.dumpfile.tell()

    self.dumpfile.write('0000000000000000\n'
                        'Text-content-md5: 00000000000000000000000000000000\n'
                        'Content-length: 0000000000000000\n'
                        '\n')

    self.dumpfile.write(prop_contents + 'PROPS-END\n')

    # Insert the rev contents, calculating length and checksum as we go.
    checksum = md5.new()
    length = 0
    buf = pipe.read()
    while buf:
      checksum.update(buf)
      length = length + len(buf)
      self.dumpfile.write(buf)
      buf = pipe.read()
    if pipe.close() is not None:
      sys.exit('%s: Command failed: "%s"' % (error_prefix, pipe_cmd))

    # Go back to patch up the length and checksum headers:
    self.dumpfile.seek(pos, 0)
    # We left 16 zeros for the text length; replace them with the real
    # length, padded on the left with spaces:
    self.dumpfile.write('%16d' % length)
    # 16... + 1 newline + len('Text-content-md5: ') == 35
    self.dumpfile.seek(pos + 35, 0)
    self.dumpfile.write(checksum.hexdigest())
    # 35... + 32 bytes of checksum + 1 newline + len('Content-length: ') == 84
    self.dumpfile.seek(pos + 84, 0)
    # The content length is the length of property data, text data,
    # and any metadata around/inside around them.
    self.dumpfile.write('%16d' % (length + props_len))
    # Jump back to the end of the stream
    self.dumpfile.seek(0, 2)

    # This record is done (write two newlines -- one to terminate
    # contents that weren't themselves newline-termination, one to
    # provide a blank line for readability.
    self.dumpfile.write('\n\n')
    return change.closed_tags, change.closed_branches

  def delete_path(self, ctx, c_rev, svn_path):
    """If SVN_PATH exists in the head mirror, output the deletion to
    the dumpfile, else output nothing to the dumpfile.

    Return a tuple (path_deleted, closed_tags, closed_branches), where
    path_deleted is the path deleted if any or None if no deletion was
    necessary, and closed_tags and closed_names are lists of symbolic
    names closed off by this deletion -- that is, tags or branches
    which could be rooted in the previous revision of PATH, but not in
    this revision, because this rev changes PATH.  If path_deleted is
    None, then closed_tags and closed_branches will both be empty.

    Iff PRUNE is true, then the path deleted can be not None, yet
    shorter than SVN_PATH because of pruning."""
    deleted_path, closed_tags, closed_branches \
                  = self.repos_mirror.delete_path(svn_path, ctx.prune)
    if deleted_path:
      print "    (deleted '%s')" % deleted_path
      self.dumpfile.write('Node-path: %s\n'
                          'Node-action: delete\n'
                          '\n' % self.utf8_path(deleted_path))
    return deleted_path, closed_tags, closed_branches

  def close(self):
    self.repos_mirror.close()

    # If we're only making a dumpfile, we should be done now.  Just
    # close the dumpfile.  Otherwise, we're in "incremental" mode, and
    # we need to close our incremental dumpfile, flush it to the
    # repository, and then remove it.
    if self.dump_only:
      self.dumpfile.close()
    else:
      self.flush_and_remove_dumpfile()
      ret = self.loader_pipe.close()
      if ret:
        sys.stderr.write('%s: svnadmin load exited with error code %s' %
            (error_prefix, ret))
        sys.exit(1)


def format_date(date):
  """Return an svn-compatible date string for DATE (seconds since epoch)."""
  # A Subversion date looks like "2002-09-29T14:44:59.000000Z"
  return time.strftime("%Y-%m-%dT%H:%M:%S.000000Z", time.gmtime(date))


def make_revision_props(ctx, symbolic_name, is_tag, date=None):
  """Return a dictionary of revision properties for the manufactured
  commit that finished SYMBOLIC_NAME.  If IS_TAG is true, write the
  log message as though for a tag, else as though for a branch.
  If DATE is passed, use it as the value of the svn:date property."""
  if is_tag:
    type = 'tag'
  else:
    type = 'branch'

  # In Python 2.2.3, we could use textwrap.fill().  Oh well :-).
  if len(symbolic_name) >= 13:
    space_or_newline = '\n'
  else:
    space_or_newline = ' '

  log = "This commit was manufactured by cvs2svn to create %s%s'%s'." \
        % (type, space_or_newline, symbolic_name)
  
  return { 'svn:author' : ctx.username,
           'svn:log' : log,
           'svn:date' : date or format_date(time.time())}


class DummySymbolicNameTracker:
  def __getattr__(self, attr):
    return self.noop

  def noop(self, *foo):
    pass


class SymbolicNameTracker:
  """Track the Subversion path/revision ranges of CVS symbolic names.
  This is done in a .db file, representing a tree in the usual way.
  In addition to directory entries, each object in the database stores
  the earliest revision from which it could be copied, and the first
  revision from which it could no longer be copied.  Intermediate
  directories go one step farther: they record counts for the various
  revisions from which items under them could have been copied, and
  counts for the cutoff revisions.  For example:
                                                                      
                               .----------.                           
                               |  sub1    | [(2, 1), (3, 3)]          
                               |  /       | [(5, 1), (17, 2), (50, 1)]         
                               | /        |                                    
                               |/ sub2    |                           
                               /    \     |                           
                              /|_____\____|                           
                             /        \                               
                      ______/          \_________                     
                     /                           \                    
                    /                             \                   
                   /                               \                  
              .---------.                     .---------.             
              |  file1  |                     |  file3  |             
              |   /     | [(3, 2)]            |     \   | [(2, 1), (3, 1)] 
              |  /      | [(17, 1), (50, 1)]  |      \  | [(5, 1), (10, 1)]
              | /       |                     |       \ |                  
              |/ file2  |                     |  file4 \|                  
              /    \    |                     |    /    \                 
             /|_____\___|                     |___/_____|\                
            /        \                           /        \               
           /          \                         /          \              
          /            \                       /            \             
         /              +                     /              +            
    +======+            |                 +======+           |            
    |      | [(3, 1)]   |                 |      | [(2, 1)]  |            
    |      | [(17, 1)]  |                 |      | [(5, 1)]  |            
    |      |            |                 |      |           |            
    +======+            |                 +======+           |            
                    +======+                             +======+         
                    |      | [(3, 1)]                    |      | [(3, 1)]
                    |      | [(50, 1)]                   |      | [(17, 1)]
                    |      |                             |      |
                    +======+                             +======+
  
  The two lists to the right of each node represent the 'opening' and
  'closing' revisions respectively.  Each tuple in a list is of the
  form (REV, COUNT).  For leaf nodes, COUNT is always 1, of course.
  For intermediate nodes, the counts are the sums of the corresponding
  counts of child nodes.

  These revision scores are used to determine the optimal copy
  revisions for each tree/subtree at branch or tag creation time.

  The svn path input will most often be a trunk path, because the
  path/rev information recorded here is about where and when the given
  symbolic name could be rooted, *not* a path/rev for which commits
  along that symbolic name take place (of course, commits only happen on
  branches anyway)."""

  def __init__(self):
    self.db_file = SYMBOLIC_NAMES_DB
    self.db = Database(self.db_file, 'n')
    Cleanup().register(self.db_file, pass8)
    self.root_key = gen_key()
    self.db[self.root_key] = {}

    # The keys for the opening and closing revision lists attached to
    # each directory or file.  Includes "/" so as never to conflict
    # with any real entry.
    ### TODO These should be 2 chars when not debugging
    self.opening_revs_key = "/o"
    self.closing_revs_key = "/c"

    # When a node is copied into the repository, the revision copied
    # is stored under the appropriate key, and the corresponding
    # opening and closing rev lists are removed.
    self.copyfrom_rev_key = "/r"
    self.file_key = "/f"
    ###TODO self.tags should be stored in the symnames_db
    self.tags = {}

  def probe_path(self, symbolic_name, path, debugging=None):
    """If 'SYMBOLIC_NAME/PATH' exists in the symbolic name tree,
    return the value of its last component, else return None.
    PATH may be None, but may not start with '/'.
    If DEBUGGING is true, then print trace output to stdout."""
    if path:
      components = [symbolic_name] + string.split(path, '/')
    else:
      components = [symbolic_name]
    
    if debugging:
      print "PROBING SYMBOLIC NAME:\n", components

    parent_key = self.root_key
    parent = self.db[parent_key]
    last_component = "/"
    i = 1
    for component in components:
      if debugging:
        print "  " * i,
        print "'%s' key: %s, val:" % (last_component, parent_key), parent

      # Check for a "can't happen."
      if not parent.has_key(component):
        sys.stderr.write("%s: sym probe failed: '%s' does not contain '%s'\n"
                         % (error_prefix, last_component, component))
        sys.exit(1)

      this_entry_key = parent[component]
      this_entry_val = self.db[this_entry_key]
      parent_key = this_entry_key
      parent = this_entry_val
      last_component = component
      i = i + 1
  
    if debugging:
      print "  " * i,
      print "parent_key: %s, val:" % parent_key, parent

    # It's not actually a parent at this point, it's the leaf node.
    return parent


  # The verb form of "root" is "root", but that would be misleading in
  # this case; and the opposite of "uproot" is presumably "downroot",
  # but that wouldn't exactly clarify either.  Hence, "enroot" :-).
  def enroot_names(self, svn_path, svn_rev, names):
    """Record SVN_PATH at SVN_REV as the earliest point from which the
    symbolic names in NAMES could be copied.  SVN_PATH does not start
    with '/'."""

    # Guard against names == None
    if not names:
      return

    for name in names:
      components = [name] + string.split(svn_path, '/')
      last_component = components[-1]
      parent_key = self.root_key
      for component in components:
        parent = self.db[parent_key]
        if not parent.has_key(component):
          new_child_key = gen_key()
          parent[component] = new_child_key
          if component is last_component:
            self.db[new_child_key] = {self.file_key : 1,
                                      self.opening_revs_key : svn_rev}
          else:
            self.db[new_child_key] = {}
          self.db[parent_key] = parent
        # One way or another, parent now has an entry for component.
        this_entry_key = parent[component]
        this_entry_val = self.db[this_entry_key]
        # Swaparoo.
        parent_key = this_entry_key
        parent = this_entry_val


  def enroot_tags(self, svn_path, svn_rev, tags):
    """Record SVN_PATH at SVN_REV as the earliest point from which the
    symbolic names in TAGS could be copied.  SVN_PATH does not start
    with '/'."""
    for tag in tags:
      self.tags[tag] = None
    self.enroot_names(svn_path, svn_rev, tags)

  def enroot_branches(self, svn_path, svn_rev, branches):
    """Record SVN_PATH at SVN_REV as the earliest point from which the
    symbolic names in BRANCHES could be copied.  SVN_PATH does not
    start with '/'."""
    self.enroot_names(svn_path, svn_rev, branches)

  def close_names(self, svn_path, svn_rev, names):
    """Record that as of SVN_REV, SVN_PATH could no longer be the
    source from which any of symbolic names in NAMES could be copied.
    SVN_PATH does not start with '/'."""

    # Guard against names == None
    if not names:
      return

    for name in names:
      components = [name] + string.split(svn_path, '/')
      parent_key = self.root_key
      parent = self.db[parent_key]
      # If this symbolic name isn't even in the tracker anymore,
      # bail (RepositoryMirror may return closed names for tags and
      # branches that are already closed, and we should just ignore
      # them).
      if not parent.has_key(name):
        return
      last_component = components[-1]
      for component in components:
        # Check for a "can't happen".
        if not parent.has_key(component):
          sys.stderr.write("%s: in path '%s', value for parent key '%s' "
                           "does not have entry '%s'\n"
                           % (error_prefix, svn_path, parent_key, component))
          sys.exit(1)
        this_entry_key = parent[component]
        this_entry_val = self.db[this_entry_key]

        if component is last_component:
          this_entry_val[self.closing_revs_key] = svn_rev
          self.db[this_entry_key] = this_entry_val

        # Swaparoo.
        parent_key = this_entry_key
        parent = this_entry_val

  def close_tags(self, svn_path, svn_rev, tags):
    """Record that as of SVN_REV, SVN_PATH could no longer be the
    source from which any of TAGS could be copied.  SVN_PATH does not
    start with '/'."""
    self.close_names(svn_path, svn_rev, tags)

  def close_branches(self, svn_path, svn_rev, branches):
    """Record that as of SVN_REV, SVN_PATH could no longer be the
    source from which any of BRANCHES could be copied.  SVN_PATH does
    not start with '/'."""
    self.close_names(svn_path, svn_rev, branches)

  def score_revisions(self, openings, closings):
    """Return a list of revisions and scores based on OPENINGS and
    CLOSINGS.  The returned list looks like:

       [(REV1 SCORE1), (REV2 SCORE2), ...]

    where REV2 > REV1.  OPENINGS and CLOSINGS are the values of
    self.opening_revs_key and self.closing_revs_key, or
    self.opening_revs_key and self.closing_revs_key, from some file or
    directory node, or else None.

    Each score indicates that copying the corresponding revision (or any
    following revision up to the next revision in the list) of
    the object in question would yield that many correct paths at or
    underneath the object.  There may be other paths underneath it
    which are not correct and need to be deleted or recopied; those
    can only be detected by descending and examining their scores.

    If OPENINGS is false, return the empty list."""

    # First look for easy outs.
    if not openings:
      return []

    # Must be able to call len(closings) below.
    if closings is None:
      closings = []
      
    # No easy out, so wish for lexical closures and calculate the scores :-). 
    scores = []
    opening_score_accum = 0
    for i in range(len(openings)):
      opening_rev, opening_score = openings[i]
      opening_score_accum = opening_score_accum + opening_score
      scores.append((opening_rev, opening_score_accum))
    min = 0
    for i in range(len(closings)):
      closing_rev, closing_score = closings[i]
      done_exact_rev = None
      insert_index = None
      insert_score = None
      for j in range(min, len(scores)):
        score_rev, score = scores[j]
        if score_rev >= closing_rev:
          if not done_exact_rev:
            if score_rev > closing_rev:
              insert_index = j
              insert_score = scores[j-1][1] - closing_score
            done_exact_rev = 1
          scores[j] = (score_rev, score - closing_score)
        else:
          min = j + 1
      if not done_exact_rev:
        scores.append((closing_rev,scores[-1][1] - closing_score))
      if insert_index is not None:
        scores.insert(insert_index, (closing_rev, insert_score))
    return scores
  
  def best_rev(self, scores, prefer_rev, limit_rev):
    """Return the revision older than LIMIT_REV with the highest score
    from SCORES, a list returned by score_revisions(). When the maximum score
    is shared by multiple revisions, the oldest revision is selected, unless
    PREFER_REV is one of the possibilities, in which case, it is selected."""
    max_score = 0
    prefer_rev_score = -1
    rev = SVN_INVALID_REVNUM
    for pair in scores:
      if pair[1] > max_score and pair[0] < limit_rev:
        max_score = pair[1]
        rev = pair[0]
      if pair[0] <= prefer_rev:
        prefer_rev_score = pair[1]
    if prefer_rev_score == max_score:
      rev = prefer_rev
    return rev

  def is_best_rev(self, scores, rev, limit_rev):
    """Return true if REV has the highest score for revisions older than
    LIMIT_REV from SCORES, a list returned by score_revisions()."""
    return self.best_rev(scores, rev, limit_rev) == rev

  def jit_score_node(self, node):
    """Return two arrays: one of opening scores and one of closing
    scores.  To do this, we walk the tree to each leaf node, get the
    opening and closing score from there, and walk back up."""

    root_key = node
    tree = {} # Make a little node tree here in-mem
    self.copy_node_tree(self.db, tree, node)
    #self.print_node_tree(tree, root_key)

    openings = self.list_revnums_in_node_tree(tree, node, self.opening_revs_key)
    closings = self.list_revnums_in_node_tree(tree, node, self.closing_revs_key)
    return self.condense_scores(openings), self.condense_scores(closings)

  def condense_scores(self, scores):
    """Takes an array of revisions (scores), for example:

      [21, 18, 6, 49, 39, 24, 24, 24, 24, 24, 24, 24]

    and adds up every occurrence of each revision and returns a sorted
    array of tuples containing (svn_revnum, count):

      [(6, 1), (18, 1), (21, 1), (24, 7), (39, 1), (49, 1)]
    """
    s = {}
    for k in scores: # Add up the scores
      if s.has_key(k):
        s[k] = s[k] + 1
      else:
        s[k] = 1
    a = s.items()
    a.sort()
    return a

  def list_revnums_in_node_tree(self, tree, node, revnum_type_key):
    """Scan TREE and return a list of all the revision numbers (including
    duplicates) contained in REVNUM_TYPE_KEY values for all nodes under NODE,
    including the score for NODE itself."""
    revnums = []
    if tree[node].has_key(revnum_type_key) and \
        tree[node].has_key(self.file_key):
      # If here, then we're at a leaf: Fetch revnum and return
      revnums.append(tree[node][revnum_type_key])
      return revnums

    for key, value in tree[node].items():
      if key[0] == '/': #Skip flags
        continue
      revnums = revnums + \
          self.list_revnums_in_node_tree(tree, value, revnum_type_key)
    return revnums

  def print_node_tree(self, tree, root_node, indent_depth=0):
    """For debugging purposes.  Prints all nodes in TREE that are
    rooted at ROOT_NODE.  INDENT_DEPTH is merely for purposes of
    debugging with the print statement in this function."""
    #print "NNN:", " " * (indent_depth * 2), root_node, tree[root_node]
    for key, value in tree[root_node].items():
      if key[0] == '/': #Skip flags
        continue
      self.print_node_tree(tree, value, (indent_depth + 1))

  def copy_node_tree(self, src, dst, start):
    """Helper for jit_score_node.  Recursively copies START node and
    all nodes under it from SRC to DST."""
    dst[start] = src[start] 
    for k, v in dst[start].items():
      if k[0] == '/': #Skip flags
        continue
      self.copy_node_tree(src, dst, v)

  # Helper for copy_descend().
  def cleanup_entries(self, rev, limit_rev, entries, is_tag):
    """Return a copy of ENTRIES, minus the individual entries whose
    highest scoring revision doesn't match REV (and also, minus and
    special '/'-denoted flags).  IS_TAG is 1 or None, based on whether
    this work is being done for the sake of a tag or a branch."""
    new_entries = {}
    for key, entry in entries.items():
      if key[0] == '/': # Skip flags
        continue
      val = self.db[entry]

      opening_scores, closing_scores = self.jit_score_node(entry)
      scores = self.score_revisions(opening_scores, closing_scores)

      if self.is_best_rev(scores, rev, limit_rev):
        new_entries[key] = entry
    return new_entries
      
  # Helper for fill_branch().
  def copy_descend(self, dumper, ctx, name, parent, entry_name,
                   parent_rev, src_path, dst_path, is_tag, jit_new_rev=None):
    """Starting with ENTRY_NAME in directory object PARENT at
    PARENT_REV, use DUMPER and CTX to copy nodes in the Subversion
    repository, manufacturing the source paths with SRC_PATH and the
    destination paths with NAME and DST_PATH.

    If IS_TAG is true, NAME is treated as a tag, else as a branch.

    If JIT_NEW_REV is not None, it is a list of one or two elements.
    If the first element is true, then if any copies are to be made,
    invoke DUMPER.start_revision() before the first copy, then set
    JIT_NEW_REV[0] to None, so no more new revisions are made for this
    symbolic name anywhere in this descent.

    The second element, if present, is the string to be used for the svn:date
    property of any JIT-created revision.
    
    ('JIT' == 'Just In Time'.)"""
    ### Hmmm, is passing [1] instead of 1 an idiomatic way of passing
    ### a side-effectable boolean in Python?  That's how the
    ### JIT_NEW_REV parameter works here and elsewhere, but maybe
    ### there's a clearer way to do it?

    key = parent[entry_name]
    val = self.db[key]

    limit_rev = dumper.revision
    if jit_new_rev and jit_new_rev[0]:
      # Because in this case the current rev is complete,
      # so is a valid copyfrom source
      limit_rev = limit_rev + 1  

    if not val.has_key(self.copyfrom_rev_key):
      # If not already copied this subdir, calculate its "best rev"
      # and see if it differs from parent's best rev.
      opening_scores, closing_scores = self.jit_score_node(key)
      scores = self.score_revisions(opening_scores, closing_scores)
      rev = self.best_rev(scores, parent_rev, limit_rev)

      if rev == SVN_INVALID_REVNUM:
        sys.stderr.write(error_prefix +
            ": failed to find a revision to copy from when copying %s " \
            "'%s'\n" % (is_tag and "tag" or "branch", name))
        sys.exit(1)

      else:
        if is_tag:
          copy_dst = make_path(ctx, dst_path, None, name)
        else:
          copy_dst = make_path(ctx, dst_path, name, None)

        expected_entries = self.cleanup_entries(rev, limit_rev,
                                                val, is_tag)
        if (rev != parent_rev):
          if jit_new_rev and jit_new_rev[0]:
            dumper.start_revision(make_revision_props(ctx, name, is_tag,
              len(jit_new_rev) > 1 and jit_new_rev[1] or None))
            jit_new_rev[0] = None
          if dumper.copy_path(src_path, rev, copy_dst, expected_entries):
            parent_rev = rev
          else:
            # If we didn't copy, then we need to prune
            dumper.prune_entries(copy_dst, expected_entries)
        else:
          # Even if we kept the already-present revision of this entry
          # instead of copying a new one, we still need to prune out
          # anything that's not part of the symbolic name.
          dumper.prune_entries(copy_dst, expected_entries)

        # Record that this copy is done:
        val[self.copyfrom_rev_key] = parent_rev
        if val.has_key(self.opening_revs_key):
          del val[self.opening_revs_key]
        if val.has_key(self.closing_revs_key):
          del val[self.closing_revs_key]
        self.db[key] = val

    for ent in val.keys():
      if not ent[0] == '/':
        if src_path:
          next_src = src_path + '/' + ent
        else:
          next_src = ent
        if dst_path:
          next_dst = dst_path + '/' + ent
        else:
          next_dst = ent
        self.copy_descend(dumper, ctx, name, val, ent, parent_rev,
                          next_src, next_dst, is_tag, jit_new_rev)

  def fill_name(self, dumper, ctx, name, is_tag, jit_new_rev=None):
    """Use DUMPER to create all currently available parts of symbolic
    name NAME that have not been created already.

    If IS_TAG is true, NAME is treated as a tag, else as a branch.

    JIT_NEW_REV is as documented for the copy_descend() function.""" 

    # A source path looks like this in the symbolic name tree:
    #
    #    thisbranch/trunk/proj/foo/bar/baz.c
    #
    # ...or occasionally...
    #
    #    thisbranch/branches/sourcebranch/proj/foo/bar/baz.c
    #
    # (the latter when 'thisbranch' is branched off 'sourcebranch').
    #
    # Meanwhile, we're copying to a location in the repository like
    #
    #    /branches/thisbranch/proj/foo/bar/baz.c    or
    #    /tags/tagname/proj/foo/bar/baz.c
    #
    # Of course all this depends on make_path()'s behavior.  At
    # various times we've changed the way it produces paths (see
    # revisions 6028 and 6347).  If it changes again, the logic here
    # must be adjusted to match.

    parent_key = self.root_key
    parent = self.db[parent_key]

    # If there are no origin records, then we must've messed up earlier.
    if not parent.has_key(name):
      if is_tag:
        sys.stderr.write("%s: no origin records for tag '%s'.\n"
                         % (error_prefix, name))
      else:
        sys.stderr.write("%s: no origin records for branch '%s'.\n"
                         % (error_prefix, name))
      sys.exit(1)

    parent_key = parent[name]
    parent = self.db[parent_key]

    if is_tag and self.tags.has_key(name):
      print "filling tag '%s'." % name
    elif not is_tag and not self.tags.has_key(name):
      print "filling branch '%s'." % name
    else:
      return

    # All Subversion source paths under the branch start with one of
    # three things:
    #
    #   /trunk/...
    #   /branches/foo/...
    #   /tags/foo/...
    #
    # (We don't care what foo is, it's just a component to skip over.)
    #
    # Since these don't all have the same number of components, we
    # manually descend into each as far as necessary, then invoke
    # copy_descend() once we're in the right place in both trees.
    #
    # Since it's possible for a branch or tag to have some source
    # paths on trunk and some on branches, there's some question about
    # what to copy as the top-level directory of the branch.  Our
    # solution is to [somewhat randomly] give preference to trunk.
    # Note that none of these paths can ever conflict; for example,
    # it would be impossible to have both
    #
    #   thisbranch/trunk/myproj/lib/drivers.c                   and
    #   thisbranch/branches/sourcebranch/myproj/lib/drivers.c
    #
    # because that would imply that the symbolic name 'thisbranch'
    # appeared twice in the RCS file header, referring to two
    # different revisions.  Well, I suppose that's *possible*, but its
    # effect is undefined, and it's as reasonable for us to just
    # overwrite one with the other as anything else -- anyway, isn't
    # that what CVS would do if you checked out the branch?  <shrug>

    if parent.has_key(ctx.trunk_base):
      self.copy_descend(dumper, ctx, name, parent, ctx.trunk_base,
                        SVN_INVALID_REVNUM, ctx.trunk_base, "",
                        is_tag, jit_new_rev)
    if parent.has_key(ctx.branches_base):
      branch_base_key = parent[ctx.branches_base]
      branch_base = self.db[branch_base_key]
      for this_source in branch_base.keys():
        # We skip special names beginning with '/' for the usual
        # reason.  We skip cases where (this_source == name) for a
        # different reason: if a CVS branch were rooted in itself,
        # that would imply that the same symbolic name appeared on two
        # different branches in an RCS file, which CVS doesn't
        # permit.  So while it wouldn't hurt to descend, it would be a
        # waste of time.
        if (this_source[0] != '/') and (this_source != name):
          src_path = ctx.branches_base + '/' + this_source
          self.copy_descend(dumper, ctx, name, branch_base, this_source,
                            SVN_INVALID_REVNUM, src_path, "",
                            is_tag, jit_new_rev)

  def fill_tag(self, dumper, ctx, tag, jit_new_rev=None):
    """Use DUMPER to create all currently available parts of TAG that
    have not been created already.  Use CTX.trunk_base, CTX.tags_base, 
    and CTX.branches_base to determine the source and destination
    paths in the Subversion repository.

    JIT_NEW_REV is as documented for the copy_descend() function.""" 
    self.fill_name(dumper, ctx, tag, 1, jit_new_rev)

  def fill_branch(self, dumper, ctx, branch, jit_new_rev=None):
    """Use DUMPER to create all currently available parts of BRANCH that
    haven't been created already.  Use CTX.trunk_base, CTX.tags_base,  
    and CTX.branches_base to determine the source and destination
    paths in the Subversion repository.

    JIT_NEW_REV is as documented for the copy_descend() function.""" 
    self.fill_name(dumper, ctx, branch, None, jit_new_rev)

  def finish(self, dumper, ctx):
    """Use DUMPER to finish branches and tags that have either
    not been created yet, or have been only partially created.
    Use CTX.trunk_base, CTX.tags_base, and CTX.branches_base to
    determine the source and destination paths in the Subversion
    repository."""
    parent_key = self.root_key
    parent = self.db[parent_key]
    # Do all branches first, then all tags.  We don't bother to check
    # here whether a given name is a branch or a tag, or is done
    # already; the fill_foo() methods will just do nothing if there's
    # nothing to do.
    #
    # We do one revision per branch or tag, for clarity to users, not
    # for correctness.  In CVS, when you make a branch off a branch,
    # the new branch will just root itself in the roots of the old
    # branch *except* where the new branch sprouts from a revision
    # that was actually committed on the old branch.  In the former
    # cases, the source paths will be the same as the source paths
    # from which the old branch was created and therefore will already
    # exist; and in the latter case, the source paths will actually be
    # on the old branch, but those paths will exist already because
    # they were commits on that branch and therefore cvs2svn must have
    # created it already (see the fill_branch call in Commit.commit).
    # So either way, the source paths exist by the time we need them.
    #
    ### It wouldn't be so awfully hard to determine whether a name is
    ### just a branch or just a tag, which would allow for more
    ### intuitive messages below.
    if not ctx.trunk_only:
      print "Finishing branches:"
      for name in parent.keys():
        if name[0] != '/':
          self.fill_branch(dumper, ctx, name, [1])
      print "Finishing tags:"
      for name in parent.keys():
        if name[0] != '/':
          self.fill_tag(dumper, ctx, name, [1])

      if 0: # Left in temporarily for debugging
        print "Cleaning up:"
        for name in parent.keys():
          if name[0] != '/':
            print "Deleting", name
            self.cleanup_symbol(name)

  def cleanup_symbol(self, name):
    """Remove the entire node tree rooted at /NAME from the
    SymbolicNameTracker."""
    root_dir = self.db[self.root_key]
    self.del_node(root_dir[name])       # Recursively delete the node-tree
    del root_dir[name]                  # Delete NAME from the root_dir
    self.db[self.root_key] = root_dir   # Save the new root_dir

  def del_node(self, node):
    """Recursively delete NODE and all its children from the db."""
    for key, subnode in self.db[node].items():
      if key[0] == '/': # Skip flags
        continue
      self.del_node(subnode)
    del self.db[node]

  # Left in temporarily for debugging purposes
  #def __del__(self):
  #  print "=" * 75
  #  for key in self.db.db.keys():
  #    print key, self.db[key]

### TODO add digest to constructor, then use it in __cmp__
class Commit:
  def __init__(self, author, log):
    self.author = author
    self.log = log

    self.files = { }
    # Lists of CVSRevisions
    self.changes = [ ]
    self.deletes = [ ]

    # Start out with a t_min higher than any incoming time T, and a
    # t_max lower than any incoming T.  This way the first T will
    # push t_min down to T, and t_max up to T, naturally (without any
    # special-casing), and successive times will then ratchet them
    # outward as appropriate.
    self.t_min = 1L<<32
    self.t_max = 0

  ###TODO: Tertiary sort by digest
  def __cmp__(self, other):
    # Commits should be sorted by t_max.  If both self and other have
    # the same t_max, break the tie using t_min.
    return cmp(self.t_max, other.t_max) or cmp(self.t_min, other.t_min)

  def has_file(self, fname):
    return self.files.has_key(fname)

  def revisions(self):
    return self.changes + self.deletes

  def contains_symbolic_name(self, name):
    """Returns true if any CVSRevision in this commit is on a tag or a
    branch or is the origin of a tag or branch."""
    for c_rev in self.revisions():
      if c_rev.contains_symbolic_name(name):
        return 1
    return 0

  def add(self, c_rev):
    # Record the time range of this commit.
    #
    # ### ISSUE: It's possible, though unlikely, that the time range
    # of a commit could get gradually expanded to be arbitrarily
    # longer than COMMIT_THRESHOLD.  I'm not sure this is a huge
    # problem, and anyway deciding where to break it up would be a
    # judgement call. For now, we just print a warning in commit() if
    # this happens.
    if c_rev.timestamp < self.t_min:
      self.t_min = c_rev.timestamp
    if c_rev.timestamp > self.t_max:
      self.t_max = c_rev.timestamp

    if c_rev.op == OP_DELETE:
      self.deletes.append(c_rev)
    else:
      # OP_CHANGE or OP_ADD
      self.changes.append(c_rev)
    self.files[c_rev.fname] = 1

  def commit(self, dumper, ctx, sym_tracker):
    # commit this transaction
    seconds = self.t_max - self.t_min
    print 'committing: %s, over %d seconds' % (time.ctime(self.t_min), seconds)
    if seconds > COMMIT_THRESHOLD:
      print '%s: commit spans more than %d seconds' \
            % (warning_prefix, COMMIT_THRESHOLD)

    do_copies = [ ]

    # State for handling default branches.
    # 
    # Here is a tempting, but ultimately nugatory, bit of logic, which
    # I share with you so you may appreciate the less attractive, but
    # refreshingly non-nugatory, logic which follows it:
    #
    # If some of the commits in this txn happened on a non-trunk
    # default branch, then those files will have to be copied into
    # trunk manually after being changed on the branch (because the
    # RCS "default branch" appears as head, i.e., trunk, in practice).
    # As long as those copies don't overwrite any trunk paths that
    # were also changed in this commit, then we can do the copies in
    # the same revision, because they won't cover changes that don't
    # appear anywhere/anywhen else.  However, if some of the trunk dst
    # paths *did* change in this commit, then immediately copying the
    # branch changes would lose those trunk mods forever.  So in this
    # case, we need to do at least that copy in its own revision.  And
    # for simplicity's sake, if we're creating the new revision for
    # even one file, then we just do all such copies together in the
    # new revision.
    #
    # Doesn't that sound nice?
    #
    # Unfortunately, Subversion doesn't support copies with sources
    # in the current txn.  All copies must be based in committed
    # revisions.  Therefore, we generate the above-described new
    # revision unconditionally.
    #
    # Each of these is a list of tuples.  Each tuple is of the form:
    #
    #   (cvs_path, branch_name, tags_rooted_here, branches_rooted_here)
    #
    # and a tuple is created for each default branch commit that will
    # need to be copied to trunk (or deleted from trunk) in the
    # generated revision following the "regular" revision.
    default_branch_copies  = [ ]
    default_branch_deletes = [ ]

    # we already have the date, so just format it
    date = format_date(self.t_max)
    try: 
      ### FIXME: The 'replace' behavior should be an option, like
      ### --encoding is.
      unicode_author = unicode(self.author, ctx.encoding, 'replace')
      unicode_log = unicode(self.log, ctx.encoding, 'replace')
      props = { 'svn:author' : unicode_author.encode('utf8'),
                'svn:log' : unicode_log.encode('utf8'),
                'svn:date' : date }
    except UnicodeError:
      print '%s: problem encoding author or log message:' % warning_prefix
      print "  author: '%s'" % self.author
      print "  log:    '%s'" % self.log
      print "  date:   '%s'" % date
      for c_rev in self.changes:
        print "    rev %s of '%s'" % (c_rev.rev, c_rev.fname)
      print "Consider rerunning with (for example) '--encoding=latin1'."
      # Just fall back to the original data.
      props = { 'svn:author' : self.author,
                'svn:log' : self.log,
                'svn:date' : date }
      

    # Tells whether we actually wrote anything to the dumpfile.
    svn_rev = SVN_INVALID_REVNUM

    # If any of the changes we are about to do are on branches, we need to
    # check and maybe fill them (in their own revisions) *before* we start
    # then data revision. So we have to iterate over changes and deletes twice.
    for c_rev in self.changes:
      if c_rev.branch_name:
        ### FIXME: Here is an obvious optimization point.  Probably
        ### dump.probe_path(PATH) is kind of slow, because it does N
        ### database lookups for the N components in PATH.  If this
        ### turns out to be a performance bottleneck, we can just
        ### maintain a database mirroring just the head tree, but
        ### keyed on full paths, to reduce the check to a quick
        ### constant time query.
        if not dumper.probe_path(c_rev.svn_path):
          sym_tracker.fill_branch(dumper, ctx, c_rev.branch_name, [1, date])

    for c_rev in self.deletes:
      # compute a repository path, dropping the ,v from the file name
      if c_rev.branch_name:
        ### FIXME: Here is an obvious optimization point.  Probably
        ### dump.probe_path(PATH) is kind of slow, because it does N
        ### database lookups for the N components in PATH.  If this
        ### turns out to be a performance bottleneck, we can just
        ### maintain a database mirroring just the head tree, but
        ### keyed on full paths, to reduce the check to a quick
        ### constant time query.
        if not dumper.probe_path(c_rev.svn_path):
          sym_tracker.fill_branch(dumper, ctx, c_rev.branch_name, [1, date])

    # Now that any branches we need exist, we can do the commits.
    for c_rev in self.changes:
      if svn_rev == SVN_INVALID_REVNUM:
        svn_rev = dumper.start_revision(props)
      sym_tracker.enroot_tags(c_rev.svn_path, svn_rev, c_rev.tags)
      sym_tracker.enroot_branches(c_rev.svn_path, svn_rev, c_rev.branches)
      print "    adding or changing %s : '%s'" % (c_rev.rev, c_rev.svn_path)

      # Only make a change if we need to.  When 1.1.1.1 has an empty
      # deltatext, the explanation is almost always that we're looking
      # at an imported file whose 1.1 and 1.1.1.1 are identical.  On
      # such imports, CVS creates an RCS file where 1.1 has the
      # content, and 1.1.1.1 has an empty deltatext, i.e, the same
      # content as 1.1.  There's no reason to reflect this non-change
      # in the repository, so we want to do nothing in this case.  (If
      # we were really paranoid, we could make sure 1.1's log message
      # is the CVS-generated "Initial revision\n", but I think the
      # conditions below are strict enough.)
      if not ((c_rev.deltatext_code == DELTATEXT_EMPTY)
              and (c_rev.rev == "1.1.1.1")
              and dumper.probe_path(c_rev.svn_path)):
        closed_tags, closed_branches = \
                     dumper.add_or_change_path(ctx, c_rev)
        if c_rev.is_default_branch_revision():
          default_branch_copies.append(c_rev)
        sym_tracker.close_tags(c_rev.svn_path, svn_rev, closed_tags)
        sym_tracker.close_branches(c_rev.svn_path, svn_rev,
                                   closed_branches)

    for c_rev in self.deletes:
      # compute a repository path, dropping the ,v from the file name
      print "    deleting %s : '%s'" % (c_rev.rev, c_rev.svn_path)
      if svn_rev == SVN_INVALID_REVNUM:
        svn_rev = dumper.start_revision(props)
      # Can this even happen on a deleted path? Yes, it can, e.g., a
      # dead branchpoint revision. In which case, we need to enroot to
      # avoid a "No origin records" error. And tags? Need to enroot so
      # that a tag set only on deleted revisions is still created in 
      # the subversion repository.
      sym_tracker.enroot_tags(c_rev.svn_path, svn_rev, c_rev.tags)
      sym_tracker.enroot_branches(c_rev.svn_path, svn_rev, c_rev.branches)
      ###TODO We generate a new revision unconditionally, even if this
      ### is a deletion of an already-deleted path, so we have a place
      ### to record the log message.  But, we should probably prefix
      ### that log message with some explanation, otherwise it can
      ### look mighty confusing to have a Subversion revision with no
      ### changed paths and a log message that may or may not make
      ### clear what happened.  The prependum to the log could say
      ### something like:
      ###
      ###   "[This revision represents the redeletion of
      ###     already-deleted files X, Y, and Z.  Therefore no paths
      ###     were changed in this revision, but...]"
      ###
      ### etc, etc.
      path_deleted, closed_tags, closed_branches = \
                    dumper.delete_path(ctx, c_rev, c_rev.svn_path)
      if c_rev.is_default_branch_revision():
        default_branch_deletes.append(c_rev)
      sym_tracker.close_tags(c_rev.svn_path, svn_rev, closed_tags)
      sym_tracker.close_branches(c_rev.svn_path, svn_rev, closed_branches)

    if svn_rev == SVN_INVALID_REVNUM:
      print '    no new revision created, as nothing to do'
    else:
      print '    new revision:', svn_rev
      if default_branch_copies or default_branch_deletes:
        previous_rev = svn_rev
        msg = 'This commit was generated by cvs2svn to compensate for '     \
              'changes in r%d,\n'                                           \
              'which included commits to RCS files with non-trunk default ' \
              'branches.\n' % previous_rev
        props = { 'svn:author' : 'cvs2svn',
                  'svn:log' : msg,
                  'svn:date' : date }
        svn_rev = dumper.start_revision(props)

        for c_rev in default_branch_copies:
          if (dumper.probe_path(c_rev.svn_trunk_path)):
            ign, closed_tags, closed_branches = \
                 dumper.delete_path(ctx, c_rev, c_rev.svn_trunk_path)
            sym_tracker.close_tags(c_rev.svn_trunk_path,
                                   svn_rev, closed_tags)
            sym_tracker.close_branches(c_rev.svn_trunk_path,
                                       svn_rev, closed_branches)
          dumper.copy_path(c_rev.svn_path, previous_rev,
                           c_rev.svn_trunk_path)

        for c_rev in default_branch_deletes:
          # Ignore the branch -- we don't need to know the default
          # branch, we already know we're deleting this from trunk.
          if (dumper.probe_path(c_rev.svn_trunk_path)):
            ign, closed_tags, closed_branches = \
                 dumper.delete_path(ctx, c_rev, c_rev.svn_trunk_path)
            sym_tracker.close_tags(c_rev.svn_trunk_path, svn_rev,
                                   closed_tags)
            sym_tracker.close_branches(c_rev.svn_trunk_path,
                                       svn_rev, closed_branches)


def read_resync(fname):
  "Read the .resync file into memory."

  ### note that we assume that we can hold the entire resync file in
  ### memory. really large repositories with whacky timestamps could
  ### bust this assumption. should that ever happen, then it is possible
  ### to split the resync file into pieces and make multiple passes,
  ### using each piece.

  #
  # A digest maps to a sequence of lists which specify a lower and upper
  # time bound for matching up the commit. We keep a sequence of these
  # because a number of checkins with the same log message (e.g. an empty
  # log message) could need to be remapped. We also make them a list because
  # we will dynamically expand the lower/upper bound as we find commits
  # that fall into a particular msg and time range.
  #
  # resync == digest -> [ [old_time_lower, old_time_upper, new_time], ... ]
  #
  resync = { }

  for line in fileinput.FileInput(fname):
    t1 = int(line[:8], 16)
    digest = line[9:DIGEST_END_IDX]
    t2 = int(line[DIGEST_END_IDX+1:], 16)
    t1_l = t1 - COMMIT_THRESHOLD/2
    t1_u = t1 + COMMIT_THRESHOLD/2
    if resync.has_key(digest):
      resync[digest].append([t1_l, t1_u, t2])
    else:
      resync[digest] = [ [t1_l, t1_u, t2] ]

  # For each digest, sort the resync items in it in increasing order,
  # based on the lower time bound.
  digests = resync.keys()
  for digest in digests:
    (resync[digest]).sort()

  return resync


class SymbolingsLogger(Singleton):
  """Manage the file that contains lines for symbol openings and
  closings.

  Determine valid SVNRevision ranges from which a file can be copied
  when creating a branch or tag in Subversion.  Do this by finding
  "Openings" and "Closings" for each file copied onto a branch or tag.

  An "Opening" is the CVSRevision from which a given branch/tag
  sprouts on a path.

  The "Closing" for that branch/tag and path is the next CVSRevision
  on the same line of development as the opening.

  For example, on file 'foo.c', branch BEE has branch number 1.2.2 and
  obviously sprouts from revision 1.2.  Therefore, 1.2 is the opening
  for BEE on path 'foo.c', and 1.3 is the closing for BEE on path
  'foo.c'.  Note that there may be many revisions chronologically
  between 1.2 and 1.3, for example, revisions on branches of 'foo.c',
  perhaps even including on branch BEE itself.  But 1.3 is the next
  revision *on the same line* as 1.2, that is why it is the closing
  revision for those symbolic names of which 1.2 is the opening.

  The reason for doing all this hullabaloo is to make branch and tag
  creation as efficient as possible by minimizing the number of copies
  and deletes per creation.  For example, revisions 1.2 and 1.3 of
  foo.c might correspond to revisions 17 and 30 in Subversion.  That
  means that when creating branch BEE, there is some motivation to do
  the copy from one of 17-30.  Now if there were another file,
  'bar.c', whose opening and closing CVSRevisions for BEE corresponded
  to revisions 24 and 39 in Subversion, we would know that the ideal
  thing would be to copy the branch from somewhere between 24 and 29,
  inclusive.
  """
  def init(self, ctx):
    self._ctx = ctx
    self.symbolings = open(SYMBOL_OPENINGS_CLOSINGS, 'a')
    Cleanup().register(SYMBOL_OPENINGS_CLOSINGS, pass8) ###TODO cleanup earlier?
    self.closings = open(SYMBOL_CLOSINGS_TMP, 'w')
    Cleanup().register(SYMBOL_CLOSINGS_TMP, pass5)

  def log_names_for_rev(self, names, c_rev, svn_revnum):
    """Write out SYMBOLIC_NAME SVN_REVNUM CVS_REV.SVN_PATH.  This
    allows us to run the logfile through sort and have all openings
    and closings for a given symbolic name grouped by symbolic name in
    svn commit order."""

    for name in names:
      name = _clean_symbolic_name(name)
      self._log(name, svn_revnum, c_rev.svn_path, OPENING)

      # If our c_rev has a next_rev, then that's the closing rev for
      # this source revision.  Log it to closings for later processing
      # since we don't know the svn_revnum yet.
      if c_rev.next_rev is not None:
        self.closings.write('%s %s\n' %
                            (name, c_rev.unique_key(c_rev.next_rev))) 

  def check_revision(self, c_rev, svn_revnum):
    """Examine a CVS Revision to see if it either opens a symbolic name."""
    ## TODO check symbolic_names

    # We log this revision if:
    # - There is branch/tag OPENING activity in this c_rev
    #   AND
    # - This c_rev is not 'dead'... that is, it's not a file added on a
    # - branch.
    if (((len(c_rev.tags) > 0) or (len(c_rev.branches) > 0))
        and not (c_rev.op == OP_DELETE)): 
      self.log_names_for_rev(c_rev.symbolic_names(), c_rev, svn_revnum)

  def _log(self, name, svn_revnum, svn_path, type):
    """Write out a single line to the symbol_openings_closings file
    representing that svn_revnum of svn_path is either the opening or
    closing (TYPE) of NAME (a symbolic name).

    TYPE should only be one of the following global constants:
    OPENING or CLOSING."""
    ###TODO 8 places gives us 999,999,999 SVN revs.  That *should* be enough.
    self.symbolings.write('%s %.8d %s %s\n' % (name, svn_revnum,
                                               type, svn_path))
      
  def close(self):
    # Iterate through the closings file, lookup the svn_revnum for
    # each closing CVSRevision, and write a proper line out to the
    # symbolings file.

    # Use this to get the c_rev.svn_path of our rev_key
    cvs_revs_db = CVSRevisionDatabase('r', self._ctx)

    self.closings.close()
    for line in fileinput.FileInput(SYMBOL_CLOSINGS_TMP):
      (name, rev_key) = line.rstrip().split(" ", 1)
      svn_revnum = CommitMapper().get_svn_revnum(rev_key)

      print "'" + rev_key + "'"
      c_rev = cvs_revs_db.get_revision(rev_key)
      print c_rev
      self._log(name, svn_revnum, c_rev.svn_path, CLOSING)

    self.symbolings.close()
    self.symbolings = open(SYMBOL_OPENINGS_CLOSINGS, 'a')


class LastSymbolicNameDatabase(Database):
  """ Passing every CVSRevision in s-revs to this class will result in
  a Database whose key is the last CVS Revision a symbolicname was
  seen in, and whose value is a list of all symbolicnames that were
  last seen in that revision."""
  def __init__(self, mode):
    self.symbols = {}
    self.symbol_revs_db = Database(SYMBOL_LAST_CVS_REVS_DB, mode)
    Cleanup().register(SYMBOL_LAST_CVS_REVS_DB, pass8)

  # Once we've gone through all the revs,
  # symbols.keys() will be a list of all tags and branches, and
  # their corresponding values will be a key into the last CVS revision
  # that they were used in.
  def log_revision(self, c_rev):
    # Gather last CVS Revision for symbolic name info and tag info
    for tag in c_rev.tags:
      self.symbols[tag] = c_rev.unique_key()
    for branch in c_rev.branches:
      self.symbols[branch] = c_rev.unique_key()

  # Creates an inversion of symbols above--a dictionary of lists (key
  # = CVS rev unique_key: val = list of symbols that close in that
  # rev.
  def create_database(self):
    for sym, rev_unique_key in self.symbols.items():
      if self.symbol_revs_db.has_key(rev_unique_key):
        ary = self.symbol_revs_db[rev_unique_key]
        ary.append(sym)
        self.symbol_revs_db[rev_unique_key] = ary
      else:
        self.symbol_revs_db[rev_unique_key] = [sym]


### TODO do we really need to map this?  We may have all the
### information that we need already.  We need this for the
### SymbolingsLogger, but we could map to an offset into s-revs,
### instead of dup'ing the s-revs data in this database.
class CVSRevisionDatabase:
  """ Passing every CVSRevision in s-revs to this class will result in
  a Database mapping CVSRevision.unique_key() to the actual s-rev
  string for the CVS Revision."""

  def __init__(self, mode, ctx=None):
    """Initialize an instance, opening database in MODE (like the MODE
    argument to Database or anydbm.open()).  CTX is required if you
    wish to call the get_revision() method."""
    self._ctx = ctx
    ###TODO Properly subclass Database
    self.cvs_revs_db = Database(CVS_REVS_DB, mode)
    Cleanup().register(CVS_REVS_DB, pass8)

  def log_revision(self, c_rev):
    # Add c_rev to the cvs_rev index db
    str = StringWriter()
    c_rev.write_revs_line(str)
    self.cvs_revs_db[c_rev.unique_key()] = str.stringValue()

  def get_revision(self, unique_key):
    """Return the CVSRevision stored under UNIQUE_KEY. Return None if
    unique_key isn't in our datastore."""
    val = self.cvs_revs_db[unique_key]
    if val is None:
      return None
    return CVSRevision(self._ctx, val)


class TagsDatabase(Database):
  """ Passing every CVSRevision in s-revs to this class will result in
  a Database that contains all Symbolic Names that are tags.  The key
  is the tag name, the value is None."""

  def __init__(self, mode):
    Database.__init__(self, TAGS_DB, mode)
    Cleanup().register(TAGS_DB, pass8)
  
  def log_revision(self, c_rev):
    for tag in c_rev.tags:
      self.db[tag] = ""


def sort_file(infile, outfile):
  # sort the log files

  # GNU sort will sort our dates differently (incorrectly!) if our
  # LC_ALL is anything but 'C', so if LC_ALL is set, temporarily set
  # it to 'C'
  if os.environ.has_key('LC_ALL'):
    lc_all_tmp = os.environ['LC_ALL']
  else:
    lc_all_tmp = None
  os.environ['LC_ALL'] = 'C'
  run_command('sort %s > %s' % (infile, outfile))
  if lc_all_tmp is None:
    del os.environ['LC_ALL']
  else:
    os.environ['LC_ALL'] = lc_all_tmp

def print_node_tree(tree, root_node, indent_depth=0):
  """For debugging purposes.  Prints all nodes in TREE that are
  rooted at ROOT_NODE.  INDENT_DEPTH is merely for purposes of
  debugging with the print statement in this function."""
  if not indent_depth:
    print "TREE", "=" * 75
  print "TREE:", " " * (indent_depth * 2), root_node, tree[root_node]
  for key, value in tree[root_node].items():
    if key[0] == '/': #Skip flags ###TODO Is this necessary?
      continue
    print_node_tree(tree, value, (indent_depth + 1))

def pass1(ctx):
  ###TODO create the CollectData object in visit_file and pass a
  ###different variable along via os.path.walk for accumulating
  ###errors.
  cd = CollectData(DATAFILE, ctx)
  p = rcsparse.Parser()
  stats = [ 0 ]
  os.path.walk(ctx.cvsroot, visit_file, (cd, p, stats))
  if ctx.verbose:
    print 'processed', stats[0], 'files'
  if len(cd.fatal_errors) > 0:
    sys.exit("Pass 1 complete.\n" + "=" * 75 + "\n"
             + "Error summary:\n"
             + "\n".join(cd.fatal_errors)
             + "\nExited due to fatal error(s).")
 
def pass2(ctx):
  "Pass 2: clean up the revision information."

  # We may have recorded some changes in revisions' timestamp. We need to
  # scan for any other files which may have had the same log message and
  # occurred at "the same time" and change their timestamps, too.

  # read the resync data file
  resync = read_resync(ctx.log_fname_base + RESYNC_SUFFIX)

  output = open(ctx.log_fname_base + CLEAN_REVS_SUFFIX, 'w')
  Cleanup().register(ctx.log_fname_base + CLEAN_REVS_SUFFIX, pass3)

  # process the revisions file, looking for items to clean up
  for line in fileinput.FileInput(ctx.log_fname_base + REVS_SUFFIX):
    c_rev = CVSRevision(ctx, line)
    if not resync.has_key(c_rev.digest):
      output.write(line)
      continue

    # we have a hit. see if this is "near" any of the resync records we
    # have recorded for this digest [of the log message].
    for record in resync[c_rev.digest]:
      if record[0] <= c_rev.timestamp <= record[1]:
        # bingo! remap the time on this (record[2] is the new time).
        print "RESYNC: '%s' (%s) : old time='%s' new time='%s'" \
              % (relative_name(ctx.cvsroot, c_rev.fname),
                 c_rev.rev, time.ctime(c_rev.timestamp), time.ctime(record[2]))

        # adjust the time range. we want the COMMIT_THRESHOLD from the
        # bounds of the earlier/latest commit in this group.
        record[0] = min(record[0], c_rev.timestamp - COMMIT_THRESHOLD/2)
        record[1] = max(record[1], c_rev.timestamp + COMMIT_THRESHOLD/2)

        c_rev.timestamp = record[2]
        c_rev.write_revs_line(output)

        # stop looking for hits
        break
    else:
      # the file/rev did not need to have its time changed.
      output.write(line)


def pass3(ctx):
  sort_file(ctx.log_fname_base + CLEAN_REVS_SUFFIX,
            ctx.log_fname_base + SORTED_REVS_SUFFIX)
  ### TODO pass8 is too late for this, but we may need it again after pass5
  Cleanup().register(ctx.log_fname_base + SORTED_REVS_SUFFIX, pass8)

def pass4(ctx):
  """Iterate through sorted revs and generate:
  1. The LastSymbolicNameDatabase, which contains the last CVSRevision
     that is a source for each tag or branch.

  2. A Database that maps CVSRevision.unique_key()s to CVSRevision
     s-rev type strings.

  3. A Database that contains all symbolic names that are tags.
  """
  last_sym_name_db = LastSymbolicNameDatabase('n')
  cvs_rev_db = CVSRevisionDatabase('n', ctx)
  tags_db = TagsDatabase('n')

  for line in fileinput.FileInput(ctx.log_fname_base + SORTED_REVS_SUFFIX):
    c_rev = CVSRevision(ctx, line)

    last_sym_name_db.log_revision(c_rev)
    cvs_rev_db.log_revision(c_rev)
    tags_db.log_revision(c_rev)

  last_sym_name_db.create_database()

def pass5(ctx):
  """
  Generate the SVNCommit <-> CVSRevision mapping
  databases. CVSCommit._commit also calls SymbolingsLogger to register
  CVSRevisions that represent an opening or closing for a path on a
  branch or tag.  See SymbolingsLogger for more details.
  """
  ###TODO we need to make sure that this file is deleted before
  ###starting this pass.  Find a better way. :)
  if os.path.isfile(SYMBOL_OPENINGS_CLOSINGS):
    os.unlink(SYMBOL_OPENINGS_CLOSINGS)
  aggregator = CVSRevisionAggregator(ctx)

  ###TODO Can we move this to pass4?
  for line in fileinput.FileInput(ctx.log_fname_base + SORTED_REVS_SUFFIX):
    c_rev = CVSRevision(ctx, line)
    aggregator.process_revision(c_rev)
  aggregator.flush()
  SymbolingsLogger(ctx).close()


def pass6(ctx):
  sort_file(SYMBOL_OPENINGS_CLOSINGS, SYMBOL_OPENINGS_CLOSINGS_SORTED)
  Cleanup().register(SYMBOL_OPENINGS_CLOSINGS_SORTED, pass8)
  pass

class SymbolingsReaderEmptyFillError(Exception):
  """Exception raised if we encounter an attempted fill of a symbolic
  name that contains no openings or closings (and consequently,
  nothing would be filled)."""
  pass

class SymbolingsReader:
  """Provides an interface to the SYMBOL_OPENINGS_CLOSINGS_SORTED file
  and the SYMBOL_OFFSETS_DB.  Does the heavy lifting of finding and
  returning the correct opening and closing Subversion revision
  numbers for a given symbolic name."""
  def __init__(self, ctx):
    """Opens the SYMBOL_OPENINGS_CLOSINGS_SORTED for reading, and
    reads the offsets database into memory."""
    self._ctx = ctx
    self.symbolings = open(SYMBOL_OPENINGS_CLOSINGS_SORTED, 'r')
    # The offsets_db is really small, and we need to read and write
    # from it a fair bit, so suck it into memory
    offsets_db = Database(SYMBOL_OFFSETS_DB, 'r') 
    self.offsets = { }
    for key in offsets_db.db.keys():
      #print " ZOO:", key, offsets_db[key]
      self.offsets[key] = offsets_db[key]

  def filling_guide_for_symbol(self, symbolic_name, svn_revnum):
    """Given SYMBOLIC_NAME and SVN_REVNUM, return a new
    SymbolicNameFillingGuide object.

    Note that if we encounter an opening rev in this fill, but the
    corresponding closing rev takes place later than SVN_REVNUM, the
    closing will not be passed to SymbolicNameFillingGuide in this
    fill (and will be discarded when encountered in a later fill).
    This is perfectly fine, because we can still do a valid fill
    without the closing--we always try to fill what we can as soon as
    we can."""
    # It's possible to have a branch start with a file that was added
    # on a branch
    if not self.offsets.has_key(symbolic_name):
      return SymbolicNameFillingGuide(self._ctx, symbolic_name)
    # set our read offset for self.symbolings to the offset for
    # symbolic_name
    self.symbolings.seek(self.offsets[symbolic_name])

    symbol_fill = SymbolicNameFillingGuide(self._ctx, symbolic_name)
    while (1):
      line = self.symbolings.readline().rstrip()
      if not line:
        break
      name, revnum, type, svn_path = line.split(" ", 3)
      revnum = int(revnum)
      if (revnum >= svn_revnum
          or name != symbolic_name):
        break

      symbol_fill.register(svn_path, revnum, type)

    # get current offset of the read marker and set it to the offset
    # for the beginning of the line we just read if we used anything
    # we read.
    if not symbol_fill.is_empty():
      # Subtract one cause we rstripped the CR above.
      self.offsets[symbolic_name] = self.symbolings.tell() - len(line) - 1
    else:
      raise SymbolingsReaderEmptyFillError, \
            ("Read no valid openings or closings for '%s' at svn_revnum '%d'"
                                             % (symbolic_name, svn_revnum))
                               
    symbol_fill.make_node_tree()
    return symbol_fill
  

class SymbolicNameFillingGuide:
  """A SymbolicNameFillingGuide is essentially a node tree
  representing the source paths to be copied to fill
  self.symbolic_name in the current SVNCommit.

  After calling self.register() on a series of openings and closings,
  call self.make_node_tree() to prepare self.node_tree for
  examination.  See the docstring for self.make_node_tree() for
  details on the structure of self.node_tree.

  By walking self.node_tree and calling self.get_best_revnum() on each
  node, the caller can determine what subversion revision number to
  copy the path corresponding to that node from.  self.node_tree
  should be treated as read-only.

  The caller can then descend to sub-nodes to see if their "best
  revnum" differs from their parents' and if it does, take appropriate
  actions to "patch up" the subtrees."""
  def __init__(self, ctx, symbolic_name):
    """Initializes a SymbolicNameFillingGuide for SYMBOLIC_NAME and
    prepares it for receiving openings and closings.

    Returns a fully functional and armed SymbolicNameFillingGuide
    object."""
    self._ctx = ctx
    self.name = symbolic_name

    self.opening_key = "/open" # TODO make 2 bytes
    self.closing_key = "/close" # TODO make 2 bytes

    # A dictionary of SVN_PATHS and SVN_REVNUMS whose format is:
    #
    # { svn_path : { self.opening_key : svn_revnum,
    #                self.closing_key : svn_revnum }
    #                ...}
    self.things = { }

    # The key for the root node of the node tree
    self.root_key = '0'
    # The dictionary that holds our node tree, seeded with the root key.
    self.node_tree = { self.root_key : { } }

  def get_best_revnum(self, node, preferred_revnum):
    """ Determine the best subversion revision number to use when
    copying the source tree beginning at NODE. Returns a
    subversion revision number.

    PREFERRED_REVNUM is passed to self._best_rev and used to
    calculate the best_revnum."""
    revnum = SVN_INVALID_REVNUM

    # Aggregate openings and closings from the rev tree
    openings = self._list_revnums_for_key(node, self.opening_key)
    closings = self._list_revnums_for_key(node, self.closing_key)

    # Score the lists
    scores = self._score_revisions(self._condense_scores(openings),
                                  self._condense_scores(closings))

    revnum = self._best_rev(scores, preferred_revnum)
  
    if revnum == SVN_INVALID_REVNUM:
      sys.stderr.write(error_prefix + ": failed to find a revision "
                       + "to copy from when copying %s\n" % name)
      sys.exit(1)
    return revnum


  def _best_rev(self, scores, preferred_rev):
    """Return the revision with the highest score from SCORES, a list
    returned by _score_revisions(). When the maximum score is shared
    by multiple revisions, the oldest revision is selected, unless
    PREFERRED_REV is one of the possibilities, in which case, it is
    selected."""
    max_score = 0
    max_rev = 1
    preferred_rev_score = -1
    rev = SVN_INVALID_REVNUM
    for revnum, count in scores:
      if revnum > max_rev:
        max_rev = revnum
      if count > max_score:
        max_score = count
        rev = revnum
      if revnum <= preferred_rev:
        preferred_rev_score = count
    if (preferred_rev_score == max_score
        and preferred_rev <= max_rev):
      rev = preferred_rev
    return rev


  def _score_revisions(self, openings, closings):
    """Return a list of revisions and scores based on OPENINGS and
    CLOSINGS.  The returned list looks like:

       [(REV1 SCORE1), (REV2 SCORE2), ...]

    where REV2 > REV1.  OPENINGS and CLOSINGS are the values of
    self.opening__key and self.closing_key from some file or
    directory node, or else None.

    Each score indicates that copying the corresponding revision (or
    any following revision up to the next revision in the list) of the
    object in question would yield that many correct paths at or
    underneath the object.  There may be other paths underneath it
    which are not correct and would need to be deleted or recopied;
    those can only be detected by descending and examining their
    scores.

    If OPENINGS is false, return the empty list."""
    # First look for easy outs.
    if not openings:
      return []

    # Must be able to call len(closings) below.
    if closings is None:
      closings = []

    ###TODO, Review with kfogel for simplification
    # No easy out, so wish for lexical closures and calculate the scores :-). 
    scores = []
    opening_score_accum = 0
    for i in range(len(openings)):
      opening_rev, opening_score = openings[i]
      opening_score_accum = opening_score_accum + opening_score
      scores.append((opening_rev, opening_score_accum))
    min = 0
    for i in range(len(closings)):
      closing_rev, closing_score = closings[i]
      done_exact_rev = None
      insert_index = None
      insert_score = None
      for j in range(min, len(scores)):
        score_rev, score = scores[j]
        if score_rev >= closing_rev:
          if not done_exact_rev:
            if score_rev > closing_rev:
              insert_index = j
              insert_score = scores[j-1][1] - closing_score
            done_exact_rev = 1
          scores[j] = (score_rev, score - closing_score)
        else:
          min = j + 1
      if not done_exact_rev:
        scores.append((closing_rev,scores[-1][1] - closing_score))
      if insert_index is not None:
        scores.insert(insert_index, (closing_rev, insert_score))
    return scores

  ###TODO find a better name for this method.
  def _condense_scores(self, rev_list):
    """Takes an array of revisions (REV_LIST), for example:

      [21, 18, 6, 49, 39, 24, 24, 24, 24, 24, 24, 24]

    and adds up every occurrence of each revision and returns a sorted
    array of tuples containing (svn_revnum, count):

      [(6, 1), (18, 1), (21, 1), (24, 7), (39, 1), (49, 1)]
    """
    s = {}
    for k in rev_list: # Add up the scores
      if s.has_key(k):
        s[k] = s[k] + 1
      else:
        s[k] = 1
    a = s.items()
    a.sort()
    return a

  def _list_revnums_for_key(self, node, revnum_type_key):
    """Scan self.node_tree and return a list of all the revision
    numbers (including duplicates) contained in REVNUM_TYPE_KEY values
    for all leaf nodes at and under NODE.

    REVNUM_TYPE_KEY should be either self.opening_key or
    self.closing_key."""
    revnums = []

    # If the node has self.opening_key, it must be a leaf node--all
    # leaf nodes have at least an opening key (although they may not
    # have a closing key.  Fetch revnum and return
    if (self.node_tree[node].has_key(self.opening_key) and
        self.node_tree[node].has_key(revnum_type_key)):
      revnums.append(self.node_tree[node][revnum_type_key])
      return revnums

    for key, node_contents in self.node_tree[node].items():
      if key[0] == '/': #Skip flags  ###TODO is this necessary?
        continue 
      revnums = revnums + \
          self._list_revnums_for_key(node_contents, revnum_type_key)
    return revnums

  def register(self, svn_path, svn_revnum, type):
    """Collects opening and closing revisions for this
    SymbolicNameFillingGuide.  SVN_PATH is the source path that needs
    to be copied into self.symbolic_name, and SVN_REVNUM is either the
    first svn revision number that we can copy from (our opening), or
    the last (not inclusive) svn revision number that we can copy from
    (our closing).  TYPE indicates whether this path is an opening or
    closing.

    The opening for a given SVN_PATH must be passed before the closing
    for it to have any effect... any closing encountered before a
    corresponding opening will be discarded.

    It is not necessary to pass a corresponding closing for every
    opening.
    """
    # Always log an OPENING
    if type == OPENING:
      self.things[svn_path] = {self.opening_key: svn_revnum}
    # Only log a closing if we've already registered the opening for that path.
    elif self.things.has_key(svn_path):
      self.things[svn_path][self.closing_key] = svn_revnum

  def make_node_tree(self):
    """Generates the SymbolicNameFillingGuide's node tree from
    self.things.  Each leaf node maps self.opening_key to the earliest
    subversion revision from which this node/path may be copied; and
    optionally map self.closing_key to the subversion revision one
    higher than the last revision from which this node/path may be
    copied.  Intermediate nodes never contain opening or closing
    flags."""

    for svn_path, open_close in self.things.items():
      parent_key = self.root_key

      path_so_far = ""
      # Walk up the path, one node at a time.
      components = svn_path.split('/')
      last_path_component = components[-1]
      for component in components:
        path_so_far = path_so_far + '/' + component

        child_key = None
        if not self.node_tree[parent_key].has_key(component):
          child_key = gen_key()
          self.node_tree[child_key] = { }
          self.node_tree[parent_key][component] = child_key
        else:
          child_key = self.node_tree[parent_key][component]

        # If this is the leaf, add the openings and closings.
        if component is last_path_component:
          self.node_tree[child_key] = open_close
        parent_key = child_key
    #print_node_tree(self.node_tree, self.root_key) 

  def is_empty(self):
    """Returns true if we haven't accumulated any openings or
    closings, false otherwise."""
    return not len(self.things)


def generate_offsets_for_symbolings():
  """This function iterates through all the lines in
  SYMBOL_OPENINGS_CLOSINGS_SORTED, writing out a file mapping
  SYMBOLIC_NAME to the file offset in SYMBOL_OPENINGS_CLOSINGS_SORTED
  where SYMBOLIC_NAME is first encountered.  This will allow us to
  seek to the various offsets in the file and sequentially read only
  the openings and closings that we need."""

  ###TODO This is a fine example of a db that can be in-memory and
  #just flushed to disk when we're done.  Later, it can just be sucked
  #back into memory.
  ###TODO we need to make sure that this file is deleted before
  ###starting this pass.  Find a better way. :)
  if os.path.isfile(SYMBOL_OFFSETS_DB):
    os.unlink(SYMBOL_OFFSETS_DB)
  offsets_db = Database(SYMBOL_OFFSETS_DB, 'c') 
  Cleanup().register(SYMBOL_OFFSETS_DB, pass8)
  
  file = open(SYMBOL_OPENINGS_CLOSINGS_SORTED, 'r')
  old_sym = ""
  while 1:
    line = file.readline()
    if not line:
      break
    sym, svn_revnum, cvs_rev_key = line.split(" ", 2)
    if not sym == old_sym:
      print sym
      old_sym = sym
      offsets_db[sym] = file.tell() - len(line)

def pass7(ctx):
  generate_offsets_for_symbolings()

###TODO move this stuff out of here! pass8 should be here.

class RepositoryBranchesInHead(Singleton):
  """Permanent storage for Subversion paths.

  While we're in the process of generating SVNCommits, this should be
  maintained to contain every branch path in the youngest revision of
  the repository and nothing else -- i.e., no non-branch paths at all,
  and no branch paths not in head.

  This class is used for determining whether a branch fill will be
  necessary when we see a commit on a branch."""
  # We're a singleton, so we use init, not __init__
  def init(self):
    self.db = Database(SVN_REPOSITORY_HEAD_DB, 'n')
    Cleanup().register(SVN_REPOSITORY_HEAD_DB, pass8) ##TODO earlier

  def add_path(self, path):
    """Remember that PATH exists."""
    self.db[path] = None

  def has_path(self, path):
    """Return true if PATH exists."""
    return self.db.has_key(path)

  def remove_path(self, path):
    """Forget that PATH exists."""
    del self.db[path]


class SVNCommitInternalInconsistencyError(Exception):
  """Exception raised if we encounter an impossible state in the
  SVNCommit Databases."""
  pass

class CommitMapper(Singleton):
  ###TODO Find a better docstring and/or name for this class.
  """Provide bidirectional mapping between CVSRevisions and SVNCommits.
  CTX is the usual annoying semi-global ctx object, which may become a
  singleton very soon, grrr."""
  def init(self, ctx):
    self._ctx = ctx
    self.svn2cvs_db = Database(SVN_REVNUMS_TO_CVS_REVS, 'c')
    Cleanup().register(SVN_REVNUMS_TO_CVS_REVS, pass8)
    self.cvs2svn_db = Database(CVS_REVS_TO_SVN_REVNUMS, 'c')
    Cleanup().register(CVS_REVS_TO_SVN_REVNUMS, pass8)
    self.svn_commit_names = Database(SVN_COMMIT_NAMES, 'c')
    Cleanup().register(SVN_COMMIT_NAMES, pass8)
    self.motivating_revnums = Database(MOTIVATING_REVNUMS, 'c')
    Cleanup().register(MOTIVATING_REVNUMS, pass8)
    self.svn_commit_metadata = Database(METADATA_DB, 'r')
    Cleanup().register(METADATA_DB, pass8)
    self.cvs_revisions = CVSRevisionDatabase('r', ctx)
    ###TODO kff Elsewhere there are comments about sucking the tags db
    ### into memory.  That seems like a good idea.
    ###TODO FITZ: We should set an is_tag var on SVNCommit...
    self.tags_db = TagsDatabase('r')

  def get_svn_revnum(self, cvs_rev_unique_key):
    """Return the Subversion revision number in which CVS_REV_UNIQUE_KEY
    was committed."""
    return int(self.cvs2svn_db[cvs_rev_unique_key])

  ###TODO: Move this to SVNCommit?
  def _manufacture_log_msg(self, symbolic_name, is_tag=None):
    """Return a log message for a manufactured commit that fills SYMBOLIC_NAME.
    If IS_TAG is true, write the log message as though for a tag, else
    write it as though for a branch.""" 
    type = 'branch'
    if is_tag:
      type = 'tag'

    # In Python 2.2.3, we could use textwrap.fill().  Oh well :-).
    space_or_newline = ' '
    if len(symbolic_name) >= 13:
      space_or_newline = '\n'
        
    return "This commit was manufactured by cvs2svn to create %s%s'%s'." \
           % (type, space_or_newline, symbolic_name)

  def get_svn_commit(self, svn_revnum):
    """Return an SVNCommit that corresponds to SVN_REVNUM.

    If no SVNCommit exists for revnum SVN_REVNUM, then return None.

    This method can throw SVNCommitInternalInconsistencyError.
    """
    svn_commit = SVNCommit(self._ctx, "Retrieved from disk", svn_revnum)
    c_rev_keys = self.svn2cvs_db.get(str(svn_revnum), None)
    if c_rev_keys == None:
      return None

    digest = None
    for key in c_rev_keys:
      c_rev = self.cvs_revisions.get_revision(key)
      svn_commit.add_revision(c_rev)
      # Set the author and log message for this commit by using
      # CVSRevision metadata, but only if haven't done so already.
      if digest is None:
        digest = c_rev.digest
        author, log_msg = self.svn_commit_metadata[digest]
        svn_commit.set_author(author)
        svn_commit.set_log_msg(log_msg)

    name = self.svn_commit_names.get(str(svn_revnum), None)
    if name:
      svn_commit.set_symbolic_name(name)
      svn_commit.set_author(self._ctx.username)
      if self.tags_db.has_key(name):
        msg = self._manufacture_log_msg(name, 1)
      else:
        msg = self._manufacture_log_msg(name)
      svn_commit.set_log_msg(msg)
      ###TODO kff: need a real date here, of course.
      svn_commit.set_date(0)

    motivating_revnum = self.motivating_revnums.get(str(svn_revnum), None)
    if motivating_revnum:
      motivating_revnum = int(motivating_revnum)
      svn_commit.set_motivating_revnum(motivating_revnum)
      ###TODO kff: get the actual related revnum here.
      ###TODO: This should be in SVNCommit
      msg = 'This commit was generated by cvs2svn to compensate for '     \
            'changes in r%d,\n'                                           \
            'which included commits to RCS files with non-trunk default ' \
            'branches.\n' % motivating_revnum
      svn_commit.set_author(self._ctx.username)
      svn_commit.set_log_msg(msg)
      ###TODO kff: need a real date here, of course.
      svn_commit.set_date(0)

    if len(svn_commit.cvs_revs) and name:
      msg = """An SVNCommit cannot have cvs_revisions *and* a
      corresponding symbolic name ('%s') to fill.""" % name
      raise SVNCommitInternalInconsistencyError(msg)

    return svn_commit
    
  def set_cvs_revs(self, svn_revnum, cvs_revs):
    """Record the bidirectional mapping between SVN_REVNUM and
    CVS_REVS.""" 
    for c_rev in cvs_revs:
      print "    KFF:", c_rev.unique_key()
    print "------------------------------------------------------------- KFF"
    self.svn2cvs_db[str(svn_revnum)] = [x.unique_key() for x in cvs_revs]
    for c_rev in cvs_revs:
      self.cvs2svn_db[c_rev.unique_key()] = svn_revnum

  def set_name(self, svn_revnum, name):
    """Store NAME as the value of SVN_REVNUM"""
    self.svn_commit_names[str(svn_revnum)] = name

  def set_motivating_revnum(self, svn_revnum, motivating_revnum):
    """Store MOTIVATING_REVNUM as the value of SVN_REVNUM"""
    self.motivating_revnums[str(svn_revnum)] = str(motivating_revnum)


# Based on Commit
class CVSCommit:
  """Each instance of this class contains a number of CVS Revisions
  that correspond to one or more Subversion Commits.  After all CVS
  Revisions are added to the grouping, calling process_revisions will
  generate a Subversion Commit (or Commits) for the set of CVS
  Revisions in the grouping."""

  def __init__(self, ctx, digest, author, log):
    self.isClosed = 0
    self._ctx = ctx

    self.digest = author
    self.author = author
    self.log = log

    self.files = { }
    # Lists of CVSRevisions
    self.changes = [ ]
    self.deletes = [ ]

    # Start out with a t_min higher than any incoming time T, and a
    # t_max lower than any incoming T.  This way the first T will
    # push t_min down to T, and t_max up to T, naturally (without any
    # special-casing), and successive times will then ratchet them
    # outward as appropriate.
    self.t_min = 1L<<32
    self.t_max = 0

    # This will be set to the SVNCommit that occurs in self._commit.
    self.motivating_commit = None

    # This is a list of all non-primary commits motivated by the main
    # commit.  We gather these so that we can set their dates to the
    # same date as the primary commit.
    self.secondary_commits = [ ]
    
    # State for handling default branches.
    # 
    # Here is a tempting, but ultimately nugatory, bit of logic, which
    # I share with you so you may appreciate the less attractive, but
    # refreshingly non-nugatory, logic which follows it:
    #
    # If some of the commits in this txn happened on a non-trunk
    # default branch, then those files will have to be copied into
    # trunk manually after being changed on the branch (because the
    # RCS "default branch" appears as head, i.e., trunk, in practice).
    # As long as those copies don't overwrite any trunk paths that
    # were also changed in this commit, then we can do the copies in
    # the same revision, because they won't cover changes that don't
    # appear anywhere/anywhen else.  However, if some of the trunk dst
    # paths *did* change in this commit, then immediately copying the
    # branch changes would lose those trunk mods forever.  So in this
    # case, we need to do at least that copy in its own revision.  And
    # for simplicity's sake, if we're creating the new revision for
    # even one file, then we just do all such copies together in the
    # new revision.
    #
    # Doesn't that sound nice?
    #
    # Unfortunately, Subversion doesn't support copies with sources
    # in the current txn.  All copies must be based in committed
    # revisions.  Therefore, we generate the above-described new
    # revision unconditionally.
    #
    # This is a list of c_revs, and a c_rev is appended for each
    # default branch commit that will need to be copied to trunk (or
    # deleted from trunk) in some generated revision following the
    # "regular" revision.
    self.default_branch_cvs_revisions = [ ]

  def __cmp__(self, other):
    # Commits should be sorted by t_max.  If both self and other have
    # the same t_max, break the tie using t_min, and lastly, digest
    return (cmp(self.t_max, other.t_max) or cmp(self.t_min, other.t_min)
            or cmp(self.digest, other.digest))

  def has_file(self, fname):
    return self.files.has_key(fname)

  def revisions(self):
    return self.changes + self.deletes

  def opens_symbolic_name(self, name):
    """Returns true if any CVSRevision in this commit is on a tag or a
    branch or is the origin of a tag or branch."""
    for c_rev in self.revisions():
      if c_rev.opens_symbolic_name(name):
        return 1
    return 0

  def add_revision(self, c_rev):
    # Record the time range of this commit.
    #
    # ### ISSUE: It's possible, though unlikely, that the time range
    # of a commit could get gradually expanded to be arbitrarily
    # longer than COMMIT_THRESHOLD.  I'm not sure this is a huge
    # problem, and anyway deciding where to break it up would be a
    # judgement call. For now, we just print a warning in commit() if
    # this happens.
    if c_rev.timestamp < self.t_min:
      self.t_min = c_rev.timestamp
    if c_rev.timestamp > self.t_max:
      self.t_max = c_rev.timestamp

    if c_rev.op == OP_DELETE:
      self.deletes.append(c_rev)
    else:
      # OP_CHANGE or OP_ADD
      self.changes.append(c_rev)

    self.files[c_rev.fname] = 1

  def _get_properties(self, c_rev):
    # we already have the date, so just format it
    date = format_date(self.t_max)
    try: 
      ### FIXME: The 'replace' behavior should be an option, like
      ### --encoding is.
      unicode_author = unicode(self.author, ctx.encoding, 'replace')
      unicode_log = unicode(self.log, ctx.encoding, 'replace')
      props = { 'svn:author' : unicode_author.encode('utf8'),
                'svn:log' : unicode_log.encode('utf8'),
                'svn:date' : date }
    except UnicodeError:
      print '%s: problem encoding author or log message:' % warning_prefix
      print "  author: '%s'" % self.author
      print "  log:    '%s'" % self.log
      print "  date:   '%s'" % date
      for c_rev in self.changes:
        print "    rev %s of '%s'" % (c_rev.rev, c_rev.fname)
      print "Consider rerunning with (for example) '--encoding=latin1'."
      # Just fall back to the original data.
      props = { 'svn:author' : self.author,
                'svn:log' : self.log,
                'svn:date' : date }
    return props

  def _pre_commit(self):    
    """Generates any SVNCommits that must exist before the main
    commit."""

    # There may be multiple c_revs in this commit that would cause
    # branch B to be filled, but we only want to fill B once.  On the
    # other hand, there might be multiple branches committed on in
    # this commit.  Whatever the case, we should count exactly one
    # commit per branch, because we only fill a branch once per
    # CVSCommit.  This list tracks which branches we've already
    # counted.
    accounted_for_sym_names = [ ]

    for c_rev in self.changes:
      # If a commit is on a branch, we must ensure that the branch
      # path being committed exists (in HEAD of the Subversion
      # repository).  If it doesn't exist, we will need to fill the
      # branch.  After the fill, the path on which we're committing
      # will exist.
      if c_rev.branch_name: ###TODO collapse if clauses
        ###TODO Check c_rev.op to see if we're an add!
        if not RepositoryBranchesInHead().has_path(c_rev.svn_path):
          ### TODO: Possible correctness issue here.  If we have a
          # branch with a single file on it, and that file was deleted
          # in the previous CVS revision, and resurrected in this
          # revision, we may not have anything to fill here, and thus,
          # given the current state of the code, we're generating an
          # empty SVN commit.  This is likely a rare edge case, and
          # generating the empty commit isn't the end of the world,
          # but hey, we're all idealists here, aren't we?
          if ((not c_rev.branch_name in accounted_for_sym_names)
              and (not c_rev.branch_name in self.done_symbols)):
            svn_commit = SVNCommit(self._ctx,
                                   "pre-commit branch '%s' (op == '%s')"
                                   % (c_rev.branch_name, c_rev.op))
            svn_commit.set_symbolic_name(c_rev.branch_name)
            self.secondary_commits.append(svn_commit)

            accounted_for_sym_names.append(c_rev.branch_name)
          RepositoryBranchesInHead().add_path(c_rev.svn_path)

    ###TODO Make sure that changes and deletes HAVE to be separate
    ### lists
    for c_rev in self.deletes:
      if c_rev.branch_name:
        ###TODO Check c_rev.op to see if we're an add!
        if not RepositoryBranchesInHead().has_path(c_rev.svn_path):
          if ((not c_rev.branch_name in accounted_for_sym_names)
              and (not c_rev.branch_name in self.done_symbols)):
            svn_commit = SVNCommit(self._ctx, "pre-commit branch DELETE '%s'"
                                   % c_rev.branch_name)
            svn_commit.set_symbolic_name(c_rev.branch_name)
            svn_commit.flush()
            accounted_for_sym_names.append(c_rev.branch_name)
        else:
          RepositoryBranchesInHead().remove_path(c_rev.svn_path)

  def _commit(self):
    """Generates the primary SVNCommit that corresponds the this
    CVSCommit."""
    # Generate an SVNCommit unconditionally.  Even if the only change
    # in this CVSCommit is a deletion of an already-deleted file (that
    # is, a CVS revision in state 'dead' whose predecessor was also in
    # state 'dead'), the conversion will still generate a Subversion
    # revision containing the log message for the second dead
    # revision, because we don't want to lose that information.
    svn_commit = SVNCommit(self._ctx, "commit")
    self.motivating_commit = svn_commit

    for c_rev in self.changes:
      svn_commit.add_revision(c_rev)
      # Only make a change if we need to.  When 1.1.1.1 has an empty
      # deltatext, the explanation is almost always that we're looking
      # at an imported file whose 1.1 and 1.1.1.1 are identical.  On
      # such imports, CVS creates an RCS file where 1.1 has the
      # content, and 1.1.1.1 has an empty deltatext, i.e, the same
      # content as 1.1.  There's no reason to reflect this non-change
      # in the repository, so we want to do nothing in this case.  (If
      # we were really paranoid, we could make sure 1.1's log message
      # is the CVS-generated "Initial revision\n", but I think the
      # conditions below are strict enough.)

      ###TODO We don't need to use RepositoryBranchesInHead here...
      ###we can figure out the same thing by examining our previous
      ###revision--if it's a trunk rev (2 components), then that means
      ###that our svn_path doesn't exist in the repository.
      ###TODO: After we make that change, then this bit of logic
      ###should move to inside of CVSRevision.
      if not ((c_rev.deltatext_code == DELTATEXT_EMPTY)
              and (c_rev.rev == "1.1.1.1")
              and RepositoryBranchesInHead().has_path(c_rev.svn_path)):
        if c_rev.is_default_branch_revision():
          self.default_branch_cvs_revisions.append(c_rev)

    for c_rev in self.deletes:
      svn_commit.add_revision(c_rev)
      if c_rev.is_default_branch_revision():
        self.default_branch_cvs_revisions.append(c_rev)

    svn_commit.flush()

    for c_rev in self.revisions():
      SymbolingsLogger(self._ctx).check_revision(c_rev, svn_commit.revnum)

  ###TODO We need a test case where we have a file on a default branch
  #and on the HEAD revision of the default branch in CVS, that file is
  #in RCS state 'dead'
  def _post_commit(self):
    """Generates any SVNCommits that we can perform now that _commit
    has happened.  That is,

    1) Handle non-trunk default branches.  Sometimes an RCS file has a
       non-trunk default branch, so a commit on that default branch
       would be visible in a default CVS checkout of HEAD.  If we
       don't copy that commit over to Subversion's trunk, then there
       will be no Subversion tree which corresponds to that CVS
       checkout.  Of course, in order to copy the path over, we may
       first need to delete the existing trunk there.
       """
    # Generate one SVNCommit for each default branch we encounter.
    # Add each c_rev on that branch to the commit, and flush all
    # commits at the end.

    ###TODO: There doesn't seem to be any real reason to generate an
    # SVNCommit for each default branch that we encounter here--after
    # all, we may have just committed changes to a whole bunch of
    # default branches in _commit, so there's no real reason to divvy
    # this out into separate commits.  This will also require that we
    # change SVNCommit.default_branch to SVNCommit.is_default_branch
    # which will indicate that c_rev.branch_name is the default branch
    # name for that c_rev in the commit.  Once the redesign is
    # complete, we should revisit this.
    svn_commits = { }
    for c_rev in self.default_branch_cvs_revisions:
      if not svn_commits.has_key(c_rev.branch_name):
        svn_commit = SVNCommit(self._ctx, "post-commit def_br_[copy|delete]")
        svn_commit.set_motivating_revnum(self.motivating_commit.revnum)
        svn_commit.add_revision(c_rev)
        svn_commits[c_rev.branch_name] = svn_commit
      else:
        svn_commit = svn_commits[c_rev.branch_name]
        svn_commit.add_revision(c_rev)

    for svn_commit in svn_commits.values():
      self.secondary_commits.append(svn_commit)

  def process_revisions(self, ctx, done_symbols):
    self.done_symbols = done_symbols
    seconds = self.t_max - self.t_min
    print ('CVS Revision grouping: %s, over %d seconds'
           % (time.ctime(self.t_min), seconds))
    if seconds > COMMIT_THRESHOLD:
      print ('%s: grouping spans more than %d seconds'
             % (warning_prefix, COMMIT_THRESHOLD))

    self._pre_commit()
    self._commit()
    self._post_commit()

    for svn_commit in self.secondary_commits:
      svn_commit.set_date(self.motivating_commit.get_date())
      svn_commit.flush()


class SVNCommit:
  """This represents one commit to the Subversion Repository.  There
  are three types of SVNCommits:

  1. Commits one or more CVSRevisions (cannot fill a symbolic name).

  2. Creates or fills a symbolic name (cannot commit CVSRevisions).

  3. Updates trunk to reflect the contents of a particular branch
     (this is to handle RCS default branches)."""
  def __init__(self, ctx, description="", revnum=None, cvs_revs=None):
    """Instantiate an SVNCommit with CTX.  DESCRIPTION is for debugging only.
    If REVNUM, the SVNCommit will correspond to that revision number;
    and if CVS_REVS, then they must be the exact set of CVSRevisions for
    REVNUM.

    It is an error to pass CVS_REVS without REVNUM, but you may pass
    REVNUM without CVS_REVS, and then add a revision at a time by
    invoking add_revision()."""
    self._ctx = ctx
    self._description = description

    # Revprop metadata for this commit.
    #
    # These initial values are placeholders.  At least the log and the
    # date should be different by the time these are used.
    #
    # They are private because their values should be returned encoded
    # in UTF8, but callers aren't required to set them in UTF8.
    # Therefore, accessor methods are used to set them, and
    # self.get_revprops() is used to to get them, in dictionary form.
    self._author = self._ctx.username
    self._log_msg = "This log message means an SVNCommit was used too soon."
    self._max_date = 0  # Latest date seen so far.

    self.cvs_revs = cvs_revs
    if cvs_revs is not None:
      self.cvs_revs = cvs_revs
    else:
      self.cvs_revs = []

    if not revnum:
      self.revnum = SVNRevNum().get_next_revnum()
    else:
      self.revnum = revnum
    # The symbolic name that is filled in this SVNCommit, if any
    self.symbolic_name = None

    # If this commit is a default branch synchronization, this
    # variable represents the subversion revision number of the
    # *primary* commit where the default branch changes actually
    # happened.  It is None otherwise.
    #
    # It is possible for multiple for multiple synchronization commits
    # to refer to the same motivating commit revision number, and it
    # is possible for a single synchronization commit to contain
    # CVSRevisions on multiple different default branches.
    self.motivating_revnum = None

  def set_symbolic_name(self, name):
    "Set self.symbolic_name to NAME."
    name = _clean_symbolic_name(name)
    self.symbolic_name = name
    CommitMapper(self._ctx).set_name(self.revnum, name)

  def set_motivating_revnum(self, revnum):
    "Set self.motivating_revnum to REVNUM."
    self.motivating_revnum = revnum
    CommitMapper(self._ctx).set_motivating_revnum(self.revnum, revnum)

  def set_author(self, author):
    """Set this SVNCommit's author to AUTHOR (a locally-encoded string).
    This is the only way to set an SVNCommit's author."""
    self._author = author

  def set_log_msg(self, msg):
    """Set this SVNCommit's log message to MSG (a locally-encoded string).
    This is the only way to set an SVNCommit's log message."""
    self._log_msg = msg

  def set_date(self, date):
    """Set this SVNCommit's date to DATE (an integer).
    Note that self.add_revision() updates this automatically based on
    a CVSRevision; so you may not need to call this at all, and even
    if you do, the value may be overwritten by a later call to
    self.add_revision()."""
    self._max_date = date
   
  def get_date(self):
    """Returns this SVNCommit's date as an integer."""
    return self._max_date
   
  def get_revprops(self):
    """Return the Subversion revprops for this SVNCommit."""
    date = format_date(self._max_date)
    try: 
      ### FIXME: The 'replace' behavior should be an option, like
      ### --encoding is.
      unicode_author = unicode(self._author, self._ctx.encoding, 'replace')
      unicode_log = unicode(self._log_msg, self._ctx.encoding, 'replace')
      return { 'svn:author' : unicode_author.encode('utf8'),
               'svn:log'    : unicode_log.encode('utf8'),
               'svn:date'   : date }
    except UnicodeError:
      print '%s: problem encoding author or log message:' % warning_prefix
      print "  author: '%s'" % self._author
      print "  log:    '%s'" % self._log_msg
      print "  date:   '%s'" % date
      print "(for rev %s of '%s')" % (cvs_rev.rev, cvs_rev.fname)
      print "Consider rerunning with (for example) '--encoding=latin1'."
      return { 'svn:author' : self._author,
               'svn:log'    : self._log_msg,
               'svn:date'   : date }

  def add_revision(self, cvs_rev):
    self.cvs_revs.append(cvs_rev)
    if cvs_rev.timestamp > self._max_date:
      self._max_date = cvs_rev.timestamp

  def flush(self):
    print "KFF: svn_revnum %d  ('%s') ->" % (self.revnum, self._description)
    CommitMapper(self._ctx).set_cvs_revs(self.revnum, self.cvs_revs)

  def __str__(self):
    """ Print a human-readable description of this SVNCommit.  This
    description is not intended to be machine-parseable (although
    we're not going to stop you if you try!)"""

    ret = "SVNCommit #: " + str(self.revnum) + "\n"
    if self.symbolic_name:
      ret = ret + "   symbolic name: " +  self.symbolic_name + "\n"
    else:
      ret = ret + "   NO symbolic name\n"
    ret = ret + "   debug description: " + self._description + "\n"
    ret = ret + "   cvs_revs:\n"
    for c_rev in self.cvs_revs:
      ret = ret + "     " + c_rev.unique_key() + "\n"
    return ret

class SVNRevNum(Singleton):
  def init(self):
    self.revnum = 0
    
  def get_next_revnum(self):
    self.revnum = self.revnum + 1
    return self.revnum


class CVSRevisionAggregator:
  """This class groups CVSRevisions into CVSCommits that represent
  at least one SVNRevision."""
  def __init__(self, ctx):
    self._ctx = ctx
    self.metadata_db = Database(METADATA_DB, 'r')
    ###TODO Cleanup().register()
    ###TODO WARNING: What happens when we call cleanup.register
    ###multiple times on the same file?????????????
    self.last_revs_db = Database(SYMBOL_LAST_CVS_REVS_DB, 'r')
    ###TODO Cleanup().register()
    self.cvs_commits = {}
    self.pending_symbols = {}
    # A list of symbols for which we've already encountered the last
    # CVSRevision that is a source for that symbol.  That is, the
    # final fill for this symbol has been done, and we never need to
    # fill it again.
    self.done_symbols = [ ]

  def process_revision(self, c_rev):
    # Each time we read a new line, we scan the commits we've
    # accumulated so far to see if any are ready for processing now.
    ready_queue = [ ]
    for digest_key, cvs_commit in self.cvs_commits.items():
      if cvs_commit.t_max + COMMIT_THRESHOLD < c_rev.timestamp:
        ready_queue.append(cvs_commit)
        del self.cvs_commits[digest_key]
        continue
      # If the inbound commit is on the same file as a pending commit,
      # close the pending commit to further changes. Don't flush it though,
      # as there may be other pending commits dated before this one.
      # ### ISSUE: the has_file() check below is not optimal.
      # It does fix the dataloss bug where revisions would get lost
      # if checked in too quickly, but it can also break apart the
      # commits. The correct fix would require tracking the dependencies
      # between change sets and committing them in proper order.
      if cvs_commit.has_file(c_rev.fname):
        unused_id = digest_key + '-'
        # Find a string that does is not already a key in
        # the self.cvs_commits dict
        while self.cvs_commits.has_key(unused_id):
          unused_id = unused_id + '-'
        self.cvs_commits[unused_id] = cvs_commit
        del self.cvs_commits[digest_key]

    # Add this item into the set of still-available commits.
    if self.cvs_commits.has_key(c_rev.digest):
      cvs_commit = self.cvs_commits[c_rev.digest]
    else:
      author, log = self.metadata_db[c_rev.digest]
      self.cvs_commits[c_rev.digest] = CVSCommit(self._ctx,
                                               c_rev.digest,
                                               author, log)
      cvs_commit = self.cvs_commits[c_rev.digest]
    cvs_commit.add_revision(c_rev)

    # If there are any elements in the ready_queue at this point, they
    # need to be processed, because this latest rev couldn't possibly
    # be part of any of them.  Sort them into time-order, then process
    # 'em.
    ready_queue.sort()

    # Make sure we attempt_to_commit_symbols for this c_rev, even if no
    # commits are ready.
    if len(ready_queue) == 0:
      self.attempt_to_commit_symbols(ready_queue, c_rev, ) 

    for cvs_commit in ready_queue[:]:
      cvs_commit.process_revisions(self._ctx, self.done_symbols)
      ready_queue.remove(cvs_commit)
      self.attempt_to_commit_symbols(ready_queue, c_rev, ) 

  def flush(self):
    """Commit anything left in self.cvs_commits."""

    ready_queue = [ ]
    for k, v in self.cvs_commits.items():
      ready_queue.append((v, k))

    ready_queue.sort()
    for cvs_commit_tuple in ready_queue[:]:
      cvs_commit_tuple[0].process_revisions(self._ctx, self.done_symbols)
      ready_queue.remove(cvs_commit_tuple)
      del self.cvs_commits[cvs_commit_tuple[1]]
      self.attempt_to_commit_symbols([]) 
    
  def attempt_to_commit_symbols(self, queued_commits, c_rev=None):
    """
    This function generates 1 SVNCommit for each symbol in
    self.pending_symbols that doesn't have an opening CVSRevision in
    either QUEUED_COMMITS or self.cvs_commits.values().

    If C_REV is not None, then we first add to self.pending_symbols
    any symbols from C_REV that C_REV is the last CVSRevision for.
    """
    # Get the symbolic names that this c_rev is the last *source*
    # CVSRevision for and add them to those left over from previous
    # passes through the aggregator.
    if c_rev:
      for sym in self.last_revs_db.get(c_rev.unique_key(), []):
        self.pending_symbols[sym] = None

    # Make a list of all symbols that still have *source* CVSRevisions
    # in the pending commit queue (self.cvs_commits).
    open_symbols = {}
    for sym in self.pending_symbols.keys():
      for cvs_commit in self.cvs_commits.values() + queued_commits:
        if cvs_commit.opens_symbolic_name(sym):
          open_symbols[sym] = None
          break

    # Sort the pending symbols so that we will always process the
    # symbols in the same order, regardless of the order in which the
    # dict hashing algorithm hands them back to us.  We do this so
    # that our tests will get the same results on all platforms.
    sorted_pending_symbols_keys = self.pending_symbols.keys()
    sorted_pending_symbols_keys.sort()
    for sym in sorted_pending_symbols_keys:
      if open_symbols.has_key(sym): # sym is still open--don't close it.
        continue
      svn_commit = SVNCommit(self._ctx, "closing tag/branch '%s'" % sym)
      svn_commit.set_symbolic_name(sym)
      svn_commit.flush()
      self.done_symbols.append(sym)
      del self.pending_symbols[sym]


class SVNRepositoryMirrorPathExistsError(Exception):
  """Exception raised if an attempt is made to add a path to the
  repository mirror and that path already exists in the youngest
  revision of the repository."""
  pass

class SVNRepositoryMirrorUnexpectedOperationError(Exception):
  """Exception raised if a CVSRevision is found to have an unexpected
  operation (OP) value."""
  pass

# This supersedes and will eventually replace RepositoryMirror.
class SVNRepositoryMirror:
  """Mirror a Subversion Repository as it is constructed, one
  SVNCommit at a time.  The mirror is skeletal; it does not contain
  file contents.  If you set the delegate, as soon as SVNRepository
  performs a repository action method ([add|change|delete|copy]path),
  it will call the delegate's corresponding repository action method.
  See SVNRepositoryMirrorDelegate for more information.

  You must invoke start_commit between SVNCommits.

  *** WARNING *** All path arguments to methods in this class CANNOT
      have leading or trailing slashes.

  """
  def __init__(self, ctx, delegate=None):
    """Set up the SVNRepositoryMirror and prepare it for SVNCommits."""
    self._ctx = ctx
    self.delegate = delegate
    self.delegate.set_mirror(self)

    # This corresponds to the 'revisions' table in a Subversion fs.
    self.revs_db = Database(SVN_MIRROR_REVISIONS_DB, 'n')
    Cleanup().register(SVN_MIRROR_REVISIONS_DB, pass8, self.close)

    # This corresponds to the 'nodes' table in a Subversion fs.  (We
    # don't need a 'representations' or 'strings' table because we
    # only track metadata, not file contents.)
    self.nodes_db = Database(SVN_MIRROR_NODES_DB, 'n')
    Cleanup().register(SVN_MIRROR_NODES_DB, pass8, self.close)

    # Init a root directory with no entries at revision 0.
    self.youngest = 0
    youngest_key = gen_key()
    self.revs_db[str(self.youngest)] = youngest_key
    self.nodes_db[youngest_key] = { }

    ###TODO Will we still need these?
    #
    # Set to 1 on a directory that's mutable in the revision currently
    # being constructed.  (Yes, this is exactly analogous to the
    # Subversion filesystem code's concept of mutability.)
    #
    # It is overloaded with a second piece of information:
    #
    # If the value of the flag is 2, then in addition to the node
    # being mutable, the node and all subnodes were created by a copy
    # operation in the current revision.  In this and only this
    # circumstance, it is valid for pruning to occur.
    ###TODO make this 2 characters long
    self.mutable_flag = "/mutable"
    # This could represent a new mutable directory or file.
    self.empty_mutable_thang = { self.mutable_flag : 1 }

    ###TODO IMPT: Suck this into memory.
    self.tags_db = TagsDatabase('r')

    self.symbolings_reader = SymbolingsReader(self._ctx)

    # Note that we haven't started committing yet
    self.active = 0


  def _youngest_key(self):
    "Returns the value of self.youngest as a string."
    return str(self.youngest)

  def start_commit(self, revnum):
    """Stabilize the current commit, then start the next one.
    (Effectively increments youngest by assigning the new revnum to
    youngest)"""
    self.stabilize_youngest()
    self.revs_db[str(revnum)] = self.revs_db[self._youngest_key()]
    self.youngest = revnum

  def _stabilize_directory(self, key):
    """Remove the mutable flag from the directory whose node key is
    KEY, effectively marking the directory as immutable."""

    dir = self.nodes_db[key]
    if dir.has_key(self.mutable_flag):
      del dir[self.mutable_flag]
      for entry_key in dir.keys():
        if not entry_key[0] == '/':
          self._stabilize_directory(dir[entry_key])
      self.nodes_db[key] = dir

  def stabilize_youngest(self):
    """Stabilize the current revision by removing mutable flags."""
    root_key = self.revs_db[self._youngest_key()]
    self._stabilize_directory(root_key)

  def delete_path(self, path):
    """Delete PATH from the tree.  PATH may not have a leading slash.

    Return the path actually deleted or None if PATH did not exist.
    This is the path on which self.delegate.delete_path will be
    invoked (exactly once), and the delegate will not be invoked at
    all if no path was deleted.

    If self._ctx.prune is not None, then delete the highest possible
    directory, which means the returned path may differ from PATH.  In
    other words, if PATH was the last entry in its parent, then delete
    PATH's parent, unless it too is the last entry in *its* parent, in
    which case delete that parent, and so on up the chain, until a
    directory is encountered that has an entry which is not a member
    of the parent stack of the original target.

    NOTE: This function does *not* allow you delete top-level entries
    (like /trunk, /branches, /tags), nor does it prune upwards beyond
    those entries.

    self._ctx.prune is like the -P option to 'cvs checkout'."""
    parent_key = self.revs_db[self._youngest_key()]
    parent = self.nodes_db[parent_key]

    # As we walk down to find the dest, we remember each parent
    # directory's name and db key, in reverse order: push each new key
    # onto the front of the list, so that by the time we reach the
    # destination node, the zeroth item in the list is the parent of
    # that destination.
    #
    # Then if we actually do the deletion, we walk the list from left
    # to right, replacing as appropriate (since we may have to
    # bubble-down).
    #
    # The root directory has name None.
    parent_chain = [ ]
    parent_chain.insert(0, (None, parent_key))

    def is_prunable(dir):
      """Return true if DIR, a dictionary representing a directory,
      has just zero or one non-special entry, else return false.
      (In a pure world, we'd just ask len(DIR) > 1; it's only
      because the directory might have mutable flags and other special
      entries that we need this function at all.)"""
      num_items = 0
      for key in dir.keys():
        if key[0] == '/':
          continue
        if num_items == 1:
          return None
        num_items = num_items + 1
      return 1

    path_so_far = None
    components = string.split(path, '/')
    ###TODO This is a problem if trunk/tags/branches is > 1 component long
    ### See issue #7.
    # We never prune our top-level directories (/trunk, /tags, /branches)
    if len(components) < 2:
      return None
    
    last_component = components[-1]
    for component in components[:-1]:
      if path_so_far:
        path_so_far = path_so_far + '/' + component
      else:
        path_so_far = component

      # If we can't reach the dest, then we don't need to do anything.
      if not parent.has_key(component):
        return None

      # Otherwise continue downward, dropping breadcrumbs.
      this_entry_key = parent[component]
      this_entry_val = self.nodes_db[this_entry_key]
      parent_key = this_entry_key
      parent = this_entry_val
      parent_chain.insert(0, (component, parent_key))

    # If the target is not present in its parent, then we're done.
    if not parent.has_key(last_component):
      return None

    # The target is present, so remove it and bubble up, making a new
    # mutable path and/or pruning as necessary.
    pruned_count = 0
    prev_entry_name = last_component
    new_key = None
    for parent_item in parent_chain:
      pkey = parent_item[1]
      pval = self.nodes_db[pkey]

      # If we're pruning at all, and we're looking at a prunable thing
      # (and that thing isn't one of our top-level directories --
      # trunk, tags, branches) ...
      if self._ctx.prune and (new_key is None) and is_prunable(pval) \
         and parent_item != parent_chain[-2]:
        # ... then up our count of pruned items, and do nothing more.
        # All the action takes place when we hit a non-prunable
        # parent.
        pruned_count = pruned_count + 1
      else:
        # Else, we've hit a non-prunable, or aren't pruning, so bubble
        # up the new gospel.
        pval[self.mutable_flag] = 1
        if new_key is None:
          del pval[prev_entry_name]
        else:
          pval[prev_entry_name] = new_key
        new_key = gen_key()

      prev_entry_name = parent_item[0]
      if new_key:
        self.nodes_db[new_key] = pval

    if new_key is None:
      new_key = gen_key()
      self.nodes_db[new_key] = self.empty_mutable_thang

    # Install the new root entry.
    self.revs_db[str(self.youngest)] = new_key

    # Sanity check -- this should be a "can't happen".
    if pruned_count > len(components):
      sys.stderr.write("%s: deleting '%s' tried to prune %d components.\n"
                       % (error_prefix, path, pruned_count))
      sys.exit(1)

    if pruned_count:
      if pruned_count == len(components):
        # We never prune away the root directory, so back up one component.
        pruned_count = pruned_count - 1
      retpath = string.join(components[:0 - pruned_count], '/')
    else:
      retpath = path

    self.delegate.delete_path(retpath)
    return retpath

  def mkdir(self, path):
    """Create PATH in the repository mirror at the youngest revision."""
    # Since we make no distinction from a file and a directory in the
    # mirror, we can merely leverage self._add_or_change_path here
    self._add_or_change_path(path)
    self.delegate.mkdir(path)

  def change_path(self, cvs_rev):
    """Register a change in self.youngest for the CVS_REV's svn_path
    in the repository mirror."""
    self._add_or_change_path(cvs_rev.svn_path)
    self.delegate.change_path(cvs_rev)

  def add_path(self, cvs_rev):
    """Add the CVS_REV's svn_path to the repository mirror."""
    self._add_or_change_path(cvs_rev.svn_path)
    self.delegate.add_path(cvs_rev)

  def _add_or_change_path(self, svn_path):
    """From the youngest revision, bubble down a chain of mutable
    nodes for SVN_PATH.  Create new (mutable) nodes as necessary, and
    calls self.delegate.mkdir() once on each intermediate path it
    creates.

    This makes nodes mutable only as needed, otherwise, mutates any
    mutable nodes it encounters."""
    ###TODO For inconsistency checking, we could return whether or not
    ###we added a node representing a new path (or path element).
    ###This could be checked by the caller and an exception could be
    ###thrown if the response was unexpected.  Code defensively.
    parent_node_key, parent_node_contents = self._get_youngest_root_node()

    path_so_far = None
    # Walk up the path, one node at a time.
    components = svn_path.split('/')
    last_component = components[-1]
    for component in components:
      if path_so_far:
        path_so_far = path_so_far + '/' + component
      else:
        path_so_far = component
      this_node_key = this_node_contents = None
      # If the parent_node_contents doesn't have an entry for this
      # component, create a new node for the component and add it to
      # the parent_node_contents.
      if not parent_node_contents.has_key(component):
        this_node_key, this_node_contents = self._new_mutable_node()
        parent_node_contents[component] = this_node_key
        # Update the parent node in the db
        self.nodes_db[parent_node_key] = parent_node_contents
        # If we create a new node and it's not a leaf node, then we've just
        # created a new directory.  Let the delegate know.
        if component is not last_component:
          self.delegate.mkdir(path_so_far)
      else:
        # NOTE: The following clause is essentially _open_path, but to
        # use it here would mean that we would have to re-walk our
        # path_so_far * len(components), which is inefficient.
        # Perhaps someone could re-work _open_path to accomodate this,
        # but I don't think it's all that impt.
        #
        # One way or another, parent dir now has an entry for component,
        # so grab it, see if it's mutable, and DTRT if it's not.
        this_node_key = parent_node_contents[component]
        this_node_contents = self.nodes_db[this_node_key]
        mutable = this_node_contents.get(self.mutable_flag)
        if not mutable:
          this_node_key, this_node_contents \
                         = self._new_mutable_node(this_node_contents)
          parent_node_contents[component] = this_node_key
          self.nodes_db[parent_node_key] = parent_node_contents

      parent_node_key = this_node_key
      parent_node_contents = this_node_contents


  def _new_mutable_node(self, node_contents=None):
    """Creates a new (mutable) node in the nodes_db and returns the
    node's key.  If NODE_CONTENTS is not None, then dict.update() the
    contents of the new node with NODE_CONTENTS before returning."""
    contents = dict(self.empty_mutable_thang)
    if node_contents is not None:
      contents.update(node_contents)
    key = gen_key()
    self.nodes_db[key] = contents
    return key, contents

  def _get_youngest_root_node(self):
    """Gets the root node key for the youngest revision.  If it's
    immutable (i.e. our current operation is the first one on this
    commit), create and return the key to a new root node.  Always
    returns a key pointing to a mutable node."""
    parent_key, parent = self._get_root_node_for_revnum(self.youngest)
    if not parent.has_key(self.mutable_flag):
      parent_key, parent = self._new_mutable_node(parent)
      self.revs_db[self._youngest_key()] = parent_key
    return parent_key, parent

  def _get_root_node_for_revnum(self, revnum):
    """Gets the root node key for the revision REVNUM."""
    parent_key = self.revs_db[str(revnum)]
    parent = self.nodes_db[parent_key]
    return parent_key, parent

  def fill_symbolic_name(self, symbolic_name):
    """Performs all copies necessary to create as much of the the tag
    or branch SYMBOLIC_NAME as possible given the current revision of
    the repository mirror."""
    symbol_fill = self.symbolings_reader.filling_guide_for_symbol(
      symbolic_name, self.youngest)

    self._fill(symbol_fill, symbol_fill.root_key, symbolic_name)

  ###TODO We need a test here, to make sure that tag copies get the
  ###correct version of a file when a file is tagged while the default
  ###branch is non-trunk.
  def synchronize_default_branch(self, svn_commit):
    """Propagate any changes that happened on a non-trunk default
    branch to the trunk of the repository.  See
    CVSCommit._post_commit() for details on why this is necessary."""
    for cvs_rev in svn_commit.cvs_revs:
      if cvs_rev.op == OP_ADD or cvs_rev.op == OP_CHANGE:
        if self.path_exists(cvs_rev.svn_trunk_path):
          # Delete the path on trunk...
          self.delete_path(cvs_rev.svn_trunk_path)
        # ...and copy over from branch
        self.copy_path(cvs_rev.svn_path, cvs_rev.svn_trunk_path,
                       svn_commit.revnum)
      elif cvs_rev.op == OP_DELETE:
        # delete trunk path
        self.delete_path(cvs_rev.svn_trunk_path)
      else:
        msg = ("Unknown CVSRevision operation '%s' in default branch sync."
               % cvs_rev.op)
        raise SVNRepositoryMirrorUnexpectedOperationError, msg

  ###TODO We need to fix our code to allow trunk/branches/tags (wishlist)
  def _dest_path_for_source_path(self, symbolic_name, path):
    """Given source path PATH, returns the copy destination path under
    SYMBOLIC_NAME.

    For example, if PATH is 'trunk', and SYMBOLIC_NAME 'mytag' is a
    tag, then we will return 'tags/mytag'.

    However, branches are treated slightly differently, for example,
    if PATH is 'branches/mybranch', and SYMBOLIC_NAME 'mytag' is a
    tag, then we will *still* return 'tags/mytag'.

    This function's behavior is undefined if any of
    self._ctx.[branches_base|trunk_base] is more than one
    path element long."""
    base_dest_path = self._ctx.branches_base
    if self.tags_db.has_key(symbolic_name):
      base_dest_path = self._ctx.tags_base

    components = path.split('/')
    if components[0] == self._ctx.branches_base:
      components = components[1:]
    dest = base_dest_path + '/' + symbolic_name 
    if len(components) > 1:
      dest = dest + '/' + '/'.join(components[1:])
    return dest

  def _fill(self, symbol_fill, key, name,
            parent_path_so_far=None, preferred_revnum=None):
    """Descends through all nodes in SYMBOL_FILL.node_tree that are
    rooted at KEY, which is a string key into SYMBOL_FILL.node_tree.
    Generates copy (and delete) commands for all destination nodes
    that don't exist in NAME.

    PARENT_PATH_SO_FAR is the parent directory of the path(s) that may
    be copied in this invocation of the method.  If None, that means
    that our source path starts from the root of the repository.

    PREFERRED_REVNUM is an int which is the source revision number
    that the caller (who may have copied KEY's parent) used to
    perform its copy.  If PREFERRED_REVNUM is None, then no revision
    is preferable to any other (which probably means that no copies
    have happened yet).

    PARENT_PATH_SO_FAR and PREFERRED_REVNUM should only be passed in
    by recursive calls."""

    parent_key = key
    for entry, key in symbol_fill.node_tree[parent_key].items():

      if entry[0] == '/': #Skip flags
        continue
      if parent_path_so_far is not None: # Avoid a leading slash
        src_path_so_far = parent_path_so_far + '/' + entry
      else:
        if entry == self._ctx.branches_base:
          self._fill(symbol_fill, key, name, entry, preferred_revnum)
          continue
        else:
          src_path_so_far = entry

      dest_path = self._dest_path_for_source_path(name, src_path_so_far)
      src_revnum = None
      # if our destination path doesn't already exist, then we may
      # have to make a copy.
      if (dest_path is not None and not self.path_exists(dest_path)):
        src_revnum = symbol_fill.get_best_revnum(key, preferred_revnum)

        # If the revnum of our parent's copy (src_revnum) is the same
        # as our preferred_revnum, then we don't need to make a copy
        # here--our parent's copy already has everything we need.
        # Just continue bubbling down.
        if src_revnum != preferred_revnum:
          # Do the copy
          new_entries = self.copy_path(src_path_so_far, dest_path, src_revnum)
          # Delete invalid entries that got swept in by the copy.
          valid_entries = symbol_fill.node_tree[key]
          bad_entries = self._get_invalid_entries(valid_entries, new_entries)
          
          for entry in bad_entries:
            del_path = dest_path + '/' + entry
            self.delete_path(del_path)

      self._fill(symbol_fill, key, name, src_path_so_far, src_revnum)

  def _get_invalid_entries(self, valid_entries, all_entries):
    """Return a list of keys in ALL_ENTRIES that do not occur in
    VALID_ENTRIES.  Ignore any key that begins with '/'."""
    bogons = [ ]
    for key in all_entries.keys():
      if key[0] == '/':
        continue
      if not valid_entries.has_key(key):
        bogons.append(key)
    return bogons

  def _open_path(self, path):
    """Open a chain of mutable nodes for PATH from the youngest
    revision.  Any nodes in the chain that are already mutable will be
    used as-is.  Immutable nodes will be copied, inserted in the
    nodes_db, and attached to their mutable parents.

    Returns a tuple consisting of the final node's key and its
    contents."""
    parent_node_key, parent_node_contents = self._get_youngest_root_node()

    components = path.split('/')
    last_component = components[-1]
    ###TODO Do something more constructive than die if someone asks
    ###for a node that doesn't exist
    for component in components:
     this_node_key = parent_node_contents[component]
     this_node_contents = self.nodes_db[this_node_key]
     mutable = this_node_contents.get(self.mutable_flag)
     if not mutable:
       this_node_key, this_node_contents \
                      = self._new_mutable_node(this_node_contents)
       parent_node_contents[component] = this_node_key
       self.nodes_db[parent_node_key] = parent_node_contents

     parent_node_key = this_node_key
     parent_node_contents = this_node_contents

    return this_node_key, this_node_contents

  def copy_path(self, src_path, dest_path, src_revnum):
    """Copy SRC_PATH at subversion revision number SRC_REVNUM to
    DEST_PATH.

    In the youngest revision of the repository, DEST_PATH's parent
    *must* exist, but DEST_PATH *cannot* exist.

    Return the contents of the new node at DEST_PATH as a dictionary.
    """
    # get the contents of the node of our src_path
    ign, src_node_contents = self._node_for_path(src_path, src_revnum)
    # get the dest node from self.youngest--it will always be mutable.

    # Get the parent path and the base path of the dest_path
    dest_components = dest_path.split('/')
    dest_parent = '/'.join(dest_components[:-1])
    dest_basename = dest_components[-1]

    # Get a mutable node for our destination parent dir.
    dest_node_key, dest_node_contents = self._open_path(dest_parent)
    
    if dest_node_contents.has_key(dest_basename):
      msg = "Attempt to add path '%s' to repository mirror " % dest_path
      msg = msg + "when it already exists in the mirror."
      raise SVNRepositoryMirrorPathExistsError, msg

    # Generate new mutable node new_node from SRC_NODE_CONTENTS and
    # update DEST_NODE_CONTENTS with
    #
    #    {COMPONENT_NAME : new_node}
    #
    #and save DEST_NODE_CONTENTS back to the nodes_db under
    #DEST_NODE_KEY.
    key, new_node = self._new_mutable_node(src_node_contents)
    dest_node_contents[dest_basename] = key
    self.nodes_db[dest_node_key] = dest_node_contents
    self.delegate.copy_path(src_path, dest_path, src_revnum)
    return new_node

  def _node_for_path(self, path, revnum, ignore_leaf=None):
    """Locates the node key in the filesystem for the last element of
    PATH under the subversion revision REVNUM.

    If IGNORE_LEAF is true, then instead of returning the leaf node,
    return its parent.

    This method should never be called with a PATH that isn't in the
    repository.

    Returns a tuple consisting of the nodes_db key and its
    contents."""
    node_key, node_contents = self._get_root_node_for_revnum(revnum)

    components = path.split('/')
    last_component = components[-1]
    ###TODO Do something more constructive than die if someone asks
    ###for a node that doesn't exist
    for component in components:
      if component is last_component and ignore_leaf:
        break
      node_key = node_contents[component]
      node_contents = self.nodes_db[node_key]

    return node_key, node_contents





  ###TODO This *might* be a bit pricey to do.  Look here for perf
  ###problems.
  def path_exists(self, path):
    """If PATH exists in self.youngest of the svn repository mirror,
    return true, else return None.
    
    PATH must not start with '/'."""
    #print "     TON: PROBING path: '%s' in %d" % (path, self.youngest)
    parent_node_key, parent_node_contents = self._get_youngest_root_node()
    previous_component = "/"

    components = string.split(path, '/')
    for component in components:
      if not parent_node_contents.has_key(component):
        return None

      this_entry_key = parent_node_contents[component]
      this_entry_val = self.nodes_db[this_entry_key]
      parent_node_key = this_entry_key
      parent_node_contents = this_entry_val
      previous_component = component
    return 1

  def commit(self, svn_commit):
    """Add an SVNCommit to the SVNRepository, incrementing the
    Repository revision number, and changing the repository.
    Invoke the delegate's start_commit() method, if there is a delegate."""

    ###TODO Make a decision about whether or not the self.methods
    ###called here are going to be public or not.  If they're going to
    ###remain private, then prefix them with an underscore.
    self.start_commit(svn_commit.revnum)
    self.delegate.start_commit(svn_commit)

    # Create tags and branches in the first commit
    ###TODO don't do this if we're trunk_only
    ###
    ###TODO: We MUST do this in a separate commit, so it's
    ### not mixed in with a user change.
    if not self.active:
      self.mkdir(self._ctx.branches_base)
      self.mkdir(self._ctx.tags_base)
      self.active = 1

    if svn_commit.symbolic_name:
      self.fill_symbolic_name(svn_commit.symbolic_name)
    elif svn_commit.motivating_revnum:
      print "COM: INACTIVE Default branch copies for motivating commit"
      svn_commit.motivating_revnum, "in SVNCommit", svn_commit.revnum
      self.synchronize_default_branch(svn_commit)
    else: # This actually commits CVSRevisions
      for cvs_rev in svn_commit.cvs_revs:
        # See comment in CVSCommit._commit() for what this is all
        # about.  Note that although asking self.path_exists() is
        # somewhat expensive, we only do it if the first two (cheap)
        # tests succeed first.
        if not ((cvs_rev.deltatext_code == DELTATEXT_EMPTY)
                and (cvs_rev.rev == "1.1.1.1")
                and self.path_exists(cvs_rev.svn_path)):
          if cvs_rev.op == OP_ADD:
            self.add_path(cvs_rev)
          elif cvs_rev.op == OP_CHANGE:
            self.change_path(cvs_rev)
          else: # Must be a delete
            path = self.delete_path(cvs_rev.svn_path)

  def close(self):
    self.delegate.finish()
    self.revs_db = None
    self.nodes_db = None



class SVNRepositoryMirrorDelegate:
  """Abstract superclass for any delegate to SVNRepositoryMirror.
  Subclasses must implement all of the methods below, and callers
  must call set_mirror before calling any other method.

  For each method, a subclass implements, in its own way, the
  Subversion operation implied by the method's name.  For example, for
  the add_path method, the DumpfileDelegate would write out a
  "Node-add:" command to a Subversion dumpfile, the StdoutDelegate
  would merely print that the path is being added to the repository,
  and the RepositoryDelegate would actually cause the path to be added
  to the Subversion repository that it is creating.
  """

  def set_mirror(self, mirror):
    """Set the SVNRepositoryMirror for this instance."""
    self.mirror = mirror

  def start_commit(self, svn_commit):
    """Perform any actions needed to start SVNCommit SVN_COMMIT;
    see subclass implementation for details."""
    raise NotImplementedError

  def mkdir(self, path):
    """PATH is a string; see subclass implementation for details."""
    raise NotImplementedError

  def add_path(self, c_rev):
    """C_REV is a CVSRevision; see subclass implementation for
    details."""
    raise NotImplementedError

  def change_path(self, c_rev):
    """C_REV is a CVSRevision; see subclass implementation for
    details."""
    raise NotImplementedError

  def delete_path(self, path):
    """PATH is a string; see subclass implementation for
    details."""
    raise NotImplementedError
  
  def copy_path(self, src_path, dest_path, src_revnum):
    """SRC_PATH and DEST_PATH are both strings, and SRC_REVNUM is a
    subversion revision number (int); see subclass implementation for
    details."""
    raise NotImplementedError
  
  def finish(self):
    """Perform any cleanup necessary after all revisions have been
    committed."""
    raise NotImplementedError

class RepositoryDelegate(SVNRepositoryMirrorDelegate):
  """Creates a new Subversion Repository."""
  pass

class DumpfileDelegate(SVNRepositoryMirrorDelegate):
  """Create a Subversion dumpfile."""

  def __init__(self, ctx):
    """Return a new DumpfileDelegate instance, attached to a dumpfile
    named according to CTX.dumpfile, using CTX.encoding.

    If CTX.cvs_revnums is true, then set the 'cvs2svn:cvs-revnum'
    property on files, when they are changed due to a corresponding
    CVS revision.

    If CTX.mime_mapper is true, then it is a MimeMapper instance, used
    to determine whether or not to set the 'svn:mime-type' property on
    files.

    If CTX.set_eol_style is true, then set 'svn:eol-style' to 'native'
    for files not marked with the CVS 'kb' flag.  (But see issue #39
    for how this might change.)""" 
    ###TODO kff: Remove this debugging "kff" shim.
    self.dumpfile_path = "kff-" + ctx.dumpfile
    self.set_cvs_revnum_properties = ctx.cvs_revnums
    self.set_eol_style = ctx.set_eol_style
    self.mime_mapper = ctx.mime_mapper
    self.path_encoding = ctx.encoding
    self.mirror = None  # Use self.set_mirror() to initialize this.
    self.revision = 0
    
    self.dumpfile = open(self.dumpfile_path, 'wb')
    self.write_dumpfile_header()

  def write_dumpfile_header(self):
    # Initialize the dumpfile with the standard headers.
    #
    # Since the CVS repository doesn't have a UUID, and the Subversion
    # repository will be created with one anyway, we don't specify a
    # UUID in the dumpflie
    self.dumpfile.write('SVN-fs-dump-format-version: 2\n\n')

  def _utf8_path(self, path):
    """Return a copy of PATH encoded in UTF-8.  PATH is assumed to be
    encoded in self.path_encoding."""
    try:
      # Log messages can be converted with the 'replace' strategy,
      # but we can't afford any lossiness here.
      unicode_path = unicode(path, self.path_encoding, 'strict')
      return unicode_path.encode('utf-8')
    except UnicodeError:
      print "Unable to convert a path '%s' to internal encoding." % path
      print "Consider rerunning with (for example) '--encoding=latin1'"
      sys.exit(1)

  def start_commit(self, svn_commit):
    """Emit the start of SVN_COMMIT (an SVNCommit)."""

    self.revision = svn_commit.revnum

    # The start of a new commit typically looks like this:
    # 
    #   Revision-number: 1
    #   Prop-content-length: 129
    #   Content-length: 129
    #   
    #   K 7
    #   svn:log
    #   V 27
    #   Log message for revision 1.
    #   K 10
    #   svn:author
    #   V 7
    #   jrandom
    #   K 8
    #   svn:date
    #   V 27
    #   2003-04-22T22:57:58.132837Z
    #   PROPS-END
    #
    # Notice that the length headers count everything -- not just the
    # length of the data but also the lengths of the lengths, including
    # the 'K ' or 'V ' prefixes.
    #
    # The reason there are both Prop-content-length and Content-length
    # is that the former includes just props, while the latter includes
    # everything.  That's the generic header form for any entity in a
    # dumpfile.  But since revisions only have props, the two lengths
    # are always the same for revisions.
    
    # Calculate the total length of the props section.
    props = svn_commit.get_revprops()
    total_len = 10  # len('PROPS-END\n')
    for propname in props.keys():
      klen = len(propname)
      klen_len = len('K %d' % klen)
      vlen = len(props[propname])
      vlen_len = len('V %d' % vlen)
      # + 4 for the four newlines within a given property's section
      total_len = total_len + klen + klen_len + vlen + vlen_len + 4
        
    # Print the revision header and props
    self.dumpfile.write('Revision-number: %d\n'
                        'Prop-content-length: %d\n'
                        'Content-length: %d\n'
                        '\n'
                        % (self.revision, total_len, total_len))

    for propname in props.keys():
      self.dumpfile.write('K %d\n' 
                          '%s\n' 
                          'V %d\n' 
                          '%s\n' % (len(propname),
                                    propname,
                                    len(props[propname]),
                                    props[propname]))

    self.dumpfile.write('PROPS-END\n')
    self.dumpfile.write('\n')

  def mkdir(self, path):
    """Emit the creation of directory PATH."""
    self.dumpfile.write("Node-path: %s\n" 
                        "Node-kind: dir\n"
                        "Node-action: add\n"
                        "Prop-content-length: 10\n"
                        "Content-length: 10\n"
                        "\n"
                        "PROPS-END\n"
                        "\n"
                        "\n" % self._utf8_path(path))

  ###TODO Move this to CVSRevision
  def _get_cvs_path(self, c_rev):
    """Return the path and executable status of the rcs file for C_REV.
    Use like this:     path, exec_status = self._get_cvs_path(C_REV)
    If the rcs file is executable, the exec_status is 1, else it is None."""
    try:
      rcs_path = c_rev.fname
      f_st = os.stat(rcs_path)
    except os.error:
      dirname, fname = os.path.split(rcs_path)
      rcs_path = os.path.join(dirname, 'Attic', fname)
      f_st = os.stat(rcs_path)
    # One of the above should have worked.  Now see about exec status.
    if f_st[0] & stat.S_IXUSR:
      return rcs_path, 1
    else:
      return rcs_path, None

  def _add_or_change_path(self, c_rev, op):
    """Emit the addition or change corresponding to C_REV.
    OP is either the constant OP_ADD or OP_CHANGE."""

    rcs_path, exec_status = self._get_cvs_path(c_rev)

    # We begin with only a "CVS revision" property.
    if self.set_cvs_revnum_properties:
      prop_contents = 'K 15\ncvs2svn:cvs-rev\nV %d\n%s\n' \
                      % (len(c_rev.rev), c_rev.rev)
    else:
      prop_contents = ''
    
    # Tack on the executableness, if any.
    if exec_status:
      prop_contents = prop_contents + 'K 14\nsvn:executable\nV 1\n*\n'

    # If the file is marked as binary, it gets a default MIME type of
    # "application/octet-stream".  Otherwise, it gets a default EOL
    # style of "native".
    mime_type = None
    eol_style = None
    if c_rev.mode == 'b':
      mime_type = 'application/octet-stream'
    else:
      eol_style = 'native'

    ### TODO: Deal with other expansion modes.  What do we do about
    ### the various keyword expansion styles (name only, value only,
    ### both name and value, old value, etc.)?  Beats me.
    
    # If using the MIME mapper, possibly override the default MIME
    # type and EOL style.
    if self.mime_mapper:
      mtype = self.mime_mapper.get_type_from_filename(c_rev.cvs_path)
      if mtype:
        mime_type = mtype
        if not mime_type.startswith("text/"):
          eol_style = None

    # Possibly set the svn:mime-type and svn:eol-style properties.
    if mime_type:
      prop_contents = prop_contents + ('K 13\nsvn:mime-type\nV %d\n%s\n' % \
                                       (len(mime_type), mime_type))
    if self.set_eol_style and eol_style:
      prop_contents = prop_contents + 'K 13\nsvn:eol-style\nV 6\nnative\n'
                                         
    # Calculate the property length (+10 for "PROPS-END\n")
    props_len = len(prop_contents) + 10
    
    ### FIXME: We ought to notice the -kb flag set on the RCS file and
    ### use it to set svn:mime-type.  See issue #39.

    basename = os.path.basename(rcs_path[:-2])
    pipe_cmd = 'co -q -x,v -p%s %s' % (c_rev.rev, escape_shell_arg(rcs_path))
    pipe = os.popen(pipe_cmd, PIPE_READ_MODE)

    if op == OP_ADD:
      action = 'add'
    elif op == OP_CHANGE:
      action = 'change'
    else:
      sys.stderr.write("%s: _add_or_change_path() called with bad op ('%s')"
                       % (error_prefix, op))
      sys.exit(1)

    self.dumpfile.write('Node-path: %s\n'
                        'Node-kind: file\n'
                        'Node-action: %s\n'
                        'Prop-content-length: %d\n'
                        'Text-content-length: '
                        % (self._utf8_path(c_rev.svn_path),
                           action, props_len))

    pos = self.dumpfile.tell()

    self.dumpfile.write('0000000000000000\n'
                        'Text-content-md5: 00000000000000000000000000000000\n'
                        'Content-length: 0000000000000000\n'
                        '\n')

    self.dumpfile.write(prop_contents + 'PROPS-END\n')

    # Insert the rev contents, calculating length and checksum as we go.
    checksum = md5.new()
    length = 0
    buf = pipe.read()
    while buf:
      checksum.update(buf)
      length = length + len(buf)
      self.dumpfile.write(buf)
      buf = pipe.read()
    if pipe.close() is not None:
      sys.exit('%s: Command failed: "%s"' % (error_prefix, pipe_cmd))

    # Go back to patch up the length and checksum headers:
    self.dumpfile.seek(pos, 0)
    # We left 16 zeros for the text length; replace them with the real
    # length, padded on the left with spaces:
    self.dumpfile.write('%16d' % length)
    # 16... + 1 newline + len('Text-content-md5: ') == 35
    self.dumpfile.seek(pos + 35, 0)
    self.dumpfile.write(checksum.hexdigest())
    # 35... + 32 bytes of checksum + 1 newline + len('Content-length: ') == 84
    self.dumpfile.seek(pos + 84, 0)
    # The content length is the length of property data, text data,
    # and any metadata around/inside around them.
    self.dumpfile.write('%16d' % (length + props_len))
    # Jump back to the end of the stream
    self.dumpfile.seek(0, 2)

    # This record is done (write two newlines -- one to terminate
    # contents that weren't themselves newline-termination, one to
    # provide a blank line for readability.
    self.dumpfile.write('\n\n')

  def add_path(self, c_rev):
    """Emit the addition corresponding to C_REV, a CVSRevision."""
    self._add_or_change_path(c_rev, OP_ADD)

  def change_path(self, c_rev):
    """Emit the change corresponding to C_REV, a CVSRevision."""
    self._add_or_change_path(c_rev, OP_CHANGE)

  def delete_path(self, path):
    """Emit the deletion of PATH."""
    ###TODO kff: This just needs to take SVN_PATH, really.
    ###
    ###TODO kff: Who is responsible for pruning behavior?  The caller,
    ### I think, but we should make sure that's what we want.
    self.dumpfile.write('Node-path: %s\n'
                        'Node-action: delete\n'
                        '\n' % self._utf8_path(path))
  
  def copy_path(self, src_path, dest_path, src_revnum):
    """Emit the copying of SRC_PATH at SRC_REV to DEST_PATH."""
    ###TODO kff: We need to settle whether callers are responsible for
    ### passing UTF8 paths, or we are responsible for converting.
    ### Right now, we convert, see self._utf8_path().  That might be
    ### fine, just investigate to make sure.
    ###
    ###TODO kff: Note how the original Dumper.copy_path() took an
    ### 'entries' arg.  Have we completely avoided the need for that
    ### now?  That would certainly be nice.

    # We don't need to include "Node-kind:" for copies; the loader
    # ignores it anyway and just uses the source kind instead.
    self.dumpfile.write('Node-path: %s\n'
                        'Node-action: add\n'
                        'Node-copyfrom-rev: %d\n'
                        'Node-copyfrom-path: /%s\n'
                        '\n'
                        % (self._utf8_path(dest_path),
                           src_revnum,
                           self._utf8_path(src_path)))
  
  def finish(self):
    """Perform any cleanup necessary after all revisions have been
    committed."""
    self.dumpfile.close()


class StdoutDelegate(SVNRepositoryMirrorDelegate):
  """Makes no changes to the disk, but writes out information to
  STDOUT about what the SVNRepositoryMirror is doing.  Of course, our
  print statements will state that we're doing something, when in
  reality, we aren't doing anything other than printing out that we're
  doing something.  Kind of zen, really."""
  def __init__(self):
    print "Starting Subversion repository."

  def start_commit(self, svn_commit):
    """Prints out the Subversion revision number of the commit that is
    being started."""
    print " ", "=" * 40
    print "  Starting Subversion commit", svn_commit.revnum

  def mkdir(self, path):
    """Print a line stating that we are creating directory PATH."""
    print "    New Directory", path

  def add_path(self, c_rev):
    """Print a line stating that we are 'adding' c_rev.svn_path."""
    print "    Adding", c_rev.svn_path

  def change_path(self, c_rev):
    """Print a line stating that we are 'changing' c_rev.svn_path."""
    print "    Changing", c_rev.svn_path

  def delete_path(self, path):
    """Print a line stating that we are 'deleting' PATH."""
    print "    Deleting", path
  
  def copy_path(self, src_path, dest_path, src_revnum):
    """Print a line stating that we are 'copying' revision SRC_REVNUM
    of SRC_PATH to DEST_PATH."""
    print "    Copying revision", src_revnum, "of", src_path
    print "                  to", dest_path
  
  def finish(self):
    """State that we are done creating our repository."""
    print "Finished creating Subversion repository."


  

def pass8(ctx):

  svncounter = 1

  # kff: toggle between these two lines to test the DumpfileDelegate or not
  #repos = SVNRepositoryMirror(ctx, StdoutDelegate())
  repos = SVNRepositoryMirror(ctx, DumpfileDelegate(ctx))

  while(1):
    svn_commit = CommitMapper(ctx).get_svn_commit(svncounter)
    if not svn_commit:
      break
    #print svn_commit

    repos.commit(svn_commit)

    svncounter += 1
  repos.close()
#  sys.exit(239)





  if ctx.trunk_only:
    sym_tracker = DummySymbolicNameTracker()
  else:
    sym_tracker = SymbolicNameTracker()
    tags_db = Database(TAGS_DB, 'r')
    symbol_revs_db = Database(SYMBOL_LAST_CVS_REVS_DB, 'r')
  metadata_db = Database(METADATA_DB, 'r')

  # A dictionary of Commit objects, keyed by digest.  Each object
  # represents one logical commit, which may involve multiple files.
  #
  # The reason this is a dictionary, not a single object, is that
  # there may be multiple commits interleaved in time.  A commit can
  # span up to COMMIT_THRESHOLD seconds, which leaves plenty of time
  # for parts of some other commit to occur.  Since the s-revs file is
  # sorted by timestamp first, then by digest within each timestamp,
  # it's quite easy to have interleaved commits.
  commits = { }

  # The total number of separate commits processed.  This is used only for
  # printing statistics, it does not affect the results in the repository.
  count = 0

  # Start the dumpfile object.
  dumper = Dumper(ctx)

  pending_symbols = {}
  # process the logfiles, creating the target
  for line in fileinput.FileInput(ctx.log_fname_base + SORTED_REVS_SUFFIX):
    c_rev = CVSRevision(ctx, line)

    ###TODO WARNING: This may leave unclosed commits in the queue!
    if ctx.trunk_only and not trunk_rev.match(c_rev.rev):
      ### note this could/should have caused a flush, but the next item
      ### will take care of that for us
      continue

    # Each time we read a new line, we scan the commits we've
    # accumulated so far to see if any are ready for processing now.
    process = [ ]
    for scan_id, scan_c in commits.items():
      if scan_c.t_max + COMMIT_THRESHOLD < c_rev.timestamp:
        process.append(scan_c)
        del commits[scan_id]
        continue
      # If the inbound commit is on the same file as a pending commit,
      # close the pending commit to further changes. Don't flush it though,
      # as there may be other pending commits dated before this one.
      # ### ISSUE: the has_file() check below is not optimal.
      # It does fix the dataloss bug where revisions would get lost
      # if checked in too quickly, but it can also break apart the
      # commits. The correct fix would require tracking the dependencies
      # between change sets and committing them in proper order.
      if scan_c.has_file(c_rev.fname):
        unused_id = scan_id + '-'
        while commits.has_key(unused_id):
          unused_id = unused_id + '-'
        commits[unused_id] = scan_c
        del commits[scan_id]

    # If there are any elements in 'process' at this point, they need
    # to be committed, because this latest rev couldn't possibly be
    # part of any of them.  Sort them into time-order, then commit 'em.
    process.sort()
    for c in process:
      c.commit(dumper, ctx, sym_tracker)
    count = count + len(process)

    # Add this item into the set of still-available commits.
    if commits.has_key(c_rev.digest):
      c = commits[c_rev.digest]
    else:
      author, log = metadata_db[c_rev.digest]
      c = commits[c_rev.digest] = Commit(author, log)
    c.add(c_rev)

    if ctx.trunk_only:
      continue
    #####################################################################
    for sym in symbol_revs_db.get(c_rev.unique_key(), []):
      pending_symbols[sym] = None

    open_symbols = {}
    for sym in pending_symbols.keys():
      for k, v in commits.items():
        if v.contains_symbolic_name(sym):
          open_symbols[sym] = None
          break

    sorted_pending_symbols_keys = pending_symbols.keys()
    sorted_pending_symbols_keys.sort()
    for sym in sorted_pending_symbols_keys:
      if open_symbols.has_key(sym): # sym is still open--don't close it now.
        continue
      if tags_db.has_key(sym):
        sym_tracker.fill_tag(dumper, ctx, sym, [1])
      else:
        sym_tracker.fill_branch(dumper, ctx, sym, [1])
      sym_tracker.cleanup_symbol(sym)
      del pending_symbols[sym]
    #####################################################################

  # End of the sorted revs file.  Flush any remaining commits:
  if commits:
    process = commits.values()
    process.sort()
    for c in process:
      c.commit(dumper, ctx, sym_tracker)
    count = count + len(process)

  # Create (or complete) any branches and tags not already done.
  sym_tracker.finish(dumper, ctx)

  dumper.close()

  if ctx.verbose:
    print count, 'commits processed.'

_passes = [
  pass1,
  pass2,
  pass3,
  pass4,
  pass5,
  pass6,
  pass7,
  pass8,
  ]


class _ctx:
  pass


class MimeMapper:
  "A class that provides mappings from file names to MIME types."

  def __init__(self):
    self.mappings = { }
    self.missing_mappings = { }


  def set_mime_types_file(self, mime_types_file):
    for line in fileinput.input(mime_types_file):
      if line.startswith("#"):
        continue

      # format of a line is something like
      # text/plain c h cpp
      extensions = line.split()
      if len(extensions) < 2:
        continue
      type = extensions.pop(0)
      for ext in extensions:
        if self.mappings.has_key(ext) and self.mappings[ext] != type:
          sys.stderr.write("%s: ambiguous MIME mapping for *.%s (%s or %s)\n" \
                           % (warning_prefix, ext, self.mappings[ext], type))
        self.mappings[ext] = type


  def get_type_from_filename(self, filename):
    basename, extension = os.path.splitext(os.path.basename(filename))

    # Extension includes the dot, so strip it (will leave extension
    # empty if filename ends with a dot, which is ok):
    extension = extension[1:]

    # If there is no extension (or the file ends with a period), use
    # the base name for mapping. This allows us to set mappings for
    # files such as README or Makefile:
    if not extension:
      extension = basename
    if self.mappings.has_key(extension):
      return self.mappings[extension]
    self.missing_mappings[extension] = 1
    return None


  def print_missing_mappings(self):
    for ext in self.missing_mappings:
      sys.stderr.write("%s: no MIME mapping for *.%s\n" % (warning_prefix, ext))


def convert(ctx, start_pass=1):
  "Convert a CVS repository to an SVN repository."

  if not os.path.exists(ctx.cvsroot):
    sys.stderr.write(error_prefix + ': \'%s\' does not exist.\n' % ctx.cvsroot)
    sys.exit(1)

  cleanup = Cleanup()
  times = [ None ] * len(_passes)
  for i in range(start_pass - 1, len(_passes)):
    times[i] = time.time()
    print '----- pass %d -----' % (i + 1)
    _passes[i](ctx)
    if not ctx.skip_cleanup:
      cleanup.cleanup(_passes[i])
  times.append(time.time())

  for i in range(start_pass, len(_passes)+1):
    print 'pass %d: %d seconds' % (i, int(times[i] - times[i-1]))
  print ' total:', int(times[len(_passes)] - times[start_pass-1]), 'seconds'


def usage(ctx):
  print 'USAGE: %s [-v] [-s svn-repos-path] [-p pass] cvs-repos-path' \
        % os.path.basename(sys.argv[0])
  print '  --help, -h           print this usage message and exit with success'
  print '  -v                   verbose'
  print '  -s PATH              path for SVN repos'
  print '  -p NUM               start at pass NUM of %d' % len(_passes)
  print '  --existing-svnrepos  load into existing SVN repository'
  print '  --dumpfile=PATH      name of intermediate svn dumpfile'
  print '  --svnadmin=PATH      path to the svnadmin program'
  print '  --trunk-only         convert only trunk commits, not tags nor branches'
  print '  --trunk=PATH         path for trunk (default: %s)'    \
        % ctx.trunk_base
  print '  --branches=PATH      path for branches (default: %s)' \
        % ctx.branches_base
  print '  --tags=PATH          path for tags (default: %s)'     \
        % ctx.tags_base
  print '  --no-prune           don\'t prune empty directories'
  print '  --dump-only          just produce a dumpfile, don\'t commit to a repos'
  print '  --encoding=ENC       encoding of log messages in CVS repos (default: %s)' \
        % ctx.encoding
  print '  --force-branch=NAME  Force NAME to be a branch.'
  print '  --force-tag=NAME     Force NAME to be a tag.'
  print '  --username=NAME      username for cvs2svn-synthesized commits'
  print '                                                  (default: %s)' \
        % ctx.username
  print '  --skip-cleanup       prevent the deletion of intermediate files'
  print '  --bdb-txn-nosync     pass --bdb-txn-nosync to "svnadmin create"'
  print '  --cvs-revnums        record CVS revision numbers as file properties'
  print '  --mime-types=FILE    specify an apache-style mime.types file for\n' \
        '                       setting svn:mime-type'
  print '  --set-eol-style      automatically set svn:eol-style=native for\n' \
        '                       text files'


def main():
  # prepare the operation context
  ctx = _ctx()
  ctx.cvsroot = None
  ctx.target = None
  ctx.log_fname_base = DATAFILE
  ctx.dumpfile = DUMPFILE
  ctx.verbose = 0
  ctx.prune = 1
  ctx.existing_svnrepos = 0
  ctx.dump_only = 0
  ctx.trunk_only = 0
  ctx.trunk_base = "trunk"
  ctx.tags_base = "tags"
  ctx.branches_base = "branches"
  ctx.encoding = "ascii"
  ctx.mime_types_file = None
  ctx.mime_mapper = None
  ctx.set_eol_style = 0
  ctx.svnadmin = "svnadmin"
  ctx.username = "unknown"
  ctx.print_help = 0
  ctx.skip_cleanup = 0
  ctx.cvs_revnums = 0
  ctx.bdb_txn_nosync = 0
  ctx.forced_branches = []
  ctx.forced_tags = []

  start_pass = 1

  try:
    opts, args = getopt.getopt(sys.argv[1:], 'p:s:vh',
                               [ "help", "create", "trunk=",
                                 "username=", "existing-svnrepos",
                                 "branches=", "tags=", "encoding=",
                                 "force-branch=", "force-tag=",
                                 "mime-types=", "set-eol-style",
                                 "trunk-only", "no-prune",
                                 "dump-only", "dumpfile=", "svnadmin=",
                                 "skip-cleanup", "cvs-revnums",
                                 "bdb-txn-nosync"])
  except getopt.GetoptError, e:
    sys.stderr.write(error_prefix + ': ' + str(e) + '\n\n')
    usage(ctx)
    sys.exit(1)

  for opt, value in opts:
    if opt == '-p':
      start_pass = int(value)
      if start_pass < 1 or start_pass > len(_passes):
        print '%s: illegal value (%d) for starting pass. ' \
              'must be 1 through %d.' % (error_prefix, start_pass,
                                         len(_passes))
        sys.exit(1)
    elif (opt == '--help') or (opt == '-h'):
      ctx.print_help = 1
    elif opt == '-v':
      ctx.verbose = 1
    elif opt == '-s':
      ctx.target = value
    elif opt == '--existing-svnrepos':
      ctx.existing_svnrepos = 1
    elif opt == '--dumpfile':
      ctx.dumpfile = value
    elif opt == '--svnadmin':
      ctx.svnadmin = value
    elif opt == '--trunk-only':
      ctx.trunk_only = 1
    elif opt == '--trunk':
      ctx.trunk_base = value
    elif opt == '--branches':
      ctx.branches_base = value
    elif opt == '--tags':
      ctx.tags_base = value
    elif opt == '--no-prune':
      ctx.prune = None
    elif opt == '--dump-only':
      ctx.dump_only = 1
    elif opt == '--encoding':
      ctx.encoding = value
    elif opt == '--force-branch':
      ctx.forced_branches.append(value)
    elif opt == '--force-tag':
      ctx.forced_tags.append(value)
    elif opt == '--mime-types':
      ctx.mime_types_file = value
    elif opt == '--set-eol-style':
      ctx.set_eol_style = 1
    elif opt == '--username':
      ctx.username = value
    elif opt == '--skip-cleanup':
      ctx.skip_cleanup = 1
    elif opt == '--cvs-revnums':
      ctx.cvs_revnums = 1
    elif opt == '--bdb-txn-nosync':
      ctx.bdb_txn_nosync = 1
    elif opt == '--create':
      sys.stderr.write(warning_prefix +
          ': The behaviour produced by the --create option is now the '
          'default,\nand passing the option is deprecated.\n')
      
  if ctx.print_help:
    usage(ctx)
    sys.exit(0)

  # Consistency check for options and arguments.
  if len(args) == 0:
    usage(ctx)
    sys.exit(1)

  if len(args) > 1:
    sys.stderr.write(error_prefix +
                     ": must pass only one CVS repository.\n")
    usage(ctx)
    sys.exit(1)

  ctx.cvsroot = args[0]

  if not os.path.isdir(ctx.cvsroot):
    sys.stderr.write(error_prefix +
                     ": the cvs-repos-path '%s' is not an "
                     "existing directory.\n" % ctx.cvsroot)
    sys.exit(1)

  if (not ctx.target) and (not ctx.dump_only):
    sys.stderr.write(error_prefix +
                     ": must pass one of '-s' or '--dump-only'.\n")
    sys.exit(1)

  def not_both(opt1val, opt1name, opt2val, opt2name):
    if opt1val and opt2val:
      sys.stderr.write(error_prefix + ": cannot pass both '%s' and '%s'.\n" \
          % (opt1name, opt2name))

  not_both(ctx.target, '-s', ctx.dump_only, '--dump-only')

  not_both(ctx.dump_only, '--dump-only',
    ctx.existing_svnrepos, '--existing-svnrepos')

  not_both(ctx.bdb_txn_nosync, '--bdb-txn-nosync',
    ctx.existing_svnrepos, '--existing-svnrepos')

  not_both(ctx.dump_only, '--dump-only',
    ctx.bdb_txn_nosync, '--bdb-txn-nosync')

  if ((string.find(ctx.trunk_base, '/') > -1)
      or (string.find(ctx.tags_base, '/') > -1)
      or (string.find(ctx.branches_base, '/') > -1)):
    sys.stderr.write("%s: cannot pass multicomponent path to "
                     "--trunk, --tags, or --branches yet.\n"
                     "  See http://cvs2svn.tigris.org/issues/show_bug.cgi?"
                     "id=7 for details.\n" % error_prefix)
    sys.exit(1)

  if ctx.existing_svnrepos and not os.path.isdir(ctx.target):
    sys.stderr.write(error_prefix +
                     ": the svn-repos-path '%s' is not an "
                     "existing directory.\n" % ctx.target)
    sys.exit(1)

  if not ctx.dump_only and not ctx.existing_svnrepos \
      and os.path.exists(ctx.target):
    sys.stderr.write(error_prefix +
                     ": the svn-repos-path '%s' exists.\nRemove it, or pass "
                     "'--existing-svnrepos'.\n" % ctx.target)
    sys.exit(1)

  if ctx.mime_types_file:
    ctx.mime_mapper = MimeMapper()
    ctx.mime_mapper.set_mime_types_file(ctx.mime_types_file)

  def clear_default_branches_db():
    # This is the only DB reference still reachable at this point;
    # lose it before removing the file.
    ctx.default_branches_db = None

  # Lock the current directory for temporary files.
  try:
    os.mkdir('cvs2svn.lock')
  except OSError:
    sys.stderr.write(error_prefix +
        ": cvs2svn writes temporary files to the current working directory.\n"
        "  The directory 'cvs2svn.lock' exists, indicating that another\n"
        "  cvs2svn process is currently using the current directory for its\n"
        "  temporary workspace. If you are certain that is not the case,\n"
        "  remove the 'cvs2svn.lock' directory.\n")
    sys.exit(1)
  try:
    ctx.default_branches_db = Database(DEFAULT_BRANCHES_DB, 'n')
    Cleanup().register(DEFAULT_BRANCHES_DB, pass8, clear_default_branches_db)
    convert(ctx, start_pass=start_pass)
  finally:
    try: os.rmdir('cvs2svn.lock')
    except: pass

  if ctx.mime_types_file:
    ctx.mime_mapper.print_missing_mappings()

if __name__ == '__main__':
  main()
