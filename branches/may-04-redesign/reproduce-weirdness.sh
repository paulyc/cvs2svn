# Reproduce a truly bizarre behavior in Python.  Search for the last
# group of "KFF" prints in the output, and the error immediately below
# them.  It will look something like this:
#
#     KFF 
#     KFF No caller passes the CVS_REVS argument, so what's going on?
#     KFF 
#     KFF REVNUM: None
#     KFF CVS_REVS: [<__main__.CVSRevision instance at 0x83bb554>, \
#                    <__main__.CVSRevision instance at 0x81b27f4>, \
#                    <__main__.CVSRevision instance at 0x8475594>]
#     Traceback (most recent call last):
#       File "./cvs2svn.py", line 4359, in ?
#         main()
#       File "./cvs2svn.py", line 4350, in main
#         convert(ctx, start_pass=start_pass)
#       File "./cvs2svn.py", line 4115, in convert
#         _passes[i](ctx)
#       File "./cvs2svn.py", line 3115, in pass5
#         aggregator.process_revision(c_rev)
#       File "./cvs2svn.py", line 3653, in process_revision
#         cvs_commit.process_revisions(self._ctx, self.done_symbols)
#       File "./cvs2svn.py", line 3511, in process_revisions
#         self._pre_commit()
#       File "./cvs2svn.py", line 3415, in _pre_commit
#         svn_commit = SVNCommit(self._ctx)
#       File "./cvs2svn.py", line 3533, in __init__
#         raise SVNCommitInternalInconsistencyError
#     __main__.SVNCommitInternalInconsistencyError
#
# What's strange about tihs is that on line 3415, we're not passing
# any CVS_REVS argument to SVNCommit().  So according to the method
# definition for SVNCommit.__init__(), that parameter should be
# initialized to [].  Yet instead it is a list of three elements, and
# of the right type too!  What is going on?

# Clean up from any previous run.
rm -rf cvs2svn-*
rm -rf repos

# Demo the bug.
./cvs2svn.py --dump-only --skip-cleanup test-data/main-cvsrepos/
