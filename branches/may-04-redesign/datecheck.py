#!/usr/bin/env python

### This is a debugging script, not required for normal cvs2svn.py usage.

'''Take a list of revisions and dates, say which are out of order.
Feed this standard input in the following format:

      r42  2003-06-02 22:20:31 -0500 (Mon, 02 Jun 2003)
      r43  2003-06-02 22:20:31 -0500 (Mon, 02 Jun 2003)
      r44  2003-06-02 23:29:14 -0500 (Mon, 02 Jun 2003)
      r45  2003-06-02 23:29:14 -0500 (Mon, 02 Jun 2003)
      r46  2003-06-02 23:33:13 -0500 (Mon, 02 Jun 2003)
      r47  2003-06-10 15:19:47 -0500 (Tue, 10 Jun 2003)
      r48  2003-06-02 23:33:13 -0500 (Mon, 02 Jun 2003)
      r49  2003-06-10 15:19:48 -0500 (Tue, 10 Jun 2003)
      r50  2003-06-02 23:33:13 -0500 (Mon, 02 Jun 2003)

The "rX"s begin at the left edge, the indentation above is just for
readability in this doc string.  The output will look like this:

      r42  1054596031      OK  (2003-06-02 22:20:31 -0500)
      r43  1054596031      OK  (2003-06-02 22:20:31 -0500)
      r44  1054600154      OK  (2003-06-02 23:29:14 -0500)
      r45  1054600154      OK  (2003-06-02 23:29:14 -0500)
      r46  1054600393      OK  (2003-06-02 23:33:13 -0500)
      r47  1055261987      OK  (2003-06-10 15:19:47 -0500)
      r48  1054600393  NOT OK  (2003-06-02 23:33:13 -0500)
      r49  1055261988      OK  (2003-06-10 15:19:48 -0500)
      r50  1054600393  NOT OK  (2003-06-02 23:33:13 -0500)
'''

import sys
import time

line = sys.stdin.readline()
last_date = 0
while line:
  # This may get some trailing whitespace for small revision numbers,
  # but that's okay, we want our output to look tabular anyway.
  revstr = line [0:4]
  # We only need the machine-readable portion of the date.
  datestr = line[5:24]
  # We'll parse the offset by hand, and adjust the date accordingly,
  # because http://docs.python.org/lib/module-time.html doesn't seem
  # to offer any way to parse "-0500", "-0600" suffixes.  Arggh.
  offsetstr = line[25:30]
  offset_sign    = offsetstr[0:1]
  offset_hours   = int(offsetstr[1:3])
  offset_minutes = int(offsetstr[3:5])

  # Get a first draft of the date...
  date_as_int = time.mktime(time.strptime(datestr, "%Y-%m-%d %H:%M:%S"))
  # ... but it's still not correct, we have to adjust for the offset.
  if offset_sign == "-":
    date_as_int -= (offset_hours * 3600)
    date_as_int -= (offset_minutes * 60)
  elif offset_sign == "+":
    date_as_int += (offset_hours * 3600)
    date_as_int += (offset_minutes * 60)
  else:
    sys.stderr.write("Error: unknown offset sign '%s'." % offset_sign)

  ok_not_ok = "    OK"
  if last_date > date_as_int:
    ok_not_ok = "NOT OK"
  
  print "%s %10d  %s  (%s %s)" % (revstr, date_as_int, ok_not_ok,
                                  datestr, offsetstr)
  last_date = date_as_int
  line = sys.stdin.readline()
