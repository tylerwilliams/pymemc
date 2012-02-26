#!/usr/bin/env python
import subprocess
import shlex
from optparse import OptionParser

cmd = "memcached %s -p"
start_port = 11211
    
def main():
    global cmd, start_port
    parser = OptionParser(usage="usage: %prog -v <num_procs>")
    
    parser.add_option("-v", "--verbose",
                      metavar="verbosity", help="be noisy", default=0, action="count")
    
    (options, args) = parser.parse_args()
    
    if not args:
        num_procs = 1
    else:
        num_procs = int(args[0])
    
    if options.verbose:
        verbosity = "-" + "v"*options.verbose
        cmd = cmd % verbosity
    else:
        cmd = cmd % ""
    
    procs = []
    for i in xrange(num_procs):
        args = shlex.split(cmd) + [str(start_port+i)]
        print " ".join(args)
        p = subprocess.Popen(args)
        procs.append(p)
    try:
        for p in procs:
            p.wait()
    except KeyboardInterrupt:
        for p in procs:
            p.terminate()

if __name__ == "__main__":
    main()