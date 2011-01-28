#!/usr/bin/env python

#-------------------------------------------------------------------------------
# The MIT License
#
# Copyright (C) 2010 by Zack Sanborn, University of California, Santa Cruz, CA
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
#    The above copyright notice and this permission notice shall be included in
#    all copies or substantial portions of the Software.
#
#    THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
#    IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
#    FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
#    AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
#    LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
#    OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
#    THE SOFTWARE.
#-------------------------------------------------------------------------------


import os, sys, math, string, shlex, subprocess, datetime
from pyraclass import *

MAX_JOB_DEF = 2
MAX_JOB_TIME_DEF = -1 # -1 means no limit on job time
NICE_DEF = 15

def readCmdsFromFile(fname):
    inFile = open(fname)
    cmds = []
    cwd = ''
    for line in inFile:
        cmd = line.strip()
        if cmd == '':
            continue
        cmd = 'nice -n %d %s' % (NICE_DEF, os.path.join(cwd, cmd))
        cmds.append(cmd)
    inFile.close()
    return cmds
    
def readBatchesFromFile(fname):
    # list of tuples [(name1, cmds1), (name2, cmds2), ...]
    
    inFile = open(fname)
    batches = []

    cwd = ''
    name = 'unnamed'
    cmds = []
    for line in inFile:
        line = line.strip()
        if line.startswith('>'):
            if len(cmds) > 0:
                val = (name, cmds)
                batches.append(val)
            name = line[1:].split()[0]
            cmds = []
        else:
            cmd = line
            if cmd == '':
                continue
            cmd = 'nice -n %d %s' % (NICE_DEF, os.path.join(cwd, cmd))
            cmds.append(cmd)

    if len(cmds) > 0:
        val = (name, cmds)
        batches.append(val)
        
    inFile.close()
    return batches

def pycreate(args):
    if len(args) != 1:
        print "Usage: pyra.py create jobs.list"
        sys.exit(0)

    batches = readBatchesFromFile(args[0])    
    sb = superbatch(cmdbatches=batches)
    
    if sb.exists():
        print "Warning: Batch data already exists in directory"
        print "  Use 'pyra.py clean' to remove batch data"
        sys.exit(0)
    sb.write()

    p = params()
    p.setparam("maxjobs", str(MAX_JOB_DEF))
    p.setparam("maxjobtime", str(MAX_JOB_TIME_DEF))
    p.write()

    print "  Created %d jobs in %d batches:" % (sb.total(), len(sb.batches))
    for b in sb.batches:
        print "\t%s : %d jobs" % (b.name, b.total())

    # default these - sucks that the variable has to live in both notification class and here, but
    # we only want to load the notification class if these exist
    if os.environ.get("PROWL_APIKEY") is not None:
        pynotification("prowl")
    if os.environ.get("PYRA_EMAIL") is not None:
        pynotification("email")

def pypush():

    sb = superbatch()
    if not sb.exists():
        print "  No jobs to push, batch data does not exist"
        return
    
    cmd = "pyrasol.py start"
    args = shlex.split(cmd)
    try:
        subprocess.Popen(args)
    except OSError:
        print "  Unable to find pyrasol.py, make sure the pyra directory is in your path"
        return

    p = params()
    p.read()
    print "  Started pyrasol daemon and pushed %s jobs." % p.getparam('maxjobs')

def pyclean():
    if os.path.exists('.pyrasol.pid'):
        print "Warning: pyrasol daemon still running."
        print "  Use pyra.py stop to kill jobs before cleaning batch data."
        return
    
    p = params()
    p.clean()
    
    sb = superbatch()
    sb.clean()

    print "  Cleaned up all batch temporary data."
    
def pystop():
    p = params()
    p.read()
    #p.setparam("maxjobs", str(MAX_JOB_DEF))
    p.setparam("killjobs", "1")
    p.write()

    print "  Killing all running jobs, stopping pyrasol daemon."

def pymaxjob(maxjob):
    p = params()
    p.read()
    p.setparam("maxjobs", str(maxjob))
    p.write()

    print "  Max job set to %d" % maxjob

def pymaxjobtime(maxjobtime):
    p = params()
    p.read()
    p.setparam("maxjobtime", str(maxjobtime))
    p.write()

    print "  Max job time set to %d seconds" % maxjobtime

def pyparam(param,value):
    p = params()
    p.read()
    p.setparam(param, str(value))
    p.write()

    print "  %s set to %s" % (param, str(value))

def pynotification(type):
    if type == "prowl" and os.environ.get("PROWL_APIKEY") is None:
        print "  Unable to activate prowl notifications, please set PROWL_APIKEY in your env"
        return
    elif type == "email" and os.environ.get("PYRA_EMAIL") is None:
        print "  Unable to activate email notifications, please set PYRA_EMAIL in your env"
        return
    
    paramName = "notification_"+type
    p = params()
    p.read()
    if p.getparam(paramName) == "on":
        p.setparam(paramName, "off")
        print "  %s notifications disabled" % (type)
    else:
        p.setparam(paramName, "on")
        print "  %s notifications enabled" % (type)
        
    p.write()

   
def pytime():
    n = nodes()
    n.read()
    sb = superbatch()
    if not sb.exists():
        print "  Batch data for pyrasol does not exist."
        return
    
    sb.read()
    print sb.info(n)
    

def pycrashed():
    sb = superbatch()
    sb.read()

    ntotal, ncomplete, nrunning, ncrashed = sb.status()
        
    for b in sb.batches:
        print "  Crashed jobs from batch '%s'" % b.name
        for j in b.jobs:
            if j.iscrashed():
                print '\t', j.cmd

def pyinspect(batchname=None):
    if batchname is not None:
        sb = superbatch(fname=batchname)
    else:
        sb = superbatch()
    sb.read()

    for b in sb.batches:
        times = []
        mint = None
        maxt = None
        starts = []
        stops = []
        for j in b.jobs:
            if j.ispending():
                continue
            times.append(j.runningtime())
            starts.append(j.start)
            stops.append(j.stop)
            if mint is None or j.start < mint:
                mint = j.start
            if maxt is None or j.stop > maxt:
                maxt = j.stop

        if len(times) == 0:
            continue
        times.sort()
        starts.sort()
        stops.sort()
        
        startedDatetime = datetime.datetime.fromtimestamp(starts[0])
        stoppedDatetime = datetime.datetime.fromtimestamp(stops[len(stops)-1])
        
        i5 = int(math.floor(0.05 * float(len(starts))))
        starts = starts[i5:len(starts)-i5]
        stops = stops[i5:len(stops)-i5]
        
        rtime95 = max(stops) - min(starts)

        avgtall = sum(times) / len(times)
        stdall  = 0.0
        if len(times) > 2:
            sumsq = 0.0
            for t in times:
                sumsq += (t - avgtall) ** 2.0
            stdall = math.sqrt(sumsq / (len(times) - 1.0))
                    
        i5 = int(math.floor(0.05 * float(len(times))))
        times = times[i5:len(times)-i5]
        if len(times) == 0:
            continue

        avgt = sum(times) / len(times)
        std  = 0.0
        if len(times) > 2:
            sumsq = 0.0
            for t in times:
                sumsq += (t - avgt) ** 2.0
            std = math.sqrt(sumsq / (len(times) - 1.0))
        
        print '%s:' % (b.name)
        print '\tstarted %s\tfinished %s' % (startedDatetime.strftime("%m-%d-%Y %H:%M:%S"), stoppedDatetime.strftime("%m-%d-%Y %H:%M:%S"))
        print '\ttotal time     : %s' % (timestring(maxt - mint))
        print '\tavg time (all) : %s +/- %s' % (timestring(avgtall), timestring(stdall))
        print '\ttotal time (95%%) : %s' % (timestring(rtime95))
        print '\tavg time (95%%) : %s +/- %s' % (timestring(avgt), timestring(std))
        
def main(cmd, args):
    cmd = cmd.lower()
    if cmd == 'create':
        pycreate(args)
    elif cmd == 'clean':
        pyclean()
    elif cmd == 'push':
        pypush()
    elif cmd == 'time':
        pytime()
    elif cmd == 'stop':
        pystop()
    elif cmd == 'maxjob':
        pymaxjob(int(args[0]))
    elif cmd == 'maxjobtime':
        pymaxjobtime(int(args[0]))
    elif cmd == 'param':
        pyparam(args[0],args[1])
    elif cmd == 'notification':
        pynotification(args[0])
    elif cmd == 'crashed':
        pycrashed()
    elif cmd == 'inspect':
        if len(args) == 1:
            pyinspect(args[0])
        else:
            pyinspect(None)
    else:
        print "Unrecognized command: %s" % cmd

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print "Usage: "
        print "  pyra.py [command]"
        print "Commands:"
        print "  create job.list  : create a batch of jobs from file"
        print "  clean            : cleans up tmp files"
        print "  push             : push jobs"
        print "  time             : check time / status of running jobs"
        print "  stop             : kill all running jobs and stop pyrasol"
        print "  maxjob INT       : set max jobs"
        print "  maxjobtime INT   : set max job time in seconds"
        print "  notification TYPE: enable/disable notification (options: prowl,email)"

        print "  crashed          : list crashed jobs"
        print "  inspect batch.gz : compute summary of batch"
        sys.exit(0)

    cmd = sys.argv[1]
    main(cmd, sys.argv[2:])
    
    
