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

import sys, time, shlex, subprocess, signal
from daemon import Daemon
from pyraclass import *

def getSpawned(pids):
    pidlist = []
    for pid in pids:
        pidlist.append(str(pid))
    pidlist = ','.join(pidlist)

    args = ["ps --ppid %s -o pid=" % pidlist]
    p = subprocess.Popen(args, shell=True,
                         stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                         stderr=subprocess.PIPE, close_fds=True)

    spawned = []
    lines = p.stdout.readlines()
    for l in lines:
        spawned.append(int(l.strip()))

    # append any spawning process pids to clean up (if necessary) 
    for pid in pids:
        spawned.append(pid)
        
    return spawned

class MyDaemon(Daemon):
    def __init__(self, *args, **kwargs):
        self.sb = None
        self.params = None
        self.nodes = None
        self.processes = {}
        self.running = True
        Daemon.__init__(self, *args, **kwargs)

    def run(self):
        count = 0
        while self.running:
            self.update()
            count += 1            
            if count == 6:
                self.sb.write()  # only write 30 seconds
                count = 0

            if self.params.getparam("killjobs") is not None:
                self.killjobs()
                self.running = False
                break
            
            time.sleep(5)
        
        self.postRun()

        self.sb.write()
        sys.exit(0)

    def killjobs(self):
        pids = []
        for pid in self.processes:
            if pid <= 0:
                continue
            pids.append(pid)

        spawned = getSpawned(pids)
        sys.stderr.write("killing %d running jobs.\n" % len(spawned))

        # kill spawned processes, which will also kill shells which spawned them.
        for pid in spawned:
            count     = 0
            killedpid = 0
            try:
                while killedpid == 0 and count < 10:
                    count += 1                    
                    os.kill(pid, signal.SIGKILL)
                    sys.stderr.write("  killing pid %d, attempt number %d\n" % (pid, count))
                    killedpid, stat = os.waitpid(pid, os.WNOHANG)
                    time.sleep(0.1)
                    
            except OSError, err:
                err = str(err)
                if err.find("No such process") <= 0:
                    sys.stderr.write(err)
                
    def stop(self):
        Daemon.stop(self)

    def jobfornode(self, cmd, node):
        if node == -1:
            return cmd

        addr = self.nodes.getaddr(node)
        if addr == 'localhost':
            return cmd
        else:
            return 'ssh %s "%s"' % (addr, cmd.replace('"', '\"'))
        
    def pushjob(self, job, node):
        if job.cmd is None:
            return

        cmd = self.jobfornode(job.cmd, node)
        print >> sys.stderr, "node = %d, new cmd = %s" % (node, cmd)
        
        p = subprocess.Popen(cmd, shell=True)  # will execute through shell
        job.setpid(p.pid)
        job.setnode(node)
        job.setrunning()

        self.processes[p.pid] = p
        self.nodes.setactive(node)

    def popjob(self, job):
        if job.pid in self.processes:
            del self.processes[job.pid]
        self.nodes.setavailable(job.node)
        
    def checkjob(self, job, maxjobtime=-1):
        if job.pid not in self.processes:
            return

        p = self.processes[job.pid]
        retcode = p.poll()
        job.settime()

        # check if job has exceeded maxtime
        if maxjobtime > 0 and job.runningtime() > maxjobtime:
            spawned = getSpawned([job.pid])
    
            # kill spawned processes, also kills shells that spawned them.
            for pid in spawned:
                sys.stderr.write("killing spawned: %d\n" % pid)
                try:
                    os.kill(pid, signal.SIGKILL)
                except OSError, err:
                    err = str(err)
                    if err.find("No such process") <= 0:
                        sys.stderr.write(err)

        if retcode is None:
            return

        # otherwise finished in some way, check for crash
        if retcode == 0:
            job.setcompleted()
        else:
            job.setcrashed()

        self.popjob(job)
        
    def updateparams(self):
        p = params()
        if self.params is None or self.params.isnew():
            p.read()
            self.params = p

    def updatenodes(self):
        if self.nodes is None:
            n = nodes()
            n.read()
            self.nodes = n
        
    def updatebatch(self):
        if self.sb is None:
            self.sb = superbatch()
            self.sb.read()

    def update(self):
        self.updatenodes()
        
        self.updateparams()
        maxjobs = int(self.params.getparam("maxjobs"))
        maxjobtime = int(self.params.getparam("maxjobtime"))

        self.updatebatch()
        totr = self.sb.running()
        totp = self.sb.pending()

        if totp == 0 and totr == 0:  # all jobs done
            self.running = False
            return

        for batch in self.sb.batches:
            nr = batch.running()
            np = batch.pending()
            if nr == 0 and np == 0:
                continue  # done with batch, go to next

            for job in batch.jobs:
                if job.isrunning():
                    self.checkjob(job,maxjobtime)
                elif job.ispending() and totr < maxjobs:
                    node = self.nodes.getavailable()
                    if node is None:
                        break
                    self.pushjob(job, node)
                    totr += 1

                    if totr == maxjobs:
                        break
            break  # can only do a batch at a time
        
    def postRun(self):
        p = params()
        p.read()

        name = os.getcwd()
        subj = "run complete: %s" % name
        msg = self.sb.info()
        notificationModules = ['notification_prowl','notification_email']
        for nModule in notificationModules:
            if p.getparam(nModule) == "on":
                try:
                    if nModule == 'notification_prowl':
                        from pyranotification import notification_prowl
                        notification = notification_prowl()
                    elif nModule == 'notification_email':
                        from pyranotification import notification_email
                        notification = notification_email()
                    notification.setSubject(subj)
                    notification.setMessage(msg)
                    notification.send()
                except:
                    pass


if __name__ == "__main__":
    daemon = MyDaemon('.pyrasol.pid', stderr='err.log')
    if len(sys.argv) >= 2:
        if 'start' == sys.argv[1]:
            daemon.start()
        elif 'stop' == sys.argv[1]:
            daemon.stop()
        elif 'restart' == sys.argv[1]:
            daemon.restart()
        else:
            print "Unknown command"
            sys.exit(2)
        sys.exit(0)
        
    else:
        print "usage: %s cmd" % sys.argv[0]
        print "  start        : start daemon"
        print "  stop         : stop daemon"
        print "  restart      : restart daemon"
        print "  submit fname : submit batch file"
        
        sys.exit(2)
