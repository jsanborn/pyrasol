import os, sys, string, time, gzip, math

CRASH_CHAR = 'X'
COMP_CHAR  = '*'
RUN_CHAR   = 'r'

def timestring(t):
    h = int(math.floor(t / 60. / 60.))
    m = int(math.floor((t - float(h) * 60. * 60.) / 60.))
    s = int(math.floor(t - float(m) * 60. - float(h) * 60. * 60.))
    
    if h < 10:
        h = '0' + str(h)
    else:
        h = str(h)
    if m < 10:
        m = '0' + str(m)
    else:
        m = str(m)
    if s < 10:
        s = '0' + str(s)
    else:
        s = str(s)
                
    return ':'.join([h, m, s])
            
class nodes:
    def __init__(self, maxjob=None):
        self.nodes     = {}
        self.fname     = '.config'
        self.available = {}
        
    def read(self):
        if not os.path.exists(self.fname):
            for i in range(1000):
                self.nodes[i] = 'localhost'
                self.available[i] = True
            #print >> sys.stderr, ".config file doesn't exist."
            return
        
        inFile = open(self.fname)
        n = 0
        for line in inFile:
            data = line[:-1].split('\t')

            addr     = data[0]
            numcores = int(data[1])

            for i in range(numcores):
                self.nodes[n]     = addr
                self.available[n] = True
                n += 1
                
        inFile.close()
        
    def getaddr(self, n):
        if n not in self.nodes:
            return None
        return self.nodes[n]

    def setactive(self, n):
        if n not in self.available:
            return
        self.available[n] = False

    def setavailable(self, n):
        if n not in self.available:
            return
        self.available[n] = True

    def getavailable(self):
        for n in self.available:
            if self.available[n]:
                return n

class params:
    def __init__(self, maxjob=None):
        self.params = {}
        self.time = 0
        self.fname = 'pyra.params'

    def clean(self):
        if os.path.exists(self.fname):
            os.remove(self.fname)
        
    def read(self):
        inFile = open(self.fname)
        for line in inFile:
            data = line[:-1].split('\t')
            self.params[data[0]] = data[1]
        inFile.close()

        statinfo = os.stat(self.fname)
        self.time = statinfo.st_mtime
            
    def write(self):
        outFile = open(self.fname, 'w')
        for key in self.params:
            s = key + '\t' + self.params[key] + '\n'
            outFile.write(s)
        outFile.close()

    def isnew(self):
        if self.time == 0:
            return True

        statinfo = os.stat(self.fname)
        if self.time != statinfo.st_mtime:
            return True

        return False
        
    def getparam(self, key):
        if key not in self.params:
            return None
        return self.params[key]
    
    def setparam(self, key, value):
        self.params[key] = value

class job:
    def __init__(self, cmd=None, pid=-1, node=-1, start=-1.0, stop=-1.0, status='pending'):
        self.cmd    = cmd
        self.pid    = pid
        self.node   = node
        self.start  = start
        self.stop   = stop
        self.status = status
        
    def __str__(self):
        s = []
        s.append('"' + self.cmd + '"')
        s.append(str(self.pid))
        s.append(str(self.node))
        s.append(str(self.start))
        s.append(str(self.stop))
        s.append(self.status)

        return '\t'.join(s)

    def update(self, cmd=None, pid=None, node=None, start=None, stop=None, status=None):
        if cmd is not None:
            self.cmd = cmd
        if pid is not None:
            self.pid = pid
        if node is not None:
            self.node = node
        if start is not None:
            self.start = start
        if stop is not None:
            self.stop = stop
        if status is not None:
            self.status = status
            
    def read(self, str):
        fields = str.split('\t')
        if len(fields) != 6 and len(fields) != 5:
            print "error in job.read: fields != 5 or 6"
            print str
            return
        
        cmd    = fields[0][1:-1]  # remove leading/trailing quotes
        pid    = int(fields[1])
        
        if len(fields) == 5:
            node   = -1 
            start  = float(fields[2])
            stop   = float(fields[3])
            status = fields[4]
        else:
            node   = int(fields[2])
            start  = float(fields[3])
            stop   = float(fields[4])
            status = fields[5]
    
        self.update(cmd=cmd, pid=pid, node=node, start=start, stop=stop, status=status)

    def ispending(self):
        if self.status == 'pending':
            return True
        return False

    def isrunning(self):
        if self.status == 'running':
            return True
        return False

    def iscrashed(self):
        if self.status == 'crashed':
            return True
        return False

    def iscompleted(self):
        if self.status == 'completed':
            return True
        return False

    def runningtime(self):
        return self.stop - self.start
    
    def setrunning(self):
        self.setstart()
        self.update(status='running')

    def setcrashed(self):
        self.update(status='crashed')

    def setcompleted(self):
        self.update(status='completed')

    def setstart(self):
        t = time.time()
        self.update(start=t, stop=t)

    def settime(self):
        self.update(stop=time.time())

    def setpid(self, pid):
        self.update(pid=pid)

    def setnode(self, node):
        self.update(node=node)

class batch:
    def __init__(self, name='unnamed', jobs=[]):
        self.name = name
        self.jobs = []
        for j in jobs:
            self.jobs.append(job(j))
            
    def __str__(self):
        s = '>' + self.name + '\n'
        for j in self.jobs:
            s += str(j) + '\n'
        return s

    def read(self, joblist=[]):
        for line in joblist:
            j = job()
            j.read(line)
            self.jobs.append(j)
            
    def running(self):
        n = 0
        for job in self.jobs:
            if job.isrunning():
                n += 1
        return n

    def pending(self):
        n = 0
        for job in self.jobs:
            if job.ispending():
                n += 1
        return n

    def crashed(self):
        n = 0
        for job in self.jobs:
            if job.iscrashed():
                n += 1
        return n

    def total(self):
        return len(self.jobs)

    def status(self):
        nr  = 0
        ncr = 0
        nco = 0
        nt  = 0
        for job in self.jobs:
            nt += 1
            if job.iscrashed():
                ncr += 1
            elif job.isrunning():
                nr += 1
            elif job.iscompleted():
                nco += 1
        return nt, nco, nr, ncr

    def info(self):
        nt, nco, nr, ncr = self.status()
        
        maxwidth = 48
        if nt > maxwidth:
            width = maxwidth
        else:
            width = nt

        pco = int(round(float(width) * float(nco) / float(nt)))
        pr  = int(round(float(width) * float(nr)  / float(nt)))
        pcr = int(round(float(width) * float(ncr) / float(nt)))
        np = width - pco - pr - pcr

        s = '  ' + self.name + '\t['
        s += COMP_CHAR * pco + CRASH_CHAR * pcr + RUN_CHAR * pr + ' ' * np
        s += '] %d of %d jobs\n' % (nco + ncr, nt)
        return s
                                                                

    
class superbatch:
    def __init__(self, fname='pybatch.gz', cmdbatches = None):
        self.batches = []
        self.fname = fname

        if cmdbatches is not None:
            for name, cmds in cmdbatches:
                self.batches.append(batch(name, cmds))

    def exists(self):
        return os.path.exists(self.fname)

    def clean(self):
        if os.path.exists(self.fname):
            os.remove(self.fname)
        if os.path.exists(self.fname + '.bak'):
            os.remove(self.fname + '.bak')
            
    def write(self):
        if self.exists():
            os.rename(self.fname, self.fname + '.bak')

        # write jobs
        outFile = gzip.open(self.fname, 'wb')
        for b in self.batches:
            outFile.write(str(b))
        outFile.close()

    def read(self):
        if not self.exists():
            return
        joblist   = []
        batchname = None

        inFile = gzip.open(self.fname, 'rb')
        for line in inFile:
            line = line.strip()
            if line.startswith('>'):
                if batchname is not None and len(joblist) > 0:
                    b = batch(batchname)
                    b.read(joblist)
                    self.batches.append(b)

                    joblist = []
                    batchname = None
                batchname = line[1:].split()[0]
            else:
                if batchname is None:
                    continue
                joblist.append(line)

        if batchname is not None and len(joblist) > 0:
            b = batch(batchname)
            b.read(joblist)
            self.batches.append(b)

        inFile.close()
        
    def running(self):
        n = 0
        for batch in self.batches:
            n += batch.running()
        return n

    def remaining(self):
        n = 0
        for batch in self.batches:
            n += batch.remaining()
        return n

    def pending(self):
        n = 0
        for batch in self.batches:
            n += batch.pending()
        return n

    def crashed(self):
        n = 0
        for batch in self.batches:
            n += batch.crashed()
        return n

    def total(self):
        n = 0
        for batch in self.batches:
            n += batch.total()
        return n

    def status(self):
        nco = 0
        ncr = 0
        nr = 0
        nt = 0
        for batch in self.batches:
            bt, bco, br, bcr = batch.status()
            nt += bt
            nco += bco
            ncr += bcr
            nr += br

        return nt, nco, nr, ncr

    def info(self, nodeobj=None):
        ntotal, ncomplete, nrunning, ncrashed = self.status()
        
        mintime = None
        maxtime = None
        tr = 0.0

        nodes = {}
        for b in self.batches:
            for j in b.jobs:
                if j.ispending():
                    continue
                tr += j.runningtime()

                if nodeobj is not None:
                    node = nodeobj.getaddr(j.node)
                else:
                    node = str(j.node)
                if node not in nodes:
                    nodes[node] = 0
                nodes[node] += 1
                
                if mintime is None or (j.start > 0 and j.start < mintime):
                    mintime = j.start
                if maxtime is None or (j.stop > 0 and j.stop > maxtime):
                    maxtime = j.stop

        s = '  pyrasol superbatch info:\n'
        s += '\t%d running\n' % nrunning
        s += '\t%d of %d completed\n' % (ncomplete, ntotal)
        s += '\t%d of %d crashed\n' % (ncrashed, ntotal)

        ts = timestring(tr)
        s += '\trunning time: %s\n' % ts
        
        if maxtime is None or mintime is None:
            s += '\twall clock: N/A\n'
        else:
            ts = timestring(maxtime - mintime)
            s += '\twall clock: %s\n' % ts

        s += '  jobs on nodes: (node:jobs)\n'
        nodeinfo = []
        for n in nodes:
            if n == -1:
                nstr = 'localhost'
            else:
                nstr = n
            nodeinfo.append('%s:%d' % (nstr, nodes[n]))
        nodeinfo.sort()
        s += '\t' + ' '.join(nodeinfo) + '\n'
        
        for b in self.batches:
            s += b.info()
                        
        s += "  (complete = '%s', running = '%s', crashed = '%s')" % (COMP_CHAR, RUN_CHAR, CRASH_CHAR)
        return s
            
