#!/usr/bin/env python
import sys, time, random

if __name__ == '__main__':
    if len(sys.argv) != 2:
        print 'testpyra.py delaytime'
        print '  delaytime = time (in seconds) to delay before dying'
        sys.exit(0)

    t = int(sys.argv[1])

    for i in range(t):
        time.sleep(1)

        if random.random() < 0.05:
            print "error!"
            sys.exit(1)

        

    
