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

#
# Notification code developed by Stephen Benz, UCSC, 2010
#

import httplib, os, urllib, sys, email, smtplib
from email.MIMEText import MIMEText

#interface for notifications
class notification:
    def __init__(self):
        self.subject = ""
        self.message = ""

    def setSubject(self,subj):
        self.subject = subj

    def setMessage(self,msg):
        self.message = msg
    
    def send(self):
        pass

class notification_prowl(notification):
    def __init__(self):
        self.prowlKey = os.environ.get("PROWL_APIKEY")

    def send(self):
        params = urllib.urlencode({'apikey': self.prowlKey, 'application': 'pyra', 'event': self.subject,'description': self.message })
        
        headers = {"Content-type": "application/x-www-form-urlencoded",
                 "Accept": "text/plain"}
        
        conn = httplib.HTTPSConnection("prowl.weks.net")
        conn.request("POST", "/publicapi/add", params, headers)
        response = conn.getresponse()
        data     = response.read()
        conn.close()

class notification_email(notification):
    def __init__(self):
        self.emailAddress = os.environ.get("PYRA_EMAIL")
        self.smtpServer   = os.environ.get("PYRA_SMTP_SERVER")

    def send(self):
        msg = MIMEText(self.message)
        msg['Subject'] = "[pyra] "+self.subject
        msg['From']    = self.emailAddress
        msg['To']      = self.emailAddress

        s = smtplib.SMTP()
        s.connect(self.smtpServer)
        s.sendmail(self.emailAddress,self.emailAddress,msg.as_string())
        s.close()
        
 
