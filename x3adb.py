import xmlrpclib
import logging
import subprocess
import cookielib
import urlparse
import os
class x3adb:
    def __init__(self, cookiefilepath='x3adb-ssocookie.txt'):
        self.cookiefile = os.path.abspath(cookiefilepath)
    def authorize(self, username=None):
        print "Calling kinit, please enter your CERN password"
        call = ['kinit']
        if username is not None:
            call.append(username + "@CERN.CH")
        x=subprocess.call(call)
        print "Result of kinit: " +str(x)
        logging.info("Result of kinit: " +str(x))
        logging.info("Retrieving SSO cookie using cern-get-sso-cookie")
        self.obtainSSOCookies()
    def obtainSSOCookies(self):
        call=['env','-i','cern-get-sso-cookie', '--krb', '--url', 'https://olschew.web.cern.ch/olschew/x3adb/xmlrpc_auth/x3adb_write.php', '--reprocess', '--outfile', self.cookiefile]
        x = subprocess.call(call)
        if x > 0:
            logging.warning("Failed to retrieve a cookie, authentication not possible "+str(x))
    def destroyauth(self):
        try:
            os.remove(self.cookiefile)
        except:
            logging.warning("Error while removing cookie file")
    def getAuthServerProxy(self):
        domain = "olschew.web.cern.ch"
        url = 'https://olschew.web.cern.ch/olschew/x3adb/xmlrpc_auth/x3adb_write.php'
        customtransport = transport(url)
        customtransport.setcookies(self.cookiefile, domain)
        s = xmlrpclib.ServerProxy(url, customtransport)
        return s
    def registerMCSample(self, sample):
        s = self.getAuthServerProxy()
        return s.registerMCSample(sample)
    def editMCSample(self, id, sample):
        s = self.getAuthServerProxy()
        return s.editMCSample(id, sample)
    def insertOrReplaceMCTags(self, id, tags):
        s = self.getAuthServerProxy()
        return s.insertOrReplaceMCTags(id, tags)
    def registerDataSample(self, sample):
        s = self.getAuthServerProxy()
        return s.registerDataSample(sample)
    def getMCSample(self, sampleid):
        s = xmlrpclib.ServerProxy('https://olschew.web.cern.ch/olschew/x3adb/xmlrpc/x3adb_read.php')
        return s.getMCSample(sampleid)
    def getDataSample(self, sampleid):
        s = xmlrpclib.ServerProxy('https://olschew.web.cern.ch/olschew/x3adb/xmlrpc/x3adb_read.php')
        return s.getDataSample(sampleid)
    def test(self):
        s = self.getAuthServerProxy()
        return s.test()
        
def main():
    print "This is just a library"
    


class cookietransportrequest:
    """A Transport request method that retains cookies over its lifetime.
 
    The regular xmlrpclib transports ignore cookies. Which causes
    a bit of a problem when you need a cookie-based login, as with
    the Bugzilla XMLRPC interface.
 
    So this is a helper for defining a Transport which looks for
    cookies being set in responses and saves them to add to all future
    requests.
    """
    # From http://www.lunch.org.uk/wiki/xmlrpccookies
    # Inspiration drawn from
    # http://blog.godson.in/2010/09/how-to-make-python-xmlrpclib-client.html
    # http://www.itkovian.net/base/transport-class-for-pythons-xml-rpc-lib/
    #
    # Note this must be an old-style class so that __init__ handling works
    # correctly with the old-style Transport class. If you make this class
    # a new-style class, Transport.__init__() won't be called.
 
    cookies = []
    def setcookies(self,cookiefile, domain):
        jar=cookielib.MozillaCookieJar(cookiefile)
        jar.load(ignore_discard=False, ignore_expires=False)
        for cookie in jar:
            if cookie.domain==domain:
                self.cookies.append("$Version=1; "+cookie.name+"="+cookie.value+";")
    def send_cookies(self, connection):
        if self.cookies:
            for cookie in self.cookies:
                connection.putheader("Cookie", cookie)
 
    def request(self, host, handler, request_body, verbose=0):
        self.verbose = verbose
 
        # issue XML-RPC request
        h = self.make_connection(host)
        if verbose:
            h.set_debuglevel(1)
 
        self.send_request(h, handler, request_body)
        self.send_host(h, host)
        self.send_cookies(h)
        self.send_user_agent(h)
        self.send_content(h, request_body)
 
        # Deal with differences between Python 2.4-2.6 and 2.7.
        # In the former h is a HTTP(S). In the latter it's a
        # HTTP(S)Connection. Luckily, the 2.4-2.6 implementation of
        # HTTP(S) has an underlying HTTP(S)Connection, so extract
        # that and use it.
        try:
            response = h.getresponse()
        except AttributeError:
            response = h._conn.getresponse()
 
        # Add any cookie definitions to our list.
        for header in response.msg.getallmatchingheaders("Set-Cookie"):
            val = header.split(": ", 1)[1]
            cookie = val.split(";", 1)[0]
            self.cookies.append(cookie)
 
        if response.status != 200:
            raise xmlrpclib.ProtocolError(host + handler, response.status,
                                          response.reason, response.msg.headers)
 
        payload = response.read()
        parser, unmarshaller = self.getparser()
        parser.feed(payload)
        parser.close()
 
        return unmarshaller.close()
 
class cookietransport(cookietransportrequest, xmlrpclib.Transport):
    pass
 
class cookiesafetransport(cookietransportrequest, xmlrpclib.SafeTransport):
    pass

def transport(uri):
    """Return an appropriate Transport for the URI.

    If the URI type is https, return a CookieSafeTransport.
    If the type is http, return a CookieTransport.
    """
    if urlparse.urlparse(uri, "http")[0] == "https":
        return cookiesafetransport()
    else:
        return cookietransport()
    
if __name__=="__main__":    
    main()