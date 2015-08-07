import xmlrpclib
import logging
import subprocess
import cookielib
import urlparse
import os

log = logging.getLogger( 'aix3adb' )

def tryServerAuth(funct):
    def func_wrapper(instance, *args, **kwargs):
        try:
            return funct(instance, *args, **kwargs)
        except xmlrpclib.ProtocolError:
            instance.destroyauth()
            instance.authorize()
            return funct(instance, *args, **kwargs)
    return func_wrapper

class Aix3adbException(Exception):
    pass

class Aix3adbBaseElement:
    def __init__(self, dictionary=dict() ):
        for key in dictionary:
            if key=="error":
                raise Aix3adbException(dictionary[key])
            setattr(self, key, dictionary[key])
    def __repr__(self):
        return "%s(%r)" % (self.__class__, self.__dict__)

class MCSample(Aix3adbBaseElement):
    pass

class DataSample(Aix3adbBaseElement):
    pass

class MCSkim(Aix3adbBaseElement):
    pass

class DataSkim(Aix3adbBaseElement):
    pass

class aix3adb:
    def __init__(self, cookiefilepath='aix3adb-ssocookie.txt'):
        self.cookiefile = os.path.abspath(cookiefilepath)
        self.authurl = 'https://cms-project-aachen3a-datasets.web.cern.ch/cms-project-aachen3a-datasets/aix3adb2/xmlrpc_auth/x3adb_write.php'
        self.readurl = 'https://cms-project-aachen3a-datasets.web.cern.ch/cms-project-aachen3a-datasets/aix3adb2/xmlrpc/x3adb_read.php'
        self.domain  = 'cms-project-aachen3a-datasets.web.cern.ch'
    def authorize(self, username=None, trykerberos=3):
        print "Calling kinit, please enter your CERN password"
        call = ['kinit']
        if username is not None:
            call.append(username + "@CERN.CH")
        for i in range(trykerberos):
            x = subprocess.call(call)
            log.info("Result of kinit: " + str(x))
            if x == 0: break
            print "kinit failed. Please try again."
        self.obtainSSOCookies()
    def obtainSSOCookies(self):
        call = ['env', '-i', '/home/home1/institut_3a/olschewski/public/cernsso/cern-get-sso-cookie', '--krb', '--url', self.authurl, '--reprocess', '--outfile', self.cookiefile]
        x = subprocess.call(call)
        if x > 0:
            log.error("Failed to retrieve a cookie, authentication not possible")
    def destroyauth(self):
        try:
            os.remove(self.cookiefile)
        except:
            log.error("Failed to remove cookie file")
    def getAuthServerProxy(self):
        customtransport = transport(self.authurl)
        customtransport.setcookies(self.cookiefile, self.domain)
        s = xmlrpclib.ServerProxy(self.authurl, customtransport)
        return s
    def getMCMaxSkimID(self):
        s = xmlrpclib.ServerProxy(self.readurl)
        result = s.getMCMaxSkimID( )
        return MCSkim(result['skim']).id
    def getDataMaxSkimID(self):
        s = xmlrpclib.ServerProxy(self.readurl)
        result = s.getDataMaxSkimID( )
        return DataSkim(result['skim']).id
    # inserts
    @tryServerAuth
    def insertMCSample(self, sample):
        s = self.getAuthServerProxy()
        f = s.insertMCSample(sample.__dict__)
        return MCSample(f)
    @tryServerAuth
    def insertDataSample(self, sample):
        s = self.getAuthServerProxy()
        return DataSample(s.insertDataSample(sample.__dict__))
    @tryServerAuth
    def insertMCSkim(self, skim):
        s = self.getAuthServerProxy()
        return MCSkim(s.insertMCSkim(skim.__dict__))
    @tryServerAuth
    def insertDataSkim(self, skim):
        s = self.getAuthServerProxy()
        return DataSkim(s.insertDataSkim(skim.__dict__))
    # edits
    @tryServerAuth
    def editMCSample(self, sample):
        s = self.getAuthServerProxy()
        return MCSample(s.editMCSample(sample))
    @tryServerAuth
    def editDataSample(self, sample):
        s = self.getAuthServerProxy()
        return DataSample(s.editDataSample(sample))
    @tryServerAuth
    def editMCSkim(self, skim):
        s = self.getAuthServerProxy()
        return MCSkim(s.editMCSkim(skim))
    @tryServerAuth
    def editDataSkim(self, skim):
        s = self.getAuthServerProxy()
        return DataSkim(s.editDataSkim(skim))
    # deletes
    @tryServerAuth
    def deleteMCSampleByName(self, name):
        s = self.getAuthServerProxy()
        return s.deleteMCSample(name)["info"]
    @tryServerAuth
    def deleteDataSampleByName(self, name):
        s = self.getAuthServerProxy()
        return s.deleteDataSample(name)["info"]
    @tryServerAuth
    def deleteMCSkimById(self, skimid):
        s = self.getAuthServerProxy()
        return s.deleteMCSkim(skimid)["info"]
    @tryServerAuth
    def deleteDataSkimById(self, skimid):
        s = self.getAuthServerProxy()
        return s.editDataSkim(skimid)["info"]
    # gets
    def getMCSample(self, name):
        s = xmlrpclib.ServerProxy(self.readurl)
        return MCSample(s.getMCSample(name))
    def getDataSample(self, name):
        s = xmlrpclib.ServerProxy(self.readurl)
        return DataSample(s.getDataSample(name))
    def getMCSkim(self, skimid):
        s = xmlrpclib.ServerProxy(self.readurl)
        return MCSkim(s.getMCSkim(skimid))
    def getDataSkim(self, skimid):
        s = xmlrpclib.ServerProxy(self.readurl)
        return DataSkim(s.getDataSkim(skimid))
    def getMCLatestSkimAndSampleBySample(self, name):
        s = xmlrpclib.ServerProxy(self.readurl)
        result = s.getMCLatestSkimAndSampleBySample(name)
        return MCSkim(result['skim']), MCSample(result['sample'])
    def getDataLatestSkimAndSampleBySample(self, name):
        s = xmlrpclib.ServerProxy(self.readurl)
        result = s.getDataLatestSkimAndSampleBySample(name)
        return DataSkim(result['skim']), DataSample(result['sample'])
    def getMCLatestSkimAndSampleByDatasetpath(self, datasetpath):
        s = xmlrpclib.ServerProxy(self.readurl)
        result = s.getMCLatestSkimAndSampleByDatasetpath( datasetpath )
        return MCSkim(result['skim']), MCSample(result['sample'])
    def getDataLatestSkimAndSampleByDatasetpath(self, datasetpath):
        s = xmlrpclib.ServerProxy(self.readurl)
        result = s.getDataLatestSkimAndSampleByDatasetpath( datasetpath )
        return DataSkim(result['skim']), DataSample(result['sample'])
    def getMCSkimAndSampleBySkim(self, skimid):
        s = xmlrpclib.ServerProxy(self.readurl)
        result = s.getMCSkimAndSampleBySkim(skimid)
        return MCSkim(result['skim']), MCSample(result['sample'])
    def getDataSkimAndSampleBySkim(self, skimid):
        s = xmlrpclib.ServerProxy(self.readpurl)
        result = s.getDataSkimAndSampleBySkim(skimid)
        return DataSkim(result['skim']), DataSample(result['sample'])
    def getMCSkimAndSample(self, skimid=None, name=None):
        if skimid is not None:
            skim, sample = self.getMCSkimAndSampleBySkim(skimid)
            if name is not None:
                if name != sample.name:
                    raise Exception("Skimid "+str(skimid)+" and sample name "+str(name)+" do not match.")
        elif name is not None:
            skim, sample = self.getMCLatestSkimAndSampleBySample(name)
        else:
            raise Exception("No arguments provided.")
        return skim, sample


class aix3adbAuth(aix3adb):
   def __init__(self,username=None, trykerberos=3):
      aix3adb.__init__(self)
      self.authorize(username, trykerberos)
   def __del__(self):
      self.destroyauth()

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
        self.cookies = []
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

# helper function to directly retrieve a dblink object
def createDBlink(user, readOnly= True):

    # Create a database object.
    dblink = aix3adb()

    # Authorize to database.
    log.info( "Connecting to database: 'http://cern.ch/aix3adb'" )
    if not readOnly: dblink.authorize(username = user)
    log.info( 'Authorized to database.' )
    return dblink
