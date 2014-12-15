import xmlrpclib
import logging
import subprocess
import cookielib
import urlparse
import os

# mcm utilities imports
import urllib2
import json

log = logging.getLogger( 'aix3adb' )

class aix3adb:
    def __init__(self, cookiefilepath='aix3adb-ssocookie.txt'):
        self.cookiefile = os.path.abspath(cookiefilepath)
        self.authurl = 'https://cms-project-aachen3a-datasets.web.cern.ch/cms-project-aachen3a-datasets/aix3adb/xmlrpc_auth/x3adb_write.php'
        self.readurl = 'https://cms-project-aachen3a-datasets.web.cern.ch/cms-project-aachen3a-datasets/aix3adb/xmlrpc/x3adb_read.php'
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
        call = ['env', '-i', 'cern-get-sso-cookie', '--krb', '--url', self.authurl, '--reprocess', '--outfile', self.cookiefile]
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
    def registerMCSample(self, sample):
        check = self.checksample(sample)
        if check:
            return self.tryServerAuthFunction(self.doRegisterMCSample, sample)
    def doRegisterMCSample(self, sample):
        s = self.getAuthServerProxy()
        return s.registerMCSample(sample)
    def editMCSample(self, id, sample):
        check = self.checksample(sample)
        if check:
            return self.tryServerAuthFunction(self.doEditMCSample, id, sample)
    def doEditMCSample(self, id, sample):
        s = self.getAuthServerProxy()
        return s.editMCSample(id, sample)
    def insertOrReplaceMCTags(self, id, tags):
        s = self.getAuthServerProxy()
        return s.insertOrReplaceMCTags(id, tags)
    def registerDataSample(self, sample):
        check = self.checksample(sample)
        if check:
            return self.tryServerAuthFunction(self.doRegisterDataSample, sample)
    def doRegisterDataSample(self, sample):
        s = self.getAuthServerProxy()
        return s.registerDataSample(sample)
    def editDataSample(self, id, sample):
        check = self.checksample(sample)
        if check:
            return self.tryServerAuthFunction(self.doEditDataSample, id, sample)
    def doEditDataSample(self, id, sample):
        s = self.getAuthServerProxy()
        return s.editDataSample(id, sample)
    def getMCSample(self, sampleid):
        s = xmlrpclib.ServerProxy(self.readurl)
        return s.getMCSample(sampleid)
    def getDataSample(self, sampleid):
        s = xmlrpclib.ServerProxy(self.readurl)
        return s.getDataSample(sampleid)
    def searchMCSamples(self, searchdict):
        s = xmlrpclib.ServerProxy(self.readurl)
        return s.searchMCSamples(searchdict)
    def searchDataSamples(self, searchdict):
        s = xmlrpclib.ServerProxy(self.readurl)
        return s.searchDataSamples(searchdict)
    def test(self):
        s = self.getAuthServerProxy()
        return s.test()
    def checksample(self,sample):
        msg = None
        if None in sample: msg = "None-item in sample"
        if "tags" in sample:
            if not type(sample['tags']) == dict: msg = "Tags is not a dict"
        if "files" in sample:
            if not type(sample['files']) == list: msg = "Files is not a list"
        if msg is not None:
            log.error("Sample failed sanity checks: " + msg)
            return False
        return True
    def tryServerAuthFunction(self, funct, *params):
        try:
            return funct(*params)
        except xmlrpclib.ProtocolError:
            self.destroyauth()
            self.authorize()
            return funct(*params)


class aix3adbAuth(aix3adb):
   def __init__(self,username=None, trykerberos=3):
      aix3adb.__init__(self)
      self.authorize(username, trykerberos)
   def __del__(self):
      self.destroyauth()



class McMUtilities():
    def __init__(self):
        self.mcm_prefix = "https://cms-pdmv.cern.ch/mcm/public/restapi/requests/produces/"
        self.mcm_json = None # requested json
        self.gen_json = None # gen-sim json


    def readJSON(self, json_string):
        try:
            return json.load(json_string)
        except ValueError:
            log.error("Could not read JSON at request URL.")
            return None


    def readURL(self, mcm_dataset):
        try:
            self.mcm_json = self.readJSON(urllib2.urlopen(self.mcm_prefix + mcm_dataset))
        except urllib2.HTTPError:
            log.error("Could not find dataset " + mcm_dataset)
            self.mcm_json = None

        # reset gen-sim json
        self.gen_json = None


    # generator level information wrapper
    def getGenInfo(self, key):
        # use gen-sim json if it exists, else start with requested json
        tmp_json = self.gen_json if self.gen_json else self.mcm_json

        if len(tmp_json["results"]) is 0:
            log.error("JSON file is empty. Wrong URL?")
            return None
        
        # check if sample has generator parameters entry
        while len(tmp_json["results"]["generator_parameters"]) is 0:
            # if not, find parent dataset
            input_dataset = tmp_json["results"].get("input_dataset", None)
            if input_dataset is not None:
                tmp_json = self.readJSON(urllib2.urlopen(self.mcm_prefix + input_dataset))
            else:
                log.error("No input dataset specified in JSON. Cannot find GEN-SIM sample.")
                return None

        # store gen json for quicker access upon next call
        self.gen_json = tmp_json
        
        # return value
        value = tmp_json["results"]["generator_parameters"][0].get(key, None)
        if value is None:
            log.error("Could not retrieve " + key + ".")
        return value


    def getCrossSection(self):
        return self.getGenInfo("cross_section")


    def getFilterEfficiency(self):
        return self.getGenInfo("filter_efficiency")


    # sample level information wrapper
    def getInfo(self, key):
        value = self.mcm_json["results"].get(key, None)
        if value is None:
            log.error("Could not retrieve " + key + ".")
        return value


    def getEvents(self):
        return self.getInfo("total_events")


    def getGenerators(self):
        return self.getInfo("generators")[0]


    def getEnergy(self):
        return self.getInfo("energy")


    def getCMSSW(self):
        return self.getInfo("cmssw_release")


    def getWorkingGroup(self):
        return self.getInfo("pwg")



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

if __name__ == "__main__":
    main()
