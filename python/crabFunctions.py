#!/usr/bin/env python
## @package crabFunctions
# This module provides common functions for tasks with crab3
#
# This module provides common functions for tasks with crab3.
# You need no create a CrabController object in order to use the functions
import os,sys,glob
import tarfile
import xml.etree.ElementTree as ET
import imp
import optparse
import subprocess
import logging
import datetime
import uuid
from  httplib import HTTPException
from multiprocessing import Process, Queue
from CRABAPI.RawCommand import crabCommand
from CRABClient.UserUtilities import getConsoleLogLevel, setConsoleLogLevel
from CRABClient.ClientUtilities import LOGLEVEL_MUTE
from CRABClient.ClientExceptions import CachefileNotFoundException
setConsoleLogLevel(LOGLEVEL_MUTE)



import gridFunctions
import dbutilscms
import aix3adb
from  aix3adb import Aix3adbException

## The CrabController class
#
# This class can be used to manage Analyses using crab3

class CrabController():

    ## The constructor.
    # @type self: CrabController
    # @param self: The object pointer.
    # @type self: A logging logger instance
    # @param self: A previously defined logger. Crab log messages will use this logger as their parent logger.
    def __init__(self, debug=0, logger = None , workingArea = None, voGroup = None, username = None):
        self.debug = debug
        if workingArea is not None:
            self.workingArea = workingArea
        else:
            self.workingArea = os.getcwd()
        self.dry_run = False
        if voGroup is not None:
            self.voGroup = voGroup
        else:
            self.voGroup = "dcms"
        if username is not None:
            self.username = username
        else:
            self.username = None

        if logger is not None:
            self.logger = logger.getChild("CrabController")
        else:
            # add instance logger as logger to root
            self.logger = logging.getLogger("CrabController")
            # check if handlers are present for root logger
            # we assume that the default logging is not configured
            # if handler is present
            if len(logging.getLogger().handlers) < 1 :
                ch = logging.FileHandler('crabController.log', mode='a', encoding=None, delay=False)
                ch.setLevel(logging.DEBUG)
                # create formatter
                formatter = logging.Formatter( '%(asctime)s - %(name)s - %(levelname)s - %(message)s' )
                # add formatter to ch
                ch.setFormatter(formatter)
                self.logger.addHandler(ch)

        self.crab_q = Queue()
    ## Check if crab can write to specified site
    #
    # @type self: CrabController
    # @param self: The object pointer.
    # @type site string
    # @param site The Site symbol [default:T2_DE_RWTH]
    # @type path string
    # @param path lfn path to check write permission in. see twiki WorkBookCRAB3Tutorial
    # @return boolean which is True if user can write to site and False otherwise
    def checkwrite(self,site='T2_DE_RWTH',path='noPath'):
        if self.username is None: self.checkusername()
        try:
            self.logger.info( "Checking if user can write to /store/user/%s on site %s with voGroup %s"%(self.username,site , self.voGroup) )
            if not 'noPath' in path:
                res = crabCommand('checkwrite','--site',site,'--voGroup',self.voGroup,'--lfn', path)
            else:
                res = crabCommand('checkwrite','--site',site,'--voGroup',self.voGroup)
            if res['status'] == 'SUCCESS':
                self.logger.info("Checkwrite was sucessfully called.")
                return True
            else:
                self.logger.error( "The crab checkwrite command failed for site: %s"%site )
                return False
        except:
            self.logger.error( 'Unable to perform crab checkwrite')
            return False

    ## Check if crab can write to specified site
    #
    # @param self: The object pointer.
    # @type name string
    # @param name The crab3 config file name
    def submit(self,name):
        if self.dry_run:
            res = self.callCrabCommand(('submit', '--dryrun', name))
            self.logger.info('Dry-run: You may check the created config and sandbox')
        else:
            res = self.callCrabCommand(('submit','--wait' , name))
            self.logger.info("crab sumbit called for task %s"%name)
            if self.debug > 1:
                self.logger.info(str(res))
        return res
    ## Resubmit all failed tasks in job or specified list of jobs in task
    #
    # @type self: CrabController
    # @param self: The object pointer.
    # @type name string
    # @param name The crab3 request name, a.k.a the sample name
    # @type joblist list of strings
    # @param joblist The crab3 request name, a.k.a the sample name
    def resubmit(self,name,joblist = None):
        if self.dry_run:
            self.logger.info('Dry-run: Created config file. ')
            return {}
        #~ if joblist is not None:
            #~ jobstring ="%s"%','.join(joblist)
            #~ cmd = ('resubmit','--wait', '--jobids=',jobstring, os.path.join(self.workingArea,self._prepareFoldername(name)) )
        if False:
            pass
        else:
            cmd = ('resubmit','--wait', os.path.join(self.workingArea,self._prepareFoldername(name)) )
        res = self.callCrabCommand( cmd )
        self.logger.info("crab resumbit called for task %s"%name)
        return res
    ## Returns the hn name for a user with valid proxy
    #
    # @type self: CrabController
    # @param self: The object pointer.
    # @returns users hypernews name
    def checkusername(self):
        #depreceated string: cmd = 'crab checkHNname --voGroup=dcms'
        #~ cmd = 'crab checkusername --voGroup=dcms'
        try:
            username = os.environ["CERNUSERNAME"]
            return username
        except:pass
        res = crabCommand('checkusername')
        try:
            self.username = res['username']
            return res['username']
        except:
            return "noHNname"

    ## Check crab status
    #
    # @type self: CrabController
    # @param self: The object pointer.
    # @type name string
    # @param name The crab3 request name, a.k.a the sample name
    def status(self,name):
        if self.dry_run:
            self.logger.info('Dry-run: Created config file. crab command would have been: %s'%cmd)
        else:
            try:
                res = self.callCrabCommand( ('status', '--long', 'crab_%s' % name) )
                #print res
                if 'taskFailureMsg' in res and 'jobs' in res:
                    return res['status'], res['jobs'], res['taskFailureMsg']
                elif 'jobs' in res and 'taskFailureMsg' not in res:
                    return res['status'], res['jobs'],None
                elif 'jobs' not in res and 'taskFailureMsg' in res:
                    return res['status'], {},res['taskFailureMsg']
                else:
                     return res['status'],{},None
            except Exception as e:
                print e
                self.logger.error("Can not run crab status request")
                return "NOSTATE",{},None

    ## Call crab command in a new process and return result dict
    #
    # @param self The object pointer
    # @param crabArgs A list of arguments for crab beginning with the command
    def callCrabCommand( self, crabArgs ):
        crabCommandProcessArgs = (self.crab_q, crabArgs)
        p = Process(target=crabCommandProcess, args=(crabCommandProcessArgs))
        p.start()
        res = self.crab_q.get()
        p.join()
        return res

    ## Call crab getlog
    #
    # @param self: The object pointer.
    # @type name string
    def getlog(self, name):
        foldername = self._prepareFoldername( name)
        try:
            #res = crabCommand('--quiet','status', dir = 'crab_%s' % name)
            res = self.callCrabCommand( ('getlog',  '%s' % foldername) )
            return res['success'], res['failed']
        except:
            self.logger.error("Error calling crab getlog for %s" %foldername)
            return {}, {}

    ## Read a crab config and return python object
    #
    # @param self: The object pointer.
    # @param name The sample name (crab request name)
    def readCrabConfig( self, name ):
        pset = 'crab_%s_cfg.py' % name
        with open( pset, 'r') as cfgfile:
            cfo = imp.load_source("pycfg", pset, cfgfile )
            config = cfo.config
            del cfo
        return config

    ## Return list of all crab folders in workin area (default cwd)
    #
    # @param self The object pointer
    #
    @property
    def crabFolders(self):
        results = []
        dirlist = [ x for x in os.listdir( self.workingArea ) if (x.startswith('crab_') and os.path.isdir( os.path.join(self.workingArea,x) ) )]
        return dirlist

    ## Add crab_ to Foldername if needed
    #
    # @param getlog(self, name)
    def _prepareFoldername(self, name):
        if name.startswith("crab_"):
            crabfolder = '%s'%name
        else:
            crabfolder = "crab_%s "%name
        return crabfolder.strip()
    ## Populates an existing optparse parser or returns a new one with options for crab functions
    #
    # This functions populates a previously created (or new) instance of a
    # optparse parser object with options needed by crab functions.
    # It is possible to add three kinds of options:
    # - options where a error should be raised if the option was previously defined
    # - options where previous definitions should be kept
    # - options where previous definitions should be overriden
    # @type Optparse parser instance
    # @param parser A previously created parser oject which should be extenden [default: new instance]
    # @return A new or extenden optparse parser instance
    def commandlineOptions(self,parser = optparse.OptionParser( 'usage: %prog' )):
        # we first need to call parse_args with a dummy string at the beginning to
        # check for existing options later
        (currentoptions, args ) = parser.parse_args([" "])

        # The following block shows how variables should be added, where
        # conflicts are possible if the option is overridden by this function
        # they raise a value error
        #try:
        #    parser.add_option( '--someOption',metavar='DIR',default=None,
        #                       help='Dummy option for future integration')
        #except OptionConflictError as e:
        #    conditionalLog(crablog,"There are conflicts extending the optparse options object",'error')
        #    conditionalLog(crablog,e.strerror,'error')

        # options where it is checked if they exists and new options are added
        # otherwise
        if not hasattr(currentoptions, 'dry_run'):
            parser.add_option( '--dry-run', action='store_true', default=False,
                            help='Do everything except calling CRAB or registering samples to the database.' )
        if not hasattr(currentoptions, 'workingArea'):
            parser.add_option( '--workingArea',metavar='DIR',default=os.getcwd(),help='The area (full or relative path) where to create the CRAB project directory. '
                                 'If the area doesn\'t exist, CRAB will try to create it using the mkdir command' \
                                ' (without -p option). Defaults to the current working directory.'       )


        # Some options can be added without expected trouble with other parser
        # parts, simply because it is quite fixed what they represent.
        # those variables should be added here and will throw no exception if
        # they already exist in the parser
        #parser.set_conflict_handler('resolve')
        #parser.add_option( '--someOption',metavar='DIR',default=None,
        #                       help='Dummy option for future integration')

        return parser

## Function to run crab command in a new process
#
# Some CRAB commands (e.g. submit) create broken cmssw process objects
# when they are created in multiple calls of crabCommand via CRAB API
# Running them in a new process is a workaround, see
# https://twiki.cern.ch/twiki/bin/view/CMSPublic/CRAB3FAQ#Multiple_submission_fails_with_a
def crabCommandProcess(q,crabCommandArgs):
    # give crab3 the chance for one server glitch
    i=0
    while True:
        i+=1
        try:
            res = crabCommand(*crabCommandArgs)
            break
        except HTTPException as e:
            print "crab error ---------------"
            print e
            print "end error ---------------"
            print "will try again!"
            import time
            time.sleep(5)
        except CachefileNotFoundException as e:
            print "crab error ---------------"
            print e
            print "end error ---------------"
            print crabCommandArgs
            res={ 'status':"CachefileNotFound",'jobs':{}}
            break
        if i>5:
            res={ 'status':"YouAreFuckedByCrab",'jobs':{}}
            break
    q.put( res )

## Class for a single CrabRequest
#
# This class represents one crab3 task/request
class CrabTask:

    ## The object constructor
    #
    # @type self: CrabTask
    # @param self: The object pointer.
    # @type taskname: String
    # @param taskname: The object pointer.
    # @type initUpdate: Boolean
    # @param initUpdate: Flag if crab status should be called when an instance is created
    def __init__(self, taskname ,
                       crabController = None ,
                       initUpdate = True,
                       dblink = None,
                       localDir = "",
                       outlfn = "" ,
                       StorageFileList = [] ):
        self.name = taskname
        self.uuid = uuid.uuid4()
        #~ self.lock = multiprocessing.Lock()
        self.jobs = {}
        self.localDir = localDir
        self.outlfn = outlfn
        self.StorageFileList = StorageFileList
        self.isUpdating = False
        self.taskId = -1
        #variables for statistics
        self.nJobs = 0
        self.state = "NOSTATE"
        self.maxjobnumber = 0
        self.nUnsubmitted   = 0
        self.nIdle = 0
        self.nRunning = 0
        self.nTransferring    = 0
        self.nCooloff    = 0
        self.nFailed    = 0
        self.nFinished    = 0
        self.nComplete    = 0
        self.failureReason = None
        self.lastUpdate = datetime.datetime.now().strftime( "%Y-%m-%d_%H.%M.%S" )

        self._isData = None
        self.dblink = dblink
        self.resubmitCount = 0

        self.debug = False

        self.finalFiles = []
        self.totalEvents = 0
        # crab config as a python object should only be used via .config
        self._crabConfig = None
        #start with first updates
        if initUpdate:
            self.update()
            self.updateJobStats()

    ## Property function to find out if task runs on data
    #
    # @param self: CrabTask The object pointer.
    @property
    def isData( self ):
        if self._isData is None:
            try:
                test = self.crabConfig.Data.lumiMask
                self._isData = True
            except:
                self._isData = False
        return self._isData

    ## Function to access crab config object or read it if unititalized
    #
    # @param self: CrabTask The object pointer.
    @property
    def crabConfig( self ):
        if self._crabConfig is None:
            crab = CrabController()
            self._crabConfig = crab.readCrabConfig( self.name )
        return self._crabConfig

    ## Function to resubmit failed jobs in tasks
    #
    # @param self: CrabTask The object pointer.
    def resubmit_failed( self ):
        failedJobIds = []
        controller =  CrabController()
        for jobkey in self.jobs.keys():
            job = self.jobs[jobkey]
            if job['State'] == 'failed':
                failedJobIds.append( job['JobIds'][-1] )
        controller.resubmit( self.name, joblist = failedJobIds )
        self.lastUpdate = datetime.datetime.now().strftime( "%Y-%m-%d_%H.%M.%S" )

    ## Function to update Task in associated Jobs
    #
    # @param self: CrabTask The object pointer.
    def update(self, updateDB = False):
        #~ self.lock.acquire()
        self.isUpdating = True
        controller =  CrabController()
        self.state = "UPDATING"
        self.state , self.jobs,self.failureReason = controller.status(self.name)
        if self.state=="FAILED":
            import time
            #try it once more
            time.sleep(2)
            self.state , self.jobs,self.failureReason = controller.status(self.name)
        if self.state == "NOSTATE":
            if self.resubmitCount < 3: self.self.handleNoState()
        if self.state == "COMPLETED" and self.isFinal:
            self.state = "FINAL"
        self.nJobs = len(self.jobs.keys())
        self.updateJobStats()
        self.isUpdating = False
        self.lastUpdate = datetime.datetime.now().strftime( "%Y-%m-%d_%H.%M.%S" )
        #~ self.lock.release()

    ## Function to handle Task which received NOSTATE status
    #
    # @param self: CrabTask The object pointer.
    def handleNoState( self ):
        crab = CrabController()
        if "The CRAB3 server backend could not resubmit your task because the Grid scheduler answered with an error." in task.failureReason:
            # move folder and try it again
            cmd = 'mv %s bak_%s' %(crab._prepareFoldername( self.name ),crab._prepareFoldername( self.name ))
            p = subprocess.Popen(cmd,stdout=subprocess.PIPE, shell=True)#,shell=True,universal_newlines=True)
            (out,err) = p.communicate()
            self.state = "SHEDERR"
            configName = '%s_cfg.py' %(crab._prepareFoldername( self.name ))
            crab.submit( configName )

        elif task.failureReason is not None:
            self.state = "ERRHANDLE"
            crab.resubmit( self.name )
        self.resubmitCount += 1

    def test_print(self):
        return self.uuid
    ## Function to update JobStatistics
    #
    # @param self: The object pointer.
    # @param dCacheFilelist: A list of files on the dCache
    def updateJobStats(self,dCacheFileList = None):
        jobKeys = sorted(self.jobs.keys())
        try:
            intJobkeys = [int(x) for x in jobKeys]
        except:
            print "error parsing job numers to int"

        #maxjobnumber = max(intJobkeys)

        stateDict = {'unsubmitted':0,'idle':0,'running':0,'transferring':0,'cooloff':0,'failed':0,'finished':0}
        nComplete = 0

        # loop through jobs
        for key in jobKeys:
            job = self.jobs[key]
             #check if all completed files are on decache
            for statekey in stateDict.keys():
                if statekey in job['State']:
                    stateDict[statekey]+=1
                    # check if finished fails are found on dCache if dCacheFilelist is given
                    if dCacheFileList is not None:
                        outputFilename = "%s_%s"%( self.name, key)
                        if 'finished' in statekey and any(outputFilename in s for s in dCacheFileList):
                            nComplete +=1

        for state in stateDict:
            attrname = "n" + state.capitalize()
            setattr(self, attrname, stateDict[state])
        self.nComplete = nComplete

    ## Function to read log info from log.tar.gz
    #
    # @param self: The object pointer.
    # @param logArchName: path to the compressed log file
    # @return a dictionary with parsed info
    def readLogArch(self, logArchName):
        JobNumber = logArchName.split("/")[-1].split("_")[1].split(".")[0]
        log = {'readEvents' : 0}
        with tarfile.open( logArchName, "r") as tar:
            try:
                JobXmlFile = tar.extractfile('FrameworkJobReport-%s.xml' % JobNumber)
                root = ET.fromstring( JobXmlFile.read() )
                for child in root:
                    if child.tag == 'InputFile':
                        for subchild in child:
                            if subchild.tag == 'EventsRead':
                                nEvents = int(subchild.text)
                                log.update({'readEvents' : nEvents})
                                break
                        break
            except:
                print "Can not parse / read %s" % logArchName
        return log

    ## Function to finalize task in TAPAS workflow
    #
    # Get config files and submit samples
    def finalizeTask(self , update = False, debug= False ):

        outlfn = self.crabConfig.Data.outLFNDirBase.split('/store/user/')[1]
        if outlfn.endswith("/"): outlfn =outlfn[:-1]
        crab = CrabController()
        # Check files for each job
        dCacheFiles = gridFunctions.getdcachelist( outlfn )
        #~ success , failed = crab.getlog( sample )
        if debug:
            cmd = 'crab log %s' % crab._prepareFoldername( self.name )
        else:
            # CRAB likes to be verbos, even if you tell it to be quiet...
            cmd = 'crab --quiet log %s' % crab._prepareFoldername( self.name )
        p = subprocess.Popen(cmd,stdout=subprocess.PIPE,stderr=subprocess.PIPE, shell=True)#,shell=True,universal_newlines=True)
        (out,err) = p.communicate()
        #~ print out
        crabFolder = crab._prepareFoldername( self.name )
        #~ print crabFolder
        logArchs = glob.glob("%s/%s/results/*.log.tar.gz" % (self.crabConfig.General.workArea,crabFolder))
        finalFiles = []
        totalEvents = 0
        #~ sys.exit()
        for logArchName in logArchs:
            JobNumber = logArchName.split("/")[-1].split("_")[1].split(".")[0]
            #~ print logArchName
            log = self.readLogArch( logArchName )
            # check if file on dCache
            dfile = []
            for layer in dCacheFiles:
                dfile += [s for s in layer if "%s_%s.pxlio" %( self.name, JobNumber ) in s ]
            if len(dfile)  > 0 and log['readEvents'] > 0 :
                finalFiles.append(  {'path':dfile[0], 'nevents':log['readEvents']} )
                totalEvents += log['readEvents']
        self.finalFiles = finalFiles
        self.totalEvents = totalEvents
        if self.isData:
            self.addData2db( update )
        else:
            self.addMC2db( update )

        with open('finalSample','a') as outfile:
            outfile.write("%s:%s\n" % ( self.name,  self.crabConfig.Data.inputDataset))
        self.state = "FINAL"

    def addData2db( self, update=False):
        crab = crabFunctions.CrabController()
        # try to get sample db entry and create it otherwise
        newInDB = False
        try:
            self.dbSample = self.dblink.getDataSample( self.name  )
        except Aix3adbException:
            newInDB = True
            self.dbSample = aix3adb.DataSample( )
            self.dbSample.name = self.name
        # update fields
        self.dbSample.energy = 13

        if newInDB:
            self.dblink.insertDataSample( self.dbSample )
            # get sample again with its new id
            self.dbSample = self.dblink.getDataSample( self.name )
        else:
            self.dblink.editDataSample( self.dbSample )

        if update and not newInDB:
            self.dbSkim, self.dbSample  =  self.dblink.getDataLatestSkimAndSampleBySample( self.dbSample.name )
        else:
            self.dbSkim = aix3adb.MCSkim()

        self.fillCommonSkimFields( )
        ## fill additional fields for data
        self.dbSkim.jsonfile = self.crabConfig.Data.lumiMask.split("/")[-1]
        if update:
            self.dblink.editDataSkim( self.dbSkim )
        else:
            self.dblink.insertDataSkim( self.dbSkim )

#~ update

    def addMC2db( self, update = False ):
        crab = CrabController()
        generators = {}
        generators.update({ 'MG':'madgraph' })
        generators.update({ 'PH':'powheg' })
        generators.update({ 'HW':'herwig6' })
        generators.update({ 'HP':'herwigpp' })
        generators.update({ 'HW':'herwig' })
        generators.update({ 'SP':'sherpa' })
        generators.update({ 'MC':'mcatnlo' })
        generators.update({ 'AG':'alpgen' })
        generators.update({ 'CA':'calchep' })
        generators.update({ 'CO':'comphep'  })
        generators.update({ 'P6':'pythia6' })
        generators.update({ 'P8':'pythia8' })
        generators.update({ 'PY':'pythia8' })
        newInDB = False
            # get infos from McM
        mcmutil = dbutilscms.McMUtilities()
        mcmutil.readURL( self.crabConfig.Data.inputDataset )
        # try to get sample db entry and create it otherwise
        try:
            self.dbSample = self.dblink.getMCSample( self.name )
        except Aix3adbException:
            self.dbSample = aix3adb.MCSample()
            newInDB = True
            self.dbSample.name = self.name
            self.dbSample.generator = generators[ self.name.split("_")[-1] ]
            self.dbSample.crosssection = str(mcmutil.getCrossSection())
            self.dbSample.crosssection_reference = 'McM'
            self.dbSample.filterefficiency = mcmutil.getGenInfo('filter_efficiency')
            self.dbSample.filterefficiency_reference = 'McM'
            self.dbSample.kfactor = 1.
            self.dbSample.kfactor_reference = "None"
            self.dbSample.energy = mcmutil.getEnergy()

        if newInDB:
            self.dbSample = self.dblink.insertMCSample( self.dbSample )
        else:
            self.dbSample = self.dblink.editMCSample( self.dbSample )

        if update and not newInDB:
            self.dbSkim, self.dbSample  =  self.dblink.getMCLatestSkimAndSampleBySample( self.dbSample.name )
        else:
            self.dbSkim = aix3adb.MCSkim()

        self.fillCommonSkimFields( )

        if update:
            self.dblink.editMCSkim( self.dbSkim )
        else:
            self.dblink.insertMCSkim( self.dbSkim )

    def fillCommonSkimFields( self ):
        # create relation to dbsample object
        self.dbSkim.sampleid = self.dbSample.id
        self.dbSkim.datasetpath = self.crabConfig.Data.inputDataset
        outlfn = self.crabConfig.Data.outLFNDirBase.split('/store/user/')[1]
        crab = CrabController()
        self.dbSkim.owner = outlfn.split("/")[0]
        self.dbSkim.iscreated = 1
        self.dbSkim.isfinished = 1
        self.dbSkim.isdeprecated  = 0
        now = datetime.datetime.now()
        self.dbSkim.files = self.finalFiles
        self.dbSkim.created_at = now.strftime( "%Y-%m-%d %H-%M-%S" )
        # where to get the skimmer name ??? MUSiCSkimmer fixed
        self.dbSkim.skimmer_name = "PxlSkimmer"
        self.dbSkim.skimmer_version = outlfn.split("/")[2]
        self.dbSkim.skimmer_cmssw = os.getenv( 'CMSSW_VERSION' )
        self.dbSkim.skimmer_globaltag = [p.replace("globalTag=","").strip() for p in self.crabConfig.JobType.pyCfgParams if "globalTag" in p][0]
        self.dbSkim.nevents = str( self.totalEvents )

    @property
    def isFinal( self ):
        # find out if we work on data or mc
        try:
            test = self.crabConfig.Data.lumiMask
            runOnMC = False
        except:
            runOnMC = True
        runOnData = not runOnMC
        # fill search criteria for skim and samples
        skimCriteria = {}
        sampleCriteria = {}

        outlfn = self.crabConfig.Data.outLFNDirBase.split('/store/user/')[1]
        skimCriteria["owner"] = outlfn.split("/")[0]
        skimCriteria["skimmer_version"] = outlfn.split("/")[2]

        sampleCriteria["name"] = self.name
        try:
            if runOnMC:
                searchResult = self.dblink.searchMCSkimsAndSamples( skimCriteria, sampleCriteria )
            if runOnData:
                searchResult = self.dblink.searchDataSkimsAndSamples( skimCriteria, sampleCriteria )
        except Aix3adbException:
            return False
        except:
            return False
        if len(searchResult) > 0:
            self.state = "FINAL"
            return True
        return False

## Class holds job statistics for several Crab tasks
#
# This class saves and updates statistics from a given list of CrabTask objects.
class TaskStats:

    ## The object constructor
    #
    # @type self: TaskStats
    # @param self: The object pointer.
    # @type tasklist: List of CrabTask objects
    # @param tasklist: (Optional) List of CrabTasks for which statistics should be calculated
    def __init__(self, tasklist = None):
        if tasklist is not None:
            self.updateStats(tasklist)
        else:
            self.clearStats()

    ## This function updates the statistics for a given tasklist
    #
    # @type self: TaskStats
    # @param self: The object pointer.
    # @type tasklist: List of CrabTask objects
    # @param tasklist: List of CrabTasks for which statistics should be calculated
    def updateStats(self,tasklist):
        self.clearStats()
        self.nTasks = len(tasklist)
        for task in tasklist:
            if not task.isUpdating:
                self.nUnsubmitted   += task.nUnsubmitted
                self.nIdle += task.nIdle
                self.nRunning += task.nRunning
                self.nTransferring    += task.nTransferring
                self.nCooloff    += task.nCooloff
                self.nFailed    += task.nFailed
                self.nFinished    += task.nFinished
                self.nComplete    += task.nComplete

    ## This function sets all counts to zero
    #
    # @type self: TaskStats
    # @param self: The object pointer.
    def clearStats(self):
        self.nTasks = 0
        self.nUnsubmitted   = 0
        self.nIdle = 0
        self.nRunning = 0
        self.nTransferring    = 0
        self.nCooloff    = 0
        self.nFailed    = 0
        self.nFinished    = 0
        self.nComplete    = 0
