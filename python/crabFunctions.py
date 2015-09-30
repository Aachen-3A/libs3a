#!/usr/bin/env python
## @package crabFunctions
# This module provides common functions for tasks with crab3
#
# This module provides common functions for tasks with crab3.
# You need no create a CrabController object in order to use the functions
import os,sys
import optparse
import subprocess
import logging
import datetime
import uuid
from  httplib import HTTPException
from multiprocessing import Process, Queue
from CRABAPI.RawCommand import crabCommand
from CRABClient.UserUtilities import getConsoleLogLevel, setConsoleLogLevel, LOGLEVEL_MUTE
from CRABClient.ClientExceptions import CachefileNotFoundException

setConsoleLogLevel(LOGLEVEL_MUTE)


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
        if joblist is not None:
            jobstring ="%s"%','.join(joblist)
        else:
            res = self.callCrabCommand(('resubmit','--wait', os.path.join(self.workingArea,self._prepareFoldername(name)) ))
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
            res={ 'status':"FuckYou",'jobs':{}}
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
    def __init__(self, taskname , crabController = None , initUpdate = True, localDir = "", outlfn = "" , StorageFileList = [] ):
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
        #start with first updates
        if initUpdate:
            self.update()
            self.updateJobStats()

    ## Function to resubmit failed jobs in tasks
    #
    # @param self: CrabTask The object pointer.
    def resubmit_failed( self ):
        failedJobIds = []
        controller =  CrabController()
        for jobkey in crabTask.jobs.keys():
            job = task.jobs[jobkey]
            if job['State'] == 'failed':
                failedJobIds.append( job['JobIds'][-1] )
        controller.resubmit( self.name, joblist = failedJobIds )
        self.lastUpdate = datetime.datetime.now().strftime( "%Y-%m-%d_%H.%M.%S" )

    ## Function to update Task in associated Jobs
    #
    # @param self: CrabTask The object pointer.
    def update(self):
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
        self.nJobs = len(self.jobs.keys())
        self.updateJobStats()
        self.isUpdating = False
        self.lastUpdate = datetime.datetime.now().strftime( "%Y-%m-%d_%H.%M.%S" )
        if "COMPLETE" in self.state:
            if self.nComplete == self.nJobs:
                self.state = "DONE"
            else:
                self.state = "COMPLETE"
        #~ self.lock.release()

    def test_print(self):
        return self.uuid
    ## Function to update JobStatistics
    #
    # @type self: CrabTask
    # @param self: The object pointer.
    # @type dCacheFilelist: list of strings
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
                        outputFilename = "%s_%s"%( sample, key)
                        if 'finished' in statekey and any(outputFilename in s for s in dCacheFileList):
                            nComplete +=1

        for state in stateDict:
            attrname = "n" + state.capitalize()
            setattr(self, attrname, stateDict[state])
        self.nComplete = nComplete

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
