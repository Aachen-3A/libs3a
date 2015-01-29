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

import multiprocessing

## The CrabController class
#
# This class can be used to manage Analyses using crab3

class CrabController():
    
    ## The constructor.
    # @param self: The object pointer.
    # @param logger: A previously defined logger instance. Crab log messages will use this logger as their parent logger.
    # @param workingArea path to a workingArea where subprocesses should be run (e.g. crab commands)
    # @param voGroup The virtual organisation you belong to
    # @param username Useres Hypernews username
    def __init__(self, logger = None , workingArea = None, voGroup = None, username = None):
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
            #~ raise(Exception)
            # add instance logger as logger to root
            self.logger = logging.getLogger("CrabController")
            # check if handlers are present for root logger
            # we assume that the default logging is not configured
            # if handler is present
            if len(logging.getLogger().handlers) < 1 :
                logging.basicConfig( level=logging._levelNames[ 'DEBUG' ])
                
                # create console handler and set level to debug
                ch = logging.StreamHandler()
                ch.setLevel(logging.DEBUG)
                # create formatter
                formatter = logging.Formatter( '%(asctime)s - %(name)s - %(levelname)s - %(message)s' )
                
                # add formatter to ch
                ch.setFormatter(formatter)
                
                # add ch to logger
                logging.getLogger().addHandler(ch)
                
    ## Check if crab can write to specified site
    #
    # @param self: The object pointer.
    # @param site The Site symbol [default:T2_DE_RWTH]
    # @param path lfn path to check write permission in. see twiki WorkBookCRAB3Tutorial
    # @return boolean which is True if user can write to site and False otherwise
    def checkwrite(self,site='T2_DE_RWTH',path='noPath'):    
        cmd = ['crab checkwrite --site %s --voGroup=%s'% ( site, self.voGroup ) ]
        if not 'noPath' in path:
            cmd[0] +=' --lfn=%s'%(path)

        if self.username is None: self.checkusername()
        self.logger.info( "Checking if user can write to /store/user/%s on site %s with voGroup %s"%(self.username, site, self.voGroup) ) 

        p = subprocess.Popen(cmd,stdout=subprocess.PIPE,stderr=subprocess.PIPE,cwd=r"%s"%self.workingArea,shell=True)
        (stringlist,string_err) = p.communicate()
        if not "Able to write in /store/user/%s on site %s"%(self.username,site)  in stringlist:
            self.logger.error( "The crab checkwrite command failed for site: %s"%site )
            self.logger.debug( stringlist )
            self.logger.debug( string_err )
            return False
        else:
            self.logger.info("Checkwrite was sucessfully called.")
            return True
    
    ## Check if crab can write to specified site
    #
    # @param self: The object pointer.
    # @param name The crab3 request name, a.k.a the sample name
    def submit(self,name):
        cmd = "crab submit --voGroup=%s %s"%( self.voGroup ,name)
        if self.dry_run:
            self.logger.info('Dry-run: Created config file. crab command would have been: %s'%cmd)
        else:
            p = subprocess.Popen(cmd,stdout=subprocess.PIPE,stderr=subprocess.PIPE,stdin=subprocess.PIPE,cwd=r"%s"%self.workingArea,shell=True)
            (stringlist,string_err) = p.communicate()
            self.logger.info("crab sumbit called for task %s"%name)
            #~ self.logger.error( stringlist )
            #~ self.logger.error( string_err )

    ## Resubmit all failed tasks in job or specified list of jobs in task
    #
    # @param self: The object pointer.
    # @param name The crab3 request name, a.k.a the sample name
    # @param joblist The crab3 request name, a.k.a the sample name
    def resubmit(self,name,joblist = None):
        if str.startswith(name, "crab_ "):
            cmd = 'crab resubmit %s'%name
        else:
            cmd = "crab resubmit crab_%s "%name
        if joblist is not None:
            cmd+="--jobids %s"%','.join(joblist)
        if self.dry_run:
            self.logger.info('Dry-run: Created config file. crab command would have been: %s'%cmd)
        else:
            p = subprocess.Popen(cmd,stdout=subprocess.PIPE,stderr=subprocess.PIPE,stdin=subprocess.PIPE,cwd=r"%s"%self.workingArea,shell=True)
            (stringlist,string_err) = p.communicate()
            self.logger.info("crab resumbit called for task %s"%name)
    
    ## Returns the hn name for a user with valid proxy
    #
    # @param self: The object pointer.
    # @returns users hypernews name
    def checkusername(self):
        #depreceated string: cmd = 'crab checkHNname --voGroup=dcms'
        cmd = 'crab checkusername --voGroup=dcms'
        p = subprocess.Popen(cmd,stdout=subprocess.PIPE,stderr=subprocess.PIPE,cwd=r"%s"%self.workingArea,shell=True)
        (string_out,string_err) = p.communicate()
        string_out = string_out.split("\n")
        for line in string_out:
            # depreceated string: if "Your CMS HyperNews username is" in line:
            if "Username is" in line:
                hnname = line.split(":")[1].strip()
                self.username = hnname
                return hnname
        return "noHNname"
    
    ## Check crab status
    #
    # @param self: The object pointer.
    # @param name The crab3 request name, a.k.a the sample name (string)
    def status(self,name):
        cmd = 'crab status crab_%s --long --json'%name
        if self.dry_run:
            self.logger.info('Dry-run: Created config file. crab command would have been: %s'%cmd)
        else:
            try:
                p = subprocess.Popen(cmd,stdout=subprocess.PIPE,stderr=subprocess.PIPE,stdin=subprocess.PIPE,cwd=r"%s"%self.workingArea,shell=True)
                (stdout,stderr) = p.communicate()
            except:
                self.logger.error("error running crab status subprocess for %s"%name)
                sys.exit(1)
            
            #~ self.logger.info(stdout)
            self.logger.info("crab status called for task %s"%name)
            self.logger.info('crab status crab_%s --long --json'%name)
            #try to fetch the JSON output fro stdout
            try:
                # split output in lines and rverse order
                stdout = stdout.splitlines()
                for line in stdout:
                    if line.strip().startswith("Task status:"):
                        state = line.split(":")[1].strip() 
                stdout.reverse()
                # get the json output from stdout    
                rawjson = stdout[1]
                # parse json strin into python json object
                import json
                jsondump = json.dumps(rawjson)
                del rawjson
                statusJSON = json.loads(jsondump)
                del jsondump
                import ast
                try:
                    statusDict = ast.literal_eval(statusJSON)
                    return state,statusDict
                except:
                    self.logger.error("Can not parse Crab request JSON output")
                    return "NOSTATE",{}
            except:
                self.logger.error( "Error: current working directory %s"%self.workingArea)
                self.logger.error('Error parsing crab status json output, please check cout below ')
                self.logger.error(stdout)
                sys.exit(1)
    
    ## Populates an existing optparse parser or returns a new one with options for crab functions
    #
    # This functions populates a previously created (or new) instance of a
    # optparse parser object with options needed by crab functions.
    # It is possible to add three kinds of options:
    # - options where a error should be raised if the option was previously defined
    # - options where previous definitions should be kept
    # - options where previous definitions should be overriden
    #
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


## Class for a single CrabRequest
#
# This class represents one crab3 task/request
class CrabTask:
    
    ## The object constructor
    #
    # @param self The object pointer.
    # @param taskname The name of the crab Task
    # @param crabController A crabController object if already initalized, a new one is created if None
    # @param initUpdate Boolean flag if crab status should be called when an instance is created
    # @param localDir The local directory for the crab task
    # @param outlfn The destination for the crab job output on your local GRID storage element
    # @param StorageFileList A list of files for this task on your GRID storage element
    def __init__(self, taskname , crabController = None , initUpdate = True, localDir = "", outlfn = "" , StorageFileList = [] ):
        self.name = taskname
        self.uuid = uuid.uuid4()
        #~ self.lock = multiprocessing.Lock()       
        if crabController is None:
            self.controller =  CrabController()
        else:
            self.controller =  crabController
        self.jobs = {}
        self.localDir = localDir
        self.outlfn = outlfn
        self.StorageFileList = StorageFileList
        self.isUpdating = False
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
        
        #start with first updates
        if initUpdate:
            self.update()
            self.updateJobStats()
            
    ## Function to update Task in associated Jobs
    #
    # @param self: The object pointer.        
    def update(self):
        #~ self.lock.acquire()
        self.isUpdating = True
        self.state = "UPDATING"
        self.state , self.jobs = self.controller.status(self.name) 
        self.nJobs = len(self.jobs.keys())
        self.updateJobStats()
        self.isUpdating = False
        self.lastUpdate = datetime.datetime.now().strftime( "%Y-%m-%d_%H.%M.%S" )
        #~ if "COMPLETE" in self.state:
            #~ if nComplete == nJobs:
                #~ self.state = "DONE"
            #~ else:
                #~ self.state = "COMPLETE"
        #~ self.lock.release()
        
    ## Function to update JobStatistics
    #
    # @param self: The object pointer.           
    # @param dCacheFileList: A list of files on the dCache           
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
    # @param self: The object pointer.
    # @param tasklist: (Optional) List of CrabTasks for which statistics should be calculated
    def __init__(self, tasklist = None):
        if tasklist is not None:
            self.updateStats(tasklist)
        else:
            self.clearStats()
            
    ## This function updates the statistics for a given tasklist
    #
    # @param self: The object pointer.
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
