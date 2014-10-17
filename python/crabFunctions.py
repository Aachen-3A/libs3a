#!/usr/bin/env python
## @package crabFunctions
# This module provides common functions for tasks with crab3
#
# This module provides common functions for tasks with crab3.
# The function relies on an optpars option objects. Please use
# crab_commandlineOptions to create a new preset optparse object
# or extend your existing parser.
import os,sys
import optparse
import subprocess

## Check if crab can write to specified site
#
# @type options A optparse options object
# @param options  Options for this function. Used Option(s): workingArea
# @type site string
# @param site The Site symbol [default:T2_DE_RWTH]
# @type path string
# @param path lfn path to check write permission in. see twiki WorkBookCRAB3Tutorial
# @type crablog A logging object instance 
# @param logging Logging object where function adds log messages. Messages
#                 are sent to the prompt if no log object is specified.
def crab_checkwrite(options,site='T2_DE_RWTH',path='noPath',crablog=None):    
    log.info("Checking if user can write in output storage")
    cmd = ['crab checkwrite --site %s --voGroup=dcms'%site ]
    if not 'noPath' in path:
        cmd[0] +=' --lfn=%s'%(path)
    if options.workingArea:
        runPath = options.workingArea
    else:
        runPath ="./"
    p = subprocess.Popen(cmd,stdout=subprocess.PIPE,stderr=subprocess.PIPE,cwd=r"%s"%runPath,shell=True)
    (stringlist,string_err) = p.communicate()
    if not "Able to write to /store/user/"  in stringlist:
        conditionalLog(crablog, "The crab checkwrite command failed for site: %s"%site , 'error')
        conditionalLog(crablog, string_err , 'error')
        return False
    else:
        log.info("Checkwrite was sucessfully called.")
        return True

## Check if crab can write to specified site
#
# @type options A optparse options object
# @param options  Options for this function. Used Option(s): workingArea, dry-run
# @type site string
# @param site The Site symbol [default:T2_DE_RWTH]
# @type name string
# @param name The crab3 request name, a.k.a the sample name
# @type crablog A logging object instance 
# @param logging Logging object where function adds log messages. Messages
#                 are sent to the prompt if no log object is specified.        
def crab_submit(options,name,crablog=None):
    cmd = 'crab submit crab_%s_cfg.py'%name
    if options.workingArea:
        runPath = options.workingArea
    else:
        runPath ="./"
    if options.dry_run:
        conditionalLog(crablog,'Dry-run: Created config file. crab command would have been: %s'%cmd)
    else:
        p = subprocess.Popen(cmd,stdout=subprocess.PIPE,stderr=subprocess.PIPE,stdin=subprocess.PIPE,cwd=r"%s"%runPath,shell=True)
        (stringlist,string_err) = p.communicate()
        conditionalLog(crablog,"crab sumbit called for task %s"%name)

## Returns the hn name for a user with valid proxy
#
# @type options A optparse options object
# @param options  Options for this function. Used Option(s): workingArea
# @type crablog A logging object instance 
# @param logging Logging object where function adds log messages. Messages
#                 are sent to the prompt if no log object is specified. 
def crab_checkHNname(options,crablog=None):
    cmd = 'crab checkHNname --voGroup=dcms'
    if options.workingArea:
        runPath = options.workingArea
    else:
        runPath ="./"
    p = subprocess.Popen(cmd,stdout=subprocess.PIPE,stderr=subprocess.PIPE,cwd=r"%s"%runPath,shell=True)
    (string_out,string_err) = p.communicate()
    string_out = string_out.split("\n")
    for line in string_out:
        if "Your CMS HyperNews username is" in line:
            hnname = line.split(":")[1].strip()
            return hnname
    return "noHNname"

## Check if crab can write to specified site
#
# @type options A optparse options object
# @param options  Options for this function. Used Option(s): workingArea, dry-run
# @type name string
# @param name The crab3 request name, a.k.a the sample name
# @type crablog A logging object instance 
# @param logging Logging object where function adds log messages. Messages
#                 are sent to the prompt if no log object is specified.   
def crab_status(options,name,crablog=None):
    cmd = 'crab status crab_%s --long --json'%name
    if options.workingArea:
        runPath = options.workingArea
    else:
        runPath ="./"
    if options.dry_run:
        conditionalLog(crablog,'Dry-run: Created config file. crab command would have been: %s'%cmd)
    else:
        try:
            p = subprocess.Popen(cmd,stdout=subprocess.PIPE,stderr=subprocess.PIPE,stdin=subprocess.PIPE,cwd=r"%s"%runPath,shell=True)
            (stdout,stderr) = p.communicate()
        except:
            conditionalLog(crablog,"error running crab status subprocess for %s"%name,'error')
            sys.exit(1)
        
        conditionalLog(crablog,"crab status called for task %s"%name)
        conditionalLog(crablog,'crab status crab_%s --long --json'%name)
        #try to fetch the JSON output fro stdout
        try:
            # split output in lines and rverse order
            stdout = stdout.splitlines()
            stdout.reverse()
            # get the json output from stdout    
            rawjson = stdout[1]
            # parse json strin into python json object
            import json
            jsondump = json.dumps(rawjson)
            del rawjson
            statusJSON = json.loads(jsondump)
            del jsondump
            return statusJSON
        except:
            conditionalLog( crablog, 'Error parsing crab status json output, please check cout below ', 'error')
            conditionalLog( crablog, stdout, 'error')
            conditionalLog( crablog, "current working directory %s"%options.workingArea, 'error')
            sys.exit(1)


## Prints log message either to logging object if given or print log message to prompt
#
# @type crablog A logging object instance 
# @param crablog Logging object where function adds log messages. Messages
#                 are sent to the prompt if no log object is specified.  
# @type message string
# @param message  Message which should be written to lof
# @type logtype string
# @param logtype log category for this message (info,error,warning)
def conditionalLog(crablog,message,logtype="info"):
    for basetype in ['info','warning','error']:
        if basetype in logtype:
            try:
                logToCall = getattr(crablog, basetype)
                logToCall(message)
            except:
                print "%s: %s"%(basetype,message)

## Populates a existing optparse parser or returns a new one with all options for crab functions
#
# This functions populates a previously created (or new) instance of a
# optparse parser object with options needed by crab functions.
# It is possible to add three kinds of options:
# - options where a error should be raised if the option was previously defined
# - options where previous definitions should be kept
# - options where previous definitions should be overriden
# @type Optparse parser instance
# @param A previously created parser oject which should be extenden [default: new instance]
# @return A new or extenden optparse parser instance    
def crab_commandlineOptions(parser = optparse.OptionParser( 'usage: %prog' )):
    
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
    if not hasattr(parser, 'dry_run'):
        parser.add_option( '--dry-run', action='store_true', default=False, 
                        help='Do everything except calling CRAB or registering samples to the database.' )
    if not hasattr(parser, 'workingArea'):
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
                         
    
    
