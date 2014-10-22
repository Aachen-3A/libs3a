#! /usr/bin/env python2
import time
import sys
import os
import subprocess
import pickle
import shutil
from collections import defaultdict
import multiprocessing
import logging
import glob
#logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)
#log = multiprocessing.get_logger()

def submitWorker(job):
    job.submit()
    return job

def resubmitWorker(job):
    job.resubmit()
    return job

class Job:
    def __init__(self):
        self.inputfiles, self.outputfiles, self.arguments , self.executable = [], [], [], None
        self.frontEndStatus = ""
        self.jobid=None
    @property
    def status(self):
        try:
            status=str(self.infos["Status"])
        except:
            status="None"
        return status
    def writeJdl(self):
        if self.executable is None: self.executable = self.task.executable
        jdl = (
            '[Type = "Job";\n'
            'VirtualOrganisation = "cms";\n'
            'AllowZippedISB = true;\n'
            #'Requirements = (RegExp("rwth-aachen.de", other.GlueCEUniqueId)) && (RegExp("cream", other.GlueCEUniqueId)) && !(RegExp("short", other.GlueCEUniqueId));\n'
            'ShallowRetryCount = 10;\n'
            'RetryCount = 3;\n'
            'MyProxyServer = "";\n'
            'executable = "prologue.sh";\n'
            'StdOutput = "out.txt";\n'
            'StdError  = "err.txt";\n'
            'outputsandboxbasedesturi="gsiftp://localhost";\n'
            )

        jdl += 'InputSandbox = { "' + ('", "'.join(["./prologue.sh", self.executable]+self.inputfiles+self.task.inputfiles)) + '"};\n'
        stds=["out.txt", "err.txt"]
        jdl += 'OutputSandbox = { "' + ('", "'.join(stds+self.outputfiles+self.task.outputfiles)) + '"};\n'
        jdl += 'Arguments = "' + (' '.join(["./"+os.path.basename(self.executable)] + self.arguments)) + '";\n'
        jdl += "]"
        self.jdlfilename = "job"+str(self.nodeid)+".jdl"
        jdl_file = open(self.jdlfilename, 'w')
        jdl_file.write(jdl)
        jdl_file.close()
        self.frontEndStatus = "JDLWRITTEN"
    def submit(self):
        startdir = os.getcwd()
        if startdir!=self.task.directory:
            os.chdir(self.task.directory)

        #get a proxy for at least 4 days
        checkAndRenewVomsProxy(604800)
        for i in range(50):
            #command = ['glite-ce-job-submit', '-a', '-r', 'ce201.cern.ch:8443/cream-lsf-grid_cms', self.jdlfilename]
            command = ['glite-ce-job-submit', '-a', '-r', self.task.ceId, self.jdlfilename]
            process = subprocess.Popen(command, stdout=subprocess.PIPE)
            stdout, stderr = process.communicate()
            if "FATAL" in stdout and "Submissions are disabled!" in stdout:
                print "Submission server seems busy (Submissions are disabled). Waiting..."
                time.sleep(60*(i+1))
                continue
            if "FATAL - jobRegister" in stdout:
                print "Submission server seems busy (jobRegister). Waiting..."
                time.sleep(60*(i+1))
                continue
            if "FATAL" in stdout and "Connection timed out" in stdout:
                print "Submission server seems busy (Connection timed out). Waiting..."
                time.sleep(60*(i+1))
                continue
            if "FATAL - EOF detected during communication" in stdout:
                print "Submission server seems busy (EOF detected during communication). Waiting..."
                time.sleep(60*(i+1))
                continue
            if "FATAL" in stdout or "ERROR" in stdout or process.returncode != 0:
                log.error('Submission failed.')
                log.error('Output:\n' + stdout)
                self.error = stdout
                break
            self.frontEndStatus = "SENT"
            log.info('Submission successful.')
            for line in stdout.splitlines():
                if "https://" in line:
                    self.jobid = line.strip()
                    log.info("Submitted job "+self.jobid)
            break
        os.chdir(startdir)
        return process.returncode, stdout
    def getStatus(self):
        if self.frontEndStatus == "RETRIEVED" or self.frontEndStatus == "PURGED":
            return
        try:
            if self.jobid is None:
                return
        except AttributeError:
            return
        command = ["glite-ce-job-status", self.jobid]
        log.debug("Getting status "+self.jobid)
        process = subprocess.Popen(command, stdout=subprocess.PIPE)
        stdout, stderr = process.communicate()
        if process.returncode!=0:
            log.warning('Status retrieval failed for job id '+self.jobid)
            log.info(stdout)
            log.info(stderr)
        self.infos = parseStatus(stdout)
    def getOutput(self):
        log.debug("Getting output "+self.jobid)
        command = ["glite-ce-job-output", "--noint", "--dir", self.task.directory, self.jobid]
        process = subprocess.Popen(command, stdout=subprocess.PIPE)
        stdout, stderr = process.communicate()
        if process.returncode!=0:
            log.warning('Output retrieval failed for job id '+self.jobid)
        else:
            self.purge()
            self.frontEndStatus = "RETRIEVED"
    def cancel(self):
        log.debug("Canceling "+self.jobid)
        command = ["glite-ce-job-cancel", "--noint", self.jobid]
        process = subprocess.Popen(command, stdout=subprocess.PIPE)
        stdout, stderr = process.communicate()
        if process.returncode!=0:
            log.warning('Cancelling failed for job id '+self.jobid)
        else:
            self.frontEndStatus = "CANCELLED"
    def purge(self):
        log.debug("Purging "+self.jobid)
        command = ["glite-ce-job-purge", "--noint", self.jobid]
        process = subprocess.Popen(command, stdout=subprocess.PIPE)
        stdout, stderr = process.communicate()
        if process.returncode!=0:
            log.warning('Purging failed for job id '+self.jobid)
        else:
            self.frontEndStatus = "PURGED"
    def resubmit(self):
        if self.status in ["PENDING", "IDLE", "RUNNING", "REALLY-RUNNING", "HELD"]:
            self.cancel()
        self.purge()
        self.cleanUp()
        self.infos=dict()
        self.submit()
    @property
    def outputSubDirectory(self):
        return str(self.jobid).replace("https://","").replace(":","_").replace("/","_")


class Task:
    @classmethod
    def load(cls, directory):
        f = open(os.path.join(directory, "task.pkl"),'rb')
        obj = pickle.load(f)
        f.close()
        obj.directory = os.path.abspath(directory)
        obj.mode = "OPEN"
        # for downward compatibility with old task.pkl files. This can be removed in the future
        if not 'ceId' in obj.__dict__:
            obj.ceId = 'grid-ce.physik.rwth-aachen.de:8443/cream-pbs-cms'
        return obj
    def __init__(self, name, directory = None, mode="RECREATE", scramArch='slc5_amd64_gcc462', cmsswVersion='CMSSW_5_3_14', ceId='grid-ce.physik.rwth-aachen.de:8443/cream-pbs-cms'):
        self.name = name
        self.directory=directory
        if self.directory is None:
            self.directory = name
        self.directory = os.path.abspath(self.directory)
        self.jdlfilename = name+".jdl"
        self.inputfiles, self.outputfiles, self.jobs, self.executable = [], [], [], None
        self.mode = mode
        self.scramArch = scramArch
        self.cmsswVersion = cmsswVersion
        self.ceId = ceId
        self.jobs = []
        self.frontEndStatus=""
    def save(self):
        log.debug('Save task %s',self.name)
        f = open(os.path.join(self.directory, "task.pkl"), 'wb')
        pickle.dump(self, f)
        f.close()
        f = open(os.path.join(self.directory, "jobids.txt"), 'w')
        for job in self.jobs:
            try:
                f.write(job.jobid+"\n")
            except (AttributeError, TypeError):
                pass
        f.close()
    def addJob(self, job):
        job.task = self
        self.jobs.append(job)
    def submit(self, processes=0):
        log.debug('Submit task %s',self.name)
        # create directory
        self.createdir()
        startdir = os.getcwd()
        os.chdir(self.directory)
        self.makePrologue()
        checkAndRenewVomsProxy(604800)
        #enumerate jobs and create jdl files
        for i in range(len(self.jobs)):
            self.jobs[i].nodeid = i
            self.jobs[i].writeJdl()
        #multiprocessing
        self._dosubmit(range(len(self.jobs)), processes, submitWorker)
        self.frontEndStatus="SUBMITTED"
        os.chdir(startdir)
        #print self.jobs[0].__dict__
        self.save()
    def _dosubmit(self, nodeids, processes, worker):
        jobs = [j for j in self.jobs if j.nodeid in nodeids]
        if processes:
            pool = multiprocessing.Pool(processes)
            result = pool.map_async(worker, jobs)
            pool.close()
            #pool.join()
            while pool._cache:
                time.sleep(1)
            res = result.get()
            for job in res:  #because the task and jobs have been pickled, the references have to be restored
                job.task = self
                self.jobs[job.nodeid]=job
        else:
            for job in jobs:
                job.submit()

    def resubmit(self, nodeids, processes=0):
        log.debug('Resubmit (some) jobs of task %s',self.name)
        self._dosubmit(nodeids, processes, resubmitWorker)
        self.save()
        self.cleanUp()
            
    def makePrologue(self):
        executable = (
        '#!/bin/sh -e\n'
        +'echo Job started: $(date)\n'
        +'chmod u+x $1\n'
        +'RUNAREA=$(pwd)\n'
        +'echo Running in: $RUNAREA\n'
        +'echo Running on: $HOSTNAME\n'
        )
        if self.cmsswVersion is not None:
            executable += (
            'echo Setting SCRAM_ARCH to ' + self.scramArch +'\n'
            +'export SCRAM_ARCH=' +self.scramArch + '\n'
            +'export BUILD_ARCH=$SCRAM_ARCH\n'
            +'source $VO_CMS_SW_DIR/cmsset_default.sh\n'
            +'scram project CMSSW ' + self.cmsswVersion + '\n'
            +'cd ' + self.cmsswVersion + '\n'
            +'eval $(scramv1 ru -sh)\n'
            +'cd $RUNAREA\n'
            )
        executable += (
        'env\n'
        +'echo Current directory $PWD\n'
        +'echo Directory content:\n'
        +'ls\n'
        +'$@\n'
        +'echo Current directory $PWD\n'
        +'echo Directory content:\n'
        +'ls\n'
        +'echo Job ended: $(date)\n'
        )
        f = open("prologue.sh","w")
        f.write(executable)
        f.close()
    def createdir(self):
        if os.path.exists(self.directory) and self.mode!="RECREATE":
            raise Exception('Directory ' + self.directory + 'already exists')
        elif os.path.exists(self.directory):
            shutil.rmtree(self.directory)
        os.makedirs(self.directory)
    def _getStatusMultiple(self):
        checkAndRenewVomsProxy(604800)
        jobids = [job.jobid for job in self.jobs if job.frontEndStatus not in ["RETRIEVED", "PURGED"]]
        if not jobids: return
        command = ["glite-ce-job-status", "-L1"] + jobids
        process = subprocess.Popen(command, stdout=subprocess.PIPE)
        stdout, stderr = process.communicate()
        if process.returncode!=0:
            log.warning('Status retrieval failed for task '+self.name)
            log.info(stdout)
            log.info(stderr)
        result = parseStatusMultipleL1(stdout)
        for job in self.jobs:
            try:
                infos = result[job.jobid]
                job.infos = infos
            except:
                job.infos = dict()
    def getStatus(self):
        log.debug('Get status of task %s',self.name)
        self._getStatusMultiple()
        retrieved, done, running = True, True, False
        for job in self.jobs:
            if job.frontEndStatus!="RETRIEVED":
                retrieved=False
            if "RUNNING" in job.status:
                running=True
                break
            if "DONE" not in job.status:
                done = False
        if running: self.frontEndStatus="RUNNING"
        elif retrieved: self.frontEndStatus="RETRIEVED"
        elif done: self.frontEndStatus="DONE"
        self.save()
        return self.frontEndStatus

    def getOutput(self):
        log.info('Get output of (some) jobs of task %s',self.name)
        for job in self.jobs:
            try:
                if "DONE" in job.status and job.frontEndStatus!="RETRIEVED" and job.frontEndStatus!="PURGED":
                    job.getOutput()
            except TypeError:
                pass
        self.save()

    def jobStatusNumbers(self):
        jobStatusNumbers=defaultdict(int)
        good, bad = 0, 0
        for job in self.jobs:
            try:
                jobStatusNumbers[job.status]+=1
                jobStatusNumbers[job.frontEndStatus]+=1
                if job.status in ["ABORTED", "DONE-FAILED"]:
                    bad += 1
                if job.status == "DONE-OK":
                    if "ExitCode" in job.infos:
                        if job.infos["ExitCode"] == "0":
                            good += 1
                        else:
                            bad += 1
            except AttributeError:
                pass
        jobStatusNumbers["total"]=len(self.jobs)
        jobStatusNumbers["good"] = good
        jobStatusNumbers["bad"] = bad
        return jobStatusNumbers
    def cleanUp(self):
        log.debug('Cleaning up task %s',self.name)
        subdirs=[job.outputSubDirectory for job in self.jobs if job.jobid is not None]
        for checkdir in glob.glob(os.path.join(self.directory,"*")):
            if os.path.isdir(checkdir) and os.path.basename(checkdir)!="bak":
                if os.path.basename(checkdir) not in subdirs:
                    try:
                        os.mkdir(os.path.join(self.directory,"bak"))
                    except OSError:
                        pass
                    shutil.move(checkdir, os.path.join(self.directory, "bak"))


        

def parseStatus(stdout):
    # parses the output of glite-ce-job-status to a dict
    result=dict()
    for line in stdout.splitlines():
        try:
            key, value = line.split("=",1)
        except ValueError:
            continue
        key, value = key.strip("\t* "), value.strip()[1:-1]
        result[key] = value
    return result

def parseStatusMultiple(stdout):
    # parses the output of glite-ce-job-status to a dict for multiple jobids
    result = dict()
    jobid = None
    for line in stdout.splitlines():
        try:
            key, value = line.split("=",1)
        except ValueError:
            continue
        key, value = key.strip("\t* "), value.strip()[1:-1]
        if key=="JobID":
            jobid = value
            result[jobid]=dict()
        result[jobid][key] = value
    return result

def parseStatusMultipleL1(stdout):
    # parses the output of glite-ce-job-status -L1 to a dict, this includes the status history
    result = dict()
    jobid = None
    for line in stdout.splitlines():
        try:
            key, value = line.split("=",1)
        except ValueError:
            continue
        if "Command" in key: continue
        key, value = key.strip("\t* "), value.strip()[1:-1]
        if key=="JobID":
            jobid = value
            result[jobid]=dict()
            result[jobid]["history"]=list()
        if key=="Status":
            status=value.split()[0][0:-1]
            timestamp=value.split()[-1][1:]
            result[jobid]["history"].append( (status, timestamp,) )
            result[jobid]["Status"] = status
        else:
            result[jobid][key] = value
    return result

class ProxyError( Exception ):
    pass

def timeLeftVomsProxy():
    """Return the time left for the proxy."""
    proc = subprocess.Popen( ['voms-proxy-info', '-timeleft' ], stdout=subprocess.PIPE, stderr=subprocess.STDOUT )
    output = proc.communicate()[0]
    if proc.returncode != 0:
        return False
    else:
        return int( output )

def checkVomsProxy( time=86400 ):
    """Returns True if the proxy is valid longer than time, False otherwise."""
    timeleft = timeLeftVomsProxy()
    return timeleft > time

def renewVomsProxy( voms='cms:/cms/dcms', passphrase=None ):
    """Make a new proxy with a lifetime of one week."""
    if passphrase:
        p = subprocess.Popen(['voms-proxy-init', '--voms', voms, '--valid', '192:00'], stdout=subprocess.PIPE, stdin=subprocess.PIPE, stderr=subprocess.STDOUT)
        stdout = p.communicate(input=passphrase+'\n')[0]
        retcode = p.returncode
        if not retcode == 0:
            raise ProxyError( 'Proxy initialization command failed: '+stdout )
    else:
        retcode = subprocess.call( ['voms-proxy-init', '--voms', voms, '--valid', '192:00'] )
    if not retcode == 0:
        raise ProxyError( 'Proxy initialization command failed.')

def checkAndRenewVomsProxy( time=604800, voms='cms:/cms/dcms', passphrase=None ):
    """Check if the proxy is valid longer than time and renew if needed."""
    if not checkVomsProxy( time ):
        renewVomsProxy(passphrase=passphrase)
        if not checkVomsProxy( time ):
            raise ProxyError( 'Proxy still not valid long enough!' )
