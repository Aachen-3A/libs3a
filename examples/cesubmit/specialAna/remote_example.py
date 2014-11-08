#>/bin/env python
import sys
import cesubmit
from listFiles import getdcachelist
import binConfig_example as binConfig
import checkEnvironment
from datetime import datetime
import optparse,os,time,cPickle,subprocess,shutil
import logging
log = logging.getLogger( 'remote' )

def getFilesfromFile(cfgFile):
    sampleList={}
    file = open(cfgFile,'r')
    user,tag,sample,config=["","","",""]

    known_folders=dict()
    if not os.path.exists("data"):
        os.mkdir("data")

    if os.path.exists("data/fileList.pkl"):
        known_folders=readDcachePickle("data/fileList.pkl")

    for line in file:
        if "tag=" in line:
            tag=line.split("=")[1].strip()
            continue
        if "user=" in line:
            user=line.split("=")[1].strip()
            continue
        if "config=" in line:
            config=line.split("=")[1].strip()
            continue
        if line[0]=="#" or len(line.split())==0:
            continue
        sample=line.strip()
        log.debug( " ".join([user,tag,sample,config]))
        folder="/%s/MUSiC/%s/%s" % (user,tag,sample)
        if folder in known_folders:
            file_lists=known_folders[folder]
        else:
            time.sleep(4)
            file_lists = getdcachelist( folder,sample )
            outfile = open( "data/fileList.pkl", 'a+b' )
            cPickle.dump( {folder:file_lists}, outfile, -1 )
            outfile.close()

        if len(file_lists)>0:
            sampleList.update({sample:[file_lists,config]})
        else:
            raise IOError( 'No sample in List for folder '+sample )
    return sampleList

def readDcachePickle(file):
    infile = open(file, 'rb' )
    known_folders = dict()
    try:
        while True:
            try:
                folder = cPickle.load( infile )
                known_folders.update( folder )
            except KeyError as e:
                log.error( "KeyError: "+e)
            except IndexError as e:
                log.error( "IndexError: "+e)
                infile.close()
                return known_folders
            except cPickle.UnpicklingError as e:
                log.error( "cPickle.UnpicklingError: "+e)
                infile.close()
                return known_folders
            except ValueError as e:
                log.error( "ValueError: "+e)
    except EOFError:
        infile.close()
        return known_folders
    except:
        raise

def makeExe(user):
    from string import Template
    exe="""
    echo Copying pack...
    #Try 10 times to copy the pack file with help of srmcp.
    success=false
    for i in {1..10}; do
       if srmcp gsiftp://grid-se113.physik.rwth-aachen.de:2811/pnfs/physik.rwth-aachen.de/cms/store/user/$USER/$PROGAM/share/program.tar.gz file:///.; then
          success=true
          break
       fi
    done
    if ! $success; then
       echo Copying of pack file \\\'gsiftp://grid-se113.physik.rwth-aachen.de:2811/pnfs/physik.rwth-aachen.de/cms/store/user/$USER/$PROGAM/share/program.tar.gz\\\' failed! 1>&2
       echo Did you forget to \\\'remix --copy\\\'? 1>&2
    fi


    tar xzvf program.tar.gz
    export MUSIC_BASE=$PWD
    export LD_LIBRARY_PATH=$PWD/extra_libs:$LD_LIBRARY_PATH
    export LD_LIBRARY_PATH=$LHAPATHREPLACE/lib:$LD_LIBRARY_PATH
    #echo LD_LIBRARY_PATH=$LD_LIBRARY_PATH
    #echo Setting LHAPATH to $LHAPATHREPLACE
    export LHAPATH=$LHAPATHREPLACE2

    ldd bin/music
    echo "$@"
    bin/music "$@"

    tar czf MusicOutDir.tar.gz MusicOutDir"""


    #this should be done as a fuction parameter
    d = dict(
            USER=user,
            PROGAM="MUSiC",
            LHAPATHREPLACE="/cvmfs/cms.cern.ch/slc6_amd64_gcc481/external/lhapdf6/6.1.4/",
            LHAPATHREPLACE2="/cvmfs/cms.cern.ch/slc6_amd64_gcc481/external/lhapdf6/6.1.4/share/LHAPDF/PDFsets",
        )
    exe=Template(exe).safe_substitute(d)
    exeFile=open("runtemp.sh","w+")
    exeFile.write(exe)
    exeFile.close()

def prepare_teli(options):
    import tempfile
    import TimedCall
    log.info("Copy file to dache..")
    cpFiles=binConfig.cpFiles
    PathtoExecutable=binConfig.PathtoExecutable
    tempdir = tempfile.mkdtemp( prefix='televisionExe-' )
    for i in cpFiles:
        command="cp -r %s/%s %s"%(PathtoExecutable,i,tempdir)
        retcode, output2=TimedCall.retry(3,300,command.split(" "))
        if retcode!=0:
            log.error("Could not create a local copy check arguments!!")
            sys.exit(1)
    thidir=os.getcwd()
    os.chdir( tempdir )

    retcode, output2=TimedCall.retry(3,300,['tar', 'czf' , 'program.tar.gz']+os.listdir(tempdir) )
    if retcode!=0:
        log.error("Could not create a local copy check arguments!!")
        sys.exit(1)
    user = options.user
    path = "srm://grid-srm.physik.rwth-aachen.de:8443/pnfs/physik.rwth-aachen.de/cms/store/user/%s/MUSiC/share/"%(user)
    cmd1 = "lcg-cp"
    cmd2 = "file:///%s/program.tar.gz"% (tempdir)
    cmd3 = "%sprogram.tar.gz"% (path)
    command = [cmd1,cmd2,cmd3]
    command2 = ["uberftp","grid-ftp.physik.rwth-aachen.de",r"rm /pnfs/physik.rwth-aachen.de/cms/store/user/%s/MUSiC/share/program.tar.gz"%(user)]
    counter=0
    log.debug( " ".join(command2))
    log.debug(" ".join(command))
    while counter<3:
        retcode2, output2=TimedCall.retry( 3, 600, command2 )
        retcode, output=TimedCall.retry( 3, 600, command )
        if retcode+retcode2==0:
            break
        if (retcode+retcode2)!=0:
            log.info("Could not copy file to dcache")
            log.info(" ".join(command2))
            log.info(" ".join(command))
            log.info(output2)
            log.info(output)
    if (retcode+retcode2)!=0:
        log.info("Could not copy file to dcache")
        log.info(command2)
        log.info(command)
        log.info(output2)
        log.info(output)
        sys.exit(1)
    log.info("File "+tempdir+"/program.tar.gz  copied to dcache")
    os.chdir(thidir)


def main():

    date_time = datetime.now()
    usage = '%prog [options] CONFIG_FILE'
    parser = optparse.OptionParser( usage = usage )
    parser.add_option( '-u', '--user', default = os.getenv( 'LOGNAME' ),
                            help = 'which user on dcache [default = %s]'%(os.getenv( 'LOGNAME' )))
    parser.add_option( '-o', '--Output', default = '%s'%(binConfig.outDir).replace("USER",os.getlogin())+"/TAG", metavar = 'DIRECTORY',
                            help = 'Define the output directory. [default = %default]')
    parser.add_option( '-f', '--force', default = "force the output to overwrite", metavar = 'DIRECTORY',
                            help = 'Define the output directory. [default = %default]')
    parser.add_option( '--debug', metavar = 'LEVEL', default = 'INFO',
                       help= 'Set the debug level. Allowed values: ERROR, WARNING, INFO, DEBUG. [default = %default]' )
    parser.add_option( '-t', '--Tag', default = "output%s_%s_%s_%s_%s"%(date_time.year,
                                                                        date_time.month,
                                                                        date_time.day,
                                                                        date_time.hour,
                                                                        date_time.minute), metavar = 'DIRECTORY',
                        help = 'Define a Tag for the output directory. [default = %default]' )

    ( options, args ) = parser.parse_args()
    if len( args ) != 1:
        parser.error( 'Exactly one CONFIG_FILE required!' )
    options.Output=options.Output.replace("TAG",options.Tag)


    format = '%(levelname)s from %(name)s at %(asctime)s: %(message)s'
    date = '%F %H:%M:%S'
    logging.basicConfig( level = logging._levelNames[ options.debug ], format = format, datefmt = date )


    try:
       cmssw_version, cmssw_base, scram_arch = checkEnvironment.checkEnvironment()
    except EnvironmentError, err:
        log.error( err )
        log.info( 'Exiting...' )
        sys.exit( err.errno )


    cfgFile = args[ 0 ]
    sampleList=getFilesfromFile(cfgFile)
    prepare_teli(options)
    makeExe(options.user)

    thisdir=os.getcwd()
    if os.path.exists(options.Output) or not options.force:
        log.error("The outpath "+options.Output+" already exists pick a new one or use --force")
        sys.exit(3)
    else:
        os.makedirs(options.Output)
    shutil.copyfile(thisdir+"/runtemp.sh",options.Output+"/runtemp.sh")
    os.remove(thisdir+"/runtemp.sh")

    for sample in sampleList:
        task=cesubmit.Task(sample,options.Output+"/"+sample,scramArch='slc6_amd64_gcc481', cmsswVersion='CMSSW_7_0_7_patch1')

        task.executable=options.Output+"/runtemp.sh"
        task.inputfiles=[]
        task.outputfiles=["MusicOutDir.tar.gz"]

        #usage: bin/music [--DumpECHistos] [--NoSpecialAna] [--NoCcControl] [--NoCcEventClass] [-h] [-o value] [-N value] [-x value] [-p value] [--debug value] [-M value] a1...


        standardArg=["--NoCcControl", "--NoCcEventClass","--ECMerger","2","-o","MusicOutDir",sampleList[sample][1]]
        print "down under"
        for f in sampleList[sample][0]:
            print f
            job=cesubmit.Job()
            job.arguments=standardArg+f
            task.addJob(job)
        log.info("start submitting")
        task.submit(6)


    log.info("Thanks for zapping in, bye bye")
    log.info("The out files will be in "+options.Output)



if __name__ == '__main__':
    main()
