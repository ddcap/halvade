#!/usr/bin/python

import subprocess
import os
import sys


halvade = "halvade.config"
arguments = "halvade_run.config"
jar = "HalvadeWithLibs.jar"
s3logging = "s3://itx-abt-jnj-exasci/ddecap/halvade/logs/"
emr_config = dict()
config = dict()
flags = dict()
custom_args = dict()

# add dictionary with vcores and mem for emr
emr_mem = dict()
emr_vcores = dict()

def readConfig(filename):
	with open(filename) as f:
		for line in f:
			if len(line) > 1 and not line.startswith("#"):
				if "=" in line:
					arr = line.strip().split('=', 1)
					key = arr[0]
					val = arr[1].replace('\"','')
					if key.startswith("emr"):
						emr_config[key] = val
						print "EMR %s: %s" % (key, val)
					elif key == "ca":
						tca = val.split('=', 1)
						custom_args[tca[0]] = tca[1]
						print "CA %s: %s" % (tca[0], tca[1])
					else:
						config[key] = val
						print "%s: %s" % (key, val)
				else:
					key = line.strip()
					flags[key] = key
					print "FLAG %s: %s" % (key, key)				

def spawnDaemon(args):
        # create double-fork
        try:
                pid = os.fork()
                if pid > 0:
                        # parent process, return and keep running
                        return
        except OSError, e:
                print >> sys.stderr, "fork #1 failed: %d (%s)" % (e.errno, e.strerror)
                sys.exit(1)

        os.setsid()

        # do second fork
        try:
                pid = os.fork()
                if pid > 0:
                        # exit from second parent
                        sys.exit(0)
        except OSError, e:
                print >> sys.stderr, "fork #2 failed: %d (%s)" % (e.errno, e.strerror)
                sys.exit(1)

        print "starting halvade with pid(%d)" % (os.getpid())
        subprocess.Popen(args, stdout=open('halvade'+str(os.getpid())+'.stdout', 'a'), stderr=open('halvade'+str(os.getpid())+'.stderr', 'a'))
        os._exit(os.EX_OK)


# read config
if (len(sys.argv) > 1):
        print "*** Reading configuration from %s:" % sys.argv[1]
        readConfig(sys.argv[1])
else:   
        print "*** Reading configuration from %s:" % halvade
        readConfig(halvade)
        print "*** Reading configuration from %s:" % arguments
        readConfig(arguments)

print jar
# determine if S3 is used -> use amazon EMR
if "emr_type" in emr_config:
	print "Running Halvade on Amazon EMR:"
	emr_mem = int(config["mem"])*1024
	timeout = 1800000
	hadoopArgs="[-y,yarn.scheduler.maximum-allocation-mb=%d,-y,yarn.nodemanager.resource.memory-mb=%d,-m,mapreduce.job.reduce.slowstart.completedmaps=1.0,-m,mapreduce.task.timeout=%d]" %(emr_mem, emr_mem,timeout)
	print hadoopArgs
	argsArray = []
	argsArray.append("aws")
	argsArray.append("emr")
	argsArray.append("create-cluster")
	argsArray.append("--auto-terminate")
	argsArray.append("--instance-type")
	argsArray.append(emr_config["emr_type"])
	argsArray.append("--instance-count")
	argsArray.append(config["nodes"])
	argsArray.append("--enable-debugging")
	argsArray.append("--ami-version")
	argsArray.append(emr_config["emr_ami_v"])
	argsArray.append("--log-uri")
	argsArray.append(s3logging)
        argsArray.append("--bootstrap-actions")
        argsArray.append("Path=s3://elasticmapreduce/bootstrap-actions/configure-hadoop,Name=configuration,Args="+hadoopArgs+ ",Path="+emr_config["emr_script"]+",Name=maketmpdir")
	argsArray.append("--steps")
	argsString ="["
        for key in config:
				if (len(key) > 1):
	                argsString+="--"+key+","					
				else
	                argsString+="-"+key+","
                argsString+=config[key]+","
        for key in flags:
				if (len(key) > 1):
	                argsString+="--"+key+","					
				else
	                argsString+="-"+key+","
        for key in custom_args:
                argsString+="--CA,"
                argsString+=key+"="+custom_args[key]+","
	argsString = argsString[:-1]+"]"
	argsArray.append("Name=Halvade,Jar="+emr_config["emr_jar"]+",ActionOnFailure=TERMINATE_CLUSTER,Args="+argsString)
	print argsArray
        spawnDaemon(argsArray)

else:
	print "Running Halvade on local cluster:"
	argsArray = []
	argsArray.append("hadoop")
	argsArray.append("jar")
	argsArray.append(jar)
	for key in config:
		if (len(key) > 1):
			argsArray.append("--"+key)				
		else
			argsArray.append("-"+key)
		argsArray.append("-"+key)
		argsArray.append(config[key])
	for key in flags:
		if (len(key) > 1):
			argsArray.append("--"+key)				
		else
			argsArray.append("-"+key)
	for key in custom_args:
		argsArray.append("--CA")
		argsArray.append(key+"="+custom_args[key])
	print argsArray
        spawnDaemon(argsArray)

