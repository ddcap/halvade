#!/usr/local/bin/python

from sys import argv
from subprocess import call

halvade = "halvade.config"
arguments = "halvade_run.config"
jar = "HalvadeWithLibs.jar"
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



# read config
print "*** Reading configuration from %s:" % halvade
readConfig(halvade)
print "*** Reading configuration from %s:" % arguments
readConfig(arguments)

print jar
# determine if S3 is used -> use amazon EMR
if "emr_type" in emr_config:
	print "Running Halvade on Amazon EMR:"
	emr_mem = int(config["mem"])*1024
	timeout = 6000000
	hadoopArgs="-y,yarn.scheduler.maximum-allocation-mb=%d,-y,yarn.nodemanager.resource.memory-mb=%d,-m,mapreduce.job.reduce.slowstart.completedmaps=1.0,-m,mapreduce.task.timeout=%d" %(emr_mem, emr_mem,timeout)
	print hadoopArgs
	argsArray = []
	argsArray.append("elastic-mapreduce")
	argsArray.append("--create")
	argsArray.append("--instance-type")
	argsArray.append(emr_config["emr_type"])
	argsArray.append("--instance-count")
	argsArray.append(config["nodes"])
	argsArray.append("--enable-debugging")
	argsArray.append("--ami-version")
	argsArray.append(emr_config["emr_ami_v"])
	argsArray.append("--bootstrap-action")
	argsArray.append("s3://elasticmapreduce/bootstrap-actions/configure-hadoop")
	argsArray.append("--args")
	argsArray.append(hadoopArgs)
	argsArray.append("--bootstrap-action")
	argsArray.append(emr_config["emr_script"])
	argsArray.append("--jar")
	argsArray.append(emr_config["emr_jar"])
	for key in config:
		argsArray.append("--arg")
		argsArray.append("-"+key)
		argsArray.append("--arg")
		argsArray.append(config[key])
	for key in flags:
		argsArray.append("--arg")
		argsArray.append("-"+key)
	for key in custom_args:
		argsArray.append("--arg")
                argsArray.append("-ca")	
		argsArray.append("--arg")
                argsArray.append(key+"="+custom_args[key])
	print argsArray
	call(argsArray)


else:
	print "Running Halvade on local cluster:"
	argsArray = []
	argsArray.append("hadoop")
	argsArray.append("jar")
	argsArray.append(jar)
	for key in config:
		argsArray.append("-"+key)
		argsArray.append(config[key])
	for key in flags:
		argsArray.append("-"+key)
	for key in custom_args:
		argsArray.append("-ca")
		argsArray.append(key+"="+custom_args[key])
	print argsArray
	call(argsArray)


