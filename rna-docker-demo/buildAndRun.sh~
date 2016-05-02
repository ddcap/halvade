#!/bin/bash

echo "This process will download up to 3GB of reference files and create a Docker image (based on sequeneiq/hadoop-docker) which can grow up to 40GB in size"
echo "To build the reference genome to be used with STAR aligner at least 30GB of RAM is required."

read -r -p "Are you sure? [Y/n] " response
case $response in
    [nN][oO]|[nN])
        echo exiting
        exit
        ;;
esac


smt=1
read -r -p "Does this node support simultaneous hyperthreading? [Y/n] " response
case $response in
    [nN][oO]|[nN])
        smt=0
        exit
        ;;
esac

# the to be created image is based on the sequenceiq/hadoop-docker Dockerfile
docker_name="halvade-docker"
version="0.0.1"
mem=${1:-64}
memmb=$(($mem*1024))
cores=${2:-16}

# check if docker image exists:
hash=`docker images | grep $docker_name | grep $version | awk '{print $3}'`
if [ -z "$hash" ];
then
	# clone sequenceiq hadoop-docker source
	echo "downloading 'sequenceiq/hadoop-docker' source from git"
	git clone https://github.com/sequenceiq/hadoop-docker.git 
	mv hadoop-docker/* ./
	
	# setting up the memory and cores for the cluster
	# mapred-site.xml
	if [ ! -f tmp-mapred-site.xml ];
	then
		cat mapred-site.xml | sed 's/<\/configuration>//g' > tmp-mapred-site.xml
	fi
	cp tmp-mapred-site.xml mapred-site.xml
	cat base-mapred-site.xml >> mapred-site.xml
	echo "</configuration>" >> mapred-site.xml
	
	# yarn-site.xml
	if [ ! -f tmp-yarn-site.xml ];
	then
		cat yarn-site.xml | sed 's/<\/configuration>//g' > tmp-yarn-site.xml
	fi
	cp tmp-yarn-site.xml yarn-site.xml
	cat base-yarn-site.xml | sed "s/48/$cores/g"  | sed "s/131072/$memmb/g" >> yarn-site.xml
	echo "</configuration>" >> yarn-site.xml
	
	
	echo "ADD docker-init-script.sh /etc/docker-init-script.sh" >> Dockerfile
	echo "RUN chown root:root /etc/docker-init-script.sh" >> Dockerfile
	echo "RUN chmod 700 /etc/docker-init-script.sh" >> Dockerfile
	#fasta/fasta.fai/dict/genomedir/dbsnp/dbsnpidx/
	echo "RUN mkdir -p /usr/local/halvade/ref/" >> Dockerfile
	echo "COPY ref/ /usr/local/halvade/ref/" >> Dockerfile
	echo "COPY bin.tar.gz /usr/local/halvade/" >> Dockerfile
	#echo "RUN chown -R root:root /usr/local/halvade && chmod -R 700 /usr/local/halvade" >> Dockerfile
	
	# download all reference files + setup hadoop script for docker image:
	./base-halvade-setup.sh $cores $mem $smt

	if [ 0 -ne $? ];
        then
		echo "base-halvade-setup script failed, cannot build the docker image"
		exit -1
	else
		# build docker image
		docker build -t $docker_name:$version ./
		ret=$?
	
		# get docker hash
		hash=`docker images | grep $docker_name | grep $version | awk '{print $3}'`
	fi
else
	ret=0
fi

if [ 0 -ne $ret ];
then 
	echo "docker build failed, please try again"
	exit -2;
else
	echo "Run 'docker run -it $hash /etc/bootstrap.sh -bash' to start the hadoop cluster."
	echo "After the docker has started run '/etc/docker-init-script.sh' to preprocess the data for Halvade"
	echo "To run halvade, run '/usr/local/halvade/runHalvade.py /usr/local/halvade/demo_halvade.conf'"
fi

