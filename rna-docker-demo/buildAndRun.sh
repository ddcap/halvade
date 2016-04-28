#!/bin/bash


docker_name="sequenceiq/hadoop-docker"
version="2.7.1"
mem=${1:-64}
memmb=$(($mem*1024))
cores=${2:-16}

# clone sequenceiq hadoop-docker source
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

# update the bootstrap script to include halvade setup:
cat base-halvade-setup.sh | sed "s/48/$cores/g"  | sed "s/131072/$memmb/g" | sed "s/128/$mem/g" > halvade-setup.sh

echo "ADD halvade-setup.sh /etc/halvade-setup.sh" >> Dockerfile
echo "RUN chown root:root /etc/halvade-setup.sh" >> Dockerfile
echo "RUN chmod 700 /etc/halvade-setup.sh" >> Dockerfile

# build docker image
docker build -t $docker_name:$version .

# get docker hash
hash=`docker images | grep $docker_name | grep $version | awk '{print $3}'`

echo "run 'docker run -it $hash /etc/bootstrap.sh -bash' to start the hadoop cluster."
echo "after the docker has started run '/etc/halvade-setup.sh' to setup Halvade and download all necessary references and demo input files."
