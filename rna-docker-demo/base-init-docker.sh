#!/bin/bash

echo "push the data to hdfs:"
export PATH=$PATH:/usr/local/hadoop/bin
hdfs dfs -mkdir -p /user/root/halvade/in/
hdfs dfs -put bin.tar.gz /user/root/halvade/

export HADOOP_HEAPSIZE=memmb_128_
# here the command will come to prep the data : 8 before + 20 after + change cpu_48_ and mem_128_ and smt??

config=demo_halvade.conf
halvade_tmp=/usr/local/halvade/ref
cd /usr/local/halvade/

touch $config
echo "nodes=1" >> $config
echo "cores=cpu_48_" >> $config
echo "mem=mem_128_" >> $config
echo "B=\"/user/root/halvade/bin.tar.gz\"" >> $config
echo "D=\"$halvade_tmp/dbsnp/dbsnp_138.hg19.vcf\"" >> $config
echo "R=\"$halvade_tmp/ucsc.hg19\"" >> $config
echo "SG=\"$halvade_tmp/STAR_ref/\"" >> $config
echo "smt" >> $config
echo "rna" >> $config
echo "I=\"/user/root/halvade/in/\"" >> $config
echo "O=\"/user/root/halvade/out/\"" >> $config

dir=`pwd`
echo "run Halvade with this command: '$pwd/runHalvade.py'"
