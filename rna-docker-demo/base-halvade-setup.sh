
# download Halvade
halvade_dir=/usr/local/halvade
halvade_tmp=/tmp/halvade/
mkdir -p $halvade_tmp/dbsnp
mkdir $halvade_dir
cd $halvade_dir

echo "downloading Halvade:"
version=v1.1.0
curl -O -L https://github.com/ddcap/halvade/releases/download/$version/Halvade_${version}.tar.gz
tar xvf Halvade_${version}.tar.gz

# download Reference
echo "downloading hg reference:"
curl -O ftp://gsapubftp-anonymous@ftp.broadinstitute.org/bundle/2.8/hg19/ucsc.hg19.fasta.gz
curl -O ftp://gsapubftp-anonymous@ftp.broadinstitute.org/bundle/2.8/hg19/ucsc.hg19.fasta.fai.gz
curl -O ftp://gsapubftp-anonymous@ftp.broadinstitute.org/bundle/2.8/hg19/ucsc.hg19.dict.gz

# download dbsnp
echo "downloading hg dbsnp:"
curl -O ftp://gsapubftp-anonymous@ftp.broadinstitute.org/bundle/2.8/hg19/dbsnp_138.hg19.vcf.gz
curl -O ftp://gsapubftp-anonymous@ftp.broadinstitute.org/bundle/2.8/hg19/dbsnp_138.hg19.vcf.idx.gz

# make star reference
echo "creating star genome reference:"
tar xvf bin.tar.gz
./bin/STAR --genomeDir $halvade_tmp/STAR_ref/ --genomeFastaFiles ucsc.hg19.fasta --runMode genomeGenerate --runThreadN 48 # --genomeSAsparseD 8

# put the references in the halvade tmp dir
echo "copying the references to the correct place:"
mv ucsc.hg19* $halvade_tmp
mv dbsnp_138.hg19* $halvade_tmp/dbsnp/
gunzip $halvade_tmp/*.gz
gunzip $halvade_tmp/dbsnp/*.gz
touch $halvade_tmp/ucsc.hg19.gatk_ref
touch $halvade_tmp/STAR_ref/.star_ref
touch $halvade_tmp/dbsnp/.dbsnp

# push data to hdfs
echo "downloading RNA-seq data:"

rna1=
rna2=

echo "push the data to hdfs:"
hdfs dfs -mkdir -p /user/root/halvade/in/
hdfs dfs -put bin.tar.gz /user/root/halvade/

export HADOOP_HEAPSIZE=131072
hadoop jar HalvadeUploaderWithLibs.jar -1 $rna1 -2 $rna2 -O /user/root/halvade/in/ -t 48

config=demo_halvade.conf
touch $config
echo "nodes=1" >> $config
echo "cores=48" >> $config
echo "mem=128" >> $config
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

