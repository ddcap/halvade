
# download Halvade
#halvade_dir=/usr/local/halvade
halvade_tmp=/tmp/halvade/
mkdir -p $halvade_tmp/ref/dbsnp
#mkdir $halvade_dir
cd $halvade_tmp
cpu=${2:-16}
mem=${1:-64}

echo "downloading Halvade:"
version=v1.1.0
if [ -f HalvadeWithLibs.jar ];
then
	echo "Halvade $version already downloaded, skipping"
else
	curl -O -L https://github.com/ddcap/halvade/releases/download/$version/Halvade_${version}.tar.gz
	tar xvf Halvade_${version}.tar.gz
	cp bin.tar.gz bin_backup.tar.gz
	tar xvf bin.tar.gz
	mv bin_backup.tar.gz bin.tar.gz
fi

cd ref/

# download Reference
echo "downloading hg reference:"
if [ -f ucsc.hg19.fasta ];
then 
	echo "ucsc.hg19.fasta already downloaded, skipping"
else
	echo " *** this might take a while ***"
	curl -O ftp://gsapubftp-anonymous@ftp.broadinstitute.org/bundle/2.8/hg19/ucsc.hg19.fasta.gz
	if [ 0 -ne $? ]; 
	then
	## alternate fasta source:
		mkdir fasta
		cd fasta
		wget --timestamping 'ftp://hgdownload.cse.ucsc.edu/goldenPath/hg19/chromosomes/*'
		gunzip *.gz
		cat chrM.fa chr1.fa chr2.fa chr3.fa chr4.fa chr5.fa chr6.fa chr7.fa chr8.fa chr9.fa chr10.fa chr11.fa chr12.fa chr13.fa chr14.fa chr15.fa chr16.fa chr17.fa chr18.fa chr19.fa chr20.fa chr21.fa chr22.fa chrX.fa chrY.fa chr1_gl000191_random.fa chr1_gl000192_random.fa chr4_ctg9_hap1.fa chr4_gl000193_random.fa chr4_gl000194_random.fa chr6_apd_hap1.fa chr6_cox_hap2.fa chr6_dbb_hap3.fa chr6_mann_hap4.fa chr6_mcf_hap5.fa chr6_qbl_hap6.fa chr6_ssto_hap7.fa chr7_gl000195_random.fa chr8_gl000196_random.fa chr8_gl000197_random.fa chr9_gl000198_random.fa chr9_gl000199_random.fa chr9_gl000200_random.fa chr9_gl000201_random.fa chr11_gl000202_random.fa  chr17_ctg5_hap1.fa chr17_gl000203_random.fa chr17_gl000204_random.fa chr17_gl000205_random.fa chr17_gl000206_random.fa chr18_gl000207_random.fa chr19_gl000208_random.fa chr19_gl000209_random.fa chr21_gl000210_random.fa chrUn_gl000211.fa chrUn_gl000212.fa chrUn_gl000213.fa chrUn_gl000214.fa chrUn_gl000215.fa chrUn_gl000216.fa chrUn_gl000217.fa chrUn_gl000218.fa chrUn_gl000219.fa chrUn_gl000220.fa chrUn_gl000221.fa chrUn_gl000222.fa chrUn_gl000223.fa chrUn_gl000224.fa chrUn_gl000225.fa chrUn_gl000226.fa chrUn_gl000227.fa chrUn_gl000228.fa chrUn_gl000229.fa chrUn_gl000230.fa chrUn_gl000231.fa chrUn_gl000232.fa chrUn_gl000233.fa chrUn_gl000234.fa chrUn_gl000235.fa chrUn_gl000236.fa chrUn_gl000237.fa chrUn_gl000238.fa chrUn_gl000239.fa chrUn_gl000240.fa chrUn_gl000241.fa chrUn_gl000242.fa chrUn_gl000243.fa chrUn_gl000244.fa chrUn_gl000245.fa chrUn_gl000246.fa chrUn_gl000247.fa chrUn_gl000248.fa chrUn_gl000249.fa  > ucsc.hg19.fasta
		mv ucsc.hg19.fasta ../
		cd ../
		rm -r fasta/
		./bin/samtools faidx ucsc.hg19.fasta
		java_ver=$(java -version 2>&1 | sed 's/.*version "\(.*\)\.\(.*\)\..*"/\1\2/; 1q')
		if [ $java_ver -eq 17 ];
		then
			# requires java 1.7
			curl -L -O https://github.com/broadinstitute/picard/releases/download/1.141/picard-tools-1.141.zip
			unzip picard-tools-1.141.zip
			picard=picard-tools-1.141/picard.jar
		elif [ $java_ver -eq 18 ];
		then
			# requires java 1.8
			curl -L -O https://github.com/broadinstitute/picard/releases/download/2.2.4/picard-tools-2.2.4.zip
			unzip picard-tools-2.2.4.zip
			picard=picard-tools-2.2.4/picard.jar
		else
			echo "To build the dictionary file java 1.7 or newer is required, please install first"
			exit -1
		fi
		java -jar $picard CreateSequenceDictionary R=ucsc.hg19.fasta O=ucsc.hg19.dict
		
	else
		gunzip ucsc.hg19.fasta.gz	
	fi
fi
if [ -f ucsc.hg19.fasta.fai ];
then
	echo "fasta index already exists, skipping"
else
	curl -O ftp://gsapubftp-anonymous@ftp.broadinstitute.org/bundle/2.8/hg19/ucsc.hg19.fasta.fai.gz
	if [0 -ne $? ];
	then
		# failed to download, build it ourselves
		./bin/samtools faidx ucsc.hg19.fasta
	else
		gunzip ucsc.hg19.fasta.fai.gz
	fi
fi
if [ -f ucsc.hg19.dict ];
then
	echo "fasta dictionary already exists, skipping"
else
	curl -O ftp://gsapubftp-anonymous@ftp.broadinstitute.org/bundle/2.8/hg19/ucsc.hg19.dict.gz
	if [ 0 -ne $? ];
	then
		# failed build it ourselves
                java_ver=$(java -version 2>&1 | sed 's/.*version "\(.*\)\.\(.*\)\..*"/\1\2/; 1q')
                if [ $java_ver -eq 17 ];
                then
                        # requires java 1.7
                        curl -L -O https://github.com/broadinstitute/picard/releases/download/1.141/picard-tools-1.141.zip
                        unzip picard-tools-1.141.zip
                        picard=picard-tools-1.141/picard.jar
                elif [ $java_ver -eq 18 ];
                then
                        # requires java 1.8
                        curl -L -O https://github.com/broadinstitute/picard/releases/download/2.2.4/picard-tools-2.2.4.zip
                        unzip picard-tools-2.2.4.zip
                        picard=picard-tools-2.2.4/picard.jar
                else
                        echo "To build the dictionary file java 1.7 or newer is required, please install first"
                        exit -2
                fi
                java -jar $picard CreateSequenceDictionary R=ucsc.hg19.fasta O=ucsc.hg19.dict	
	else
		gunzip ucsc.hg19.dict.gz
	fi
fi


# download dbsnp
cd dbsnp/
echo "downloading hg dbsnp:"
if [ -f dbsnp_138.hg19.vcf ];
then
	echo "dbsnp found, skipping"
else
	echo " *** this might take a while ***"
	curl -O ftp://gsapubftp-anonymous@ftp.broadinstitute.org/bundle/2.8/hg19/dbsnp_138.hg19.vcf.gz
	if [ 0 -ne $? ];
	then
		echo "Failed to download dbsnp file (typically fts server is too crowded), try again later"
		exit -3
	else
		gunzip dbsnp_138.hg19.vcf.gz
	fi
fi
if [ -f dbsnp_138.hg19.vcf.idx ];
then
        echo "dbsnp index found, skipping"
else
	curl -O ftp://gsapubftp-anonymous@ftp.broadinstitute.org/bundle/2.8/hg19/dbsnp_138.hg19.vcf.idx.gz
        if [ 0 -ne $? ];
        then
                echo "Failed to download dbsnp index file (typically fts server is too crowded), try again later"
                exit -4
        else
                gunzip dbsnp_138.hg19.vcf.idx.gz
        fi
fi

cd ../
# make star reference
echo "creating star genome reference:"
mkdir $halvade_tmp/ref/STAR_ref/
$hadoop_tmp/bin/STAR --genomeDir $halvade_tmp/ref/STAR_ref/ --genomeFastaFiles ucsc.hg19.fasta --runMode genomeGenerate --runThreadN $cpu


# push data to hdfs
echo "downloading RNA-seq data:"
rna1=
rna2=

base_docker_sh=base-init-docker.sh
docker_sh=docker-init-script.sh
# here the command will come to prep the data : 8 before + 17 after + change cpu_48_ and mem_128_ and smt??
head -n 8 $base_docker_sh | sed "s/memmb_128_/$memmb/g" > $docker_sh
echo "hadoop jar HalvadeUploaderWithLibs.jar -1 $rna1 -2 $rna2 -O /user/root/halvade/in/ -t $cpu" >> $docker_sh
smt='echo "smt"'
read -r -p "Does this node support simultaneous hyperthreading? [Y/n] " response
case $response in
    [nN][oO]|[nN])
        smt='#echo "smt"'
        exit
        ;;
esac
memmb=$(($mem*1024))
tail -n 20 $base_docker_sh | sed "s/cpu_48_/$cpu/g" | sed "s/mem_128_/$mem/g" | sed "s/echo \"smt\"/$smt/g" >> $docker_sh



