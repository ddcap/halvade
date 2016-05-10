The human genome reference
==========================

Halvade uses the genome reference FASTA file (``ucsc.hg19.fasta``), found in the GATK resource bundle, to build the index files for both BWA and STAR. The FASTA file comes with an index and a dictionary file. Additionally a full dbSNP file (version 138) is used when recalibrating the base scores for the reads. These files are all found in the GATK resource bundle which is available `here <ftp://gsapubftp-anonymous@ftp.broadinstitute.org/bundle/2.8/hg19/>`_. This FTP site has a limited number of parallel downloads and might not load at these times. Here is how you download the files using the terminal in the current directory, it is advised to make a new directory for all reference files:

.. code-block:: bash
	:linenos:

	mkdir halvade_refs/
	cd halvade_refs/
	wget ftp://gsapubftp-anonymous@ftp.broadinstitute.org/bundle/2.8/hg19/ucsc.hg19.fasta.gz
	wget ftp://gsapubftp-anonymous@ftp.broadinstitute.org/bundle/2.8/hg19/ucsc.hg19.fasta.fai.gz
	wget ftp://gsapubftp-anonymous@ftp.broadinstitute.org/bundle/2.8/hg19/ucsc.hg19.dict.gz
	mkdir dbsnp
	cd dbsnp
	wget ftp://gsapubftp-anonymous@ftp.broadinstitute.org/bundle/2.8/hg19/dbsnp_138.hg19.vcf.gz
	wget ftp://gsapubftp-anonymous@ftp.broadinstitute.org/bundle/2.8/hg19/dbsnp_138.hg19.vcf.idx.gz
	cd ../

Next we need to unzip all these files so they can be used in Halvade:

.. code-block:: bash
	:linenos:

	gunzip ucsc.hg19.fasta.gz && gunzip ucsc.hg19.fasta.fai.gz && gunzip ucsc.hg19.dict.gz && \
	gunzip dbsnp/dbsnp_138.hg19.vcf.gz && gunzip dbsnp/dbsnp_138.hg19.vcf.idx.gz

The index files, the reference and the dbSNP file need to be uploaded to the HDFS server if a cluster with more than one node is used to run Halvade. Setting up Halvade is described in the following parts of the documentation.


BWA reference for WGS/WES data
------------------------------

The BWA aligner is used for the whole genome and exome sequencing pipelines. A BWT index of the reference FASTA file needs to be created to run BWA , which needs to be accessible by Halvade so BWA can be started correctly. The BWA binary is available in the ``bin.tar.gz`` archive, which is provided in every Halvade release. 

.. code-block:: bash
	:linenos:

	tar -xvf bin.tar.gz
	./bin/bwa index ucsc.hg19.fasta

This process will create 5 files with the provided name as a prefix, this naming convention is important as Halvade finds this index by the FASTA prefix ``ucsc.hg19``. 

STAR reference for RNA-seq data
-------------------------------

.. note:: The process to build the STAR index requires a minimum of 32 GBytes of RAM, make sure there is sufficient RAM memory.

The RNA-seq pipeline uses the STAR aligner to perform the read alignment step. Similarly to BWA, the STAR aligner requires an index of the reference FASTA file. Again, this can be created by using the STAR binary which is provided in the ``bin.tar.gz`` archive which is available in every Halvade release. 

.. code-block:: bash
	:linenos:

	tar -xvf bin.tar.gz
	mkdir ./STAR_ref/
	./bin/STAR --genomeDir ./STAR_ref/ --genomeFastaFiles ucsc.hg19.fasta --runMode genomeGenerate --runThreadN 4

The shown command to build the STAR genome index uses 4 threads, this should be updated to reflect the number of cores available. After the STAR genome index has been created, the provided output folder will contain all files needed by STAR and in turn by Halvade.




