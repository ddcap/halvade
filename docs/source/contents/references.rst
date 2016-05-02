Download an prepare the human genome references
===============================================

Halvade uses the genome reference fasta file (``ucsc.hg19.fasta``) found in the GATK resource bundle to build the index files for both BWA and STAR. The fasta file comes together with an index and a dictionary file. Additionally a full dbsnp file (version 138) is used when recalibrating the basescores for the reads. These files are all found in the GATK resource bundle which is available `here <ftp://gsapubftp-anonymous@ftp.broadinstitute.org/bundle/2.8/hg19/>`_. The ftp site has a limited number of parallel downloads and might not load at these times. Here is how you download the files with in the terminal:

.. code-block:: bash
	:linenos:

	wget ftp://gsapubftp-anonymous@ftp.broadinstitute.org/bundle/2.8/hg19/ucsc.hg19.fasta.gz
	wget ftp://gsapubftp-anonymous@ftp.broadinstitute.org/bundle/2.8/hg19/ucsc.hg19.fasta.fai.gz
	wget ftp://gsapubftp-anonymous@ftp.broadinstitute.org/bundle/2.8/hg19/ucsc.hg19.dict.gz
	wget ftp://gsapubftp-anonymous@ftp.broadinstitute.org/bundle/2.8/hg19/dbsnp_138.hg19.vcf.gz
	wget ftp://gsapubftp-anonymous@ftp.broadinstitute.org/bundle/2.8/hg19/dbsnp_138.hg19.vcf.idx.gz

Next we need to unzip all these files so they can be used in Halvade:

.. code-block:: bash
	:linenos:

	gunzip ucsc.hg19.fasta.gz && gunzip ucsc.hg19.fasta.fai.gz && gunzip ucsc.hg19.dict.gz && \
	gunzip dbsnp_138.hg19.vcf.gz && gunzip dbsnp_138.hg19.vcf.idx.gz

BWA reference for WGS/WES data
------------------------------



STAR reference for RNA-seq data
-------------------------------
