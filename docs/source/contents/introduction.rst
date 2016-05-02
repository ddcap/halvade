Introduction
============

Halvade is a Hadoop MapReduce implementation of the best practices pipeline for DNA seq from Broad Institute. This program supports both DNA seq and Exome seq analysis, the output are the vcf files for each region. This program can only be run with Hadoop on either a local cluster or Amazon EMR. The GATK only works with Java v1.7 or newer so this should be installed on every node. If this is not the default Java you should use the –J option to set the location of the Java v1.7 binary.

For Halvade, a reference FASTA file is needed and the corresponding BWT index build by BWA v 0.6 or later for DNA alignment and the STAR genome for RNA alignment. The filenames of the BWA reference index need to start with the full name of the FASTA reference. For variant detection a DBSNP file is also required, this contains known SNPs for the reference FASTA file. On the GATK website you can download the latest human genome reference containing a complete FASTA file and corresponding DBSNP file. 


