Introduction
============

Halvade is a Hadoop MapReduce implementation of the best practices pipeline for whole genome and exome sequencing as well as RNA-seq from Broad Institute. Halvade will produce a vcf output file which contains the single nucleotide variants (SNVs) and additionally indels in certain pipelines. This program requires Hadoop on either a local cluster, a single machine or an Amazon EMR cluster to run. As Hadoop is typically run on a linux cluster or machine, this documentation only provides information for a linux setup. The GATK used in Halvade only works with Java v1.7, so this version should be installed on every node.

.. note:: Halvade is available under the GNU license. However, since the GATK has its own license, which is available online `here <https://www.broadinstitute.org/gatk/about/#licensing>`_, the GATK binary is not provided in the bin.tar.gz file and needs to be added individually.

Halvade depends on existing tools to run the pipeline, these tools require additional data besides the input data. This data consists of the human genome reference fasta file, some additional files created using this one file and the dbSNP database. The index file used in the BWA or STAR aligners are created with the tools itself. The fasta index and dictionary files used by the GATK are created by samtools and Picard. The naming convention and additional information to run Halvade is all provided in this Halvade documentation. 

