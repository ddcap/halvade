Introduction
============

Halvade is a Hadoop MapReduce implementation of the best practices pipeline from Broad Institute for whole genome and exome sequencing (DNA) as well as RNA-seq. Halvade will produce a VCF output file which contains the single nucleotide variants (SNVs) and additionally insertions and deletions (indels) in certain pipelines. This program requires Hadoop on either a local cluster with one or more nodes or an Amazon EMR cluster to run. As Hadoop is typically run on a linux cluster, this documentation only provides information for a linux setup. The GATK used in Halvade only works with Java v1.7, so this version of Java should be installed on every node.

.. note:: Halvade is available under the GNU license and provides a binary with all opensource tools. However, since the GATK has its own license, which is available online `here <https://www.broadinstitute.org/gatk/about/#licensing>`_, the GATK binary is not provided in the bin.tar.gz file and needs to be added individually.

Halvade depends on existing tools to run the pipeline, these tools require additional data besides the raw sequenced reads. This data consists of the human genome reference FASTA file, some additional files created using the reference and the dbSNP database. The index file used in the BWA or STAR aligners are created with the tools itself. The FASTA index and dictionary files used by the GATK are created by samtools and Picard. The naming convention for these files and additional information to run Halvade is provided in this Halvade documentation. 

Recipes
-------

Two recipes have been created to run Halvade on WGS and RNA-seq data. These show all commands needed to run Halvade with the example data. The recipe for the DNA-seq pipeline can be found `here <https://github.com/biointec/halvade/wiki/Recipe:-DNA-seq-with-Halvade-on-a-local-Hadoop-cluster>`_. The RNA-seq data is typically less big and therefore we chose to provide a recipe to run the Halvade rna pipeline on a single node Hadoop environment, which can be found `here <https://github.com/biointec/halvade/wiki/Recipe:-RNA-seq-with-Halvade-on-a-local-Hadoop-cluster>`_.

