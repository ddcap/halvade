Halvade Options
===============

Any directory given in the command line option needs to be accessible by all nodes. This can be either on HDFS, GPFS, Amazon S3 or any other distributed filesystem, additionally when using only 1 node this can also be local scratch. If no prefix is used, HDFS will be used by default. However, the default file system can be changed with the ``fs.defaultFS`` configuration of Hadoop.  When this is changed the directories can simply be given without any prefix, else a prefix ``file:///`` needs to be given for local scratch and mounted GPFS directories. For data stored on S3 when using the Amazon EMR service, the directories need to contain the bucket name as a prefix, e.g. ``S4://bucketname/``. 
A script ``runHalvade.py`` is provided to gather all options in a simple config file which then calls Halvade with all provided options.


Required options
----------------

-B STR			Binary location. This string gives the location where bin.tar.gz is located. 
-D STR			DBSNP file location. This string gives the absolute filename of the DBSNP file, this file needs to be compatible with the reference FASTA file provided by the –R option.
-I STR			Input directory. The string points to the directory containing the preprocessed input on the used file system.
-O STR			Output directory. This string points to the directory which will contain the output vcf files of Halvade. 
-R STR			Reference Location. This string gives the prefix (without .fasta) of the absolute filename of the reference in FASTA format. The corresponding index files, built with BWA, need to be in this directory having the same prefix as the reference FASTA file. The STAR genome index is located in different folder.
-mem <INT>		Memory size. This gives the total memory each node in the cluster has. The memory size is given in GB.
-nodes INT		Node count. This gives the total number of nodes in the local cluster or the number of nodes you want to request when using Amazon EMR. Amazon AWS has a limit of 20 nodes unless the nodes are reserved for an extended period of time.
-vcores INT		Vcores count. This gives the number of cores that can be used per node on the cluster (to enable simultaneous multithreading use the -smt option).

Optional options
----------------


Halvade Uploader Options
========================

The Halvade Uploader will preprocesses the fastq files, this will interleave the paired-end reads and split the files in pieces of 60MB (by default, can be changed with the **-size** option). The Halvade Uploader will automatically upload these preprocessed files to the given output directory on either local scratch, GPFS, HDFS, Amazon S3 or any other distirubted file system. The prefix for the used distributed file system is the same as with the Halvade tool.


Synopsis
--------
.. code-block:: bash
	:linenos:

	Hadoop jar HalvadeUploaderWithLibs.jar –1 /dir/to/input.manifest -O /halvade/out/ –t 8
	Hadoop jar HalvadeUploaderWithLibs.jar –1 /dir/to/reads1.fastq -2 /dir/to/reads2.fastq -O /halvade/out/ –t 8
	Hadoop jar HalvadeUploaderWithLibs.jar –1 /dir/to/input.manifest -O s3://bucketname/halvade/out/ -profile /dir/to/credentials.txt –t 8


Performance
-----------

For better performance it is advised to increase the Java heap memory for the hadoop command, e.g. for 32GB:

.. code-block:: bash

	export HADOOP_HEAPSIZE=32768

Required options
----------------

-1 STR			Manifest/Input file. This string gives the absolute path of the Manifest file or the first input fastq file. This manifest file contains a line per file pair, separated by a tab: */dir/to/fastq1.fastq /dir/to/fastq2.fastq*. If this is equal to '-' then the fastq reads are read from standard input.
-O STR			Output directory. This string gives the directory where the output files will be put. 

Optional options
----------------

-2 STR		Input file 2. This gives the second pair of paired-end reads in a fastq file.
-dfs			Input on a DFS. This enables reading data from a distributed filesystem like HDFS and amazon S3. 
-i				Interleaved. This is used when one fastq input file is given, the input file is assumed to have both pairs of paired-end reads and the reads are interleaved.
-lz4			Lz4 compression. This enables lz4 compression, this is faster than gzip but will require more disk space. The lz4 compression library needs to be enabled in the Hadoop distribution for this to work.
-profile STR	AWS profile. Gives the path of the credentials file used to acces S3. This should be configured when installing the Amazon EMR command line interface. By default this is ``~/.aws/credentials``.
-size INT		Size. This sets the maximum file size (in bytes) of each interleaved file [60MB].
-snappy			Snappy compression. This enables snappy compression, this is faster than gzip but will require more disk space. Snappy requires less disk space than lz4 and is comparable in compression speed. The snappy compression library needs to be enabled in the Hadoop distribution for this to work.
-sse			Server side encryption. Turns on Server side encryption (SSE) when transferring the data to the Amazon S3 storage.
-t INT			Threads. This sets the number of threads used to preprocess the input data.


