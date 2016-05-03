Halvade Preprocessing
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

-2 STR			Input file 2. This gives the second pair of paired-end reads in a fastq file.
--dfs			Input on a DFS. This enables reading data from a distributed filesystem like HDFS and amazon S3. 
-i				Interleaved. This is used when one fastq input file is given, the input file is assumed to have
				both pairs of paired-end reads and the reads are interleaved.
--lz4			Lz4 compression. This enables lz4 compression, this is faster than gzip but will require more 
				disk space. The lz4 compression library needs to be enabled in the Hadoop distribution for this 
				to work.
-p, --profile STR		AWS profile. Gives the path of the credentials file used to acces S3. This should be configured 
				when installing the Amazon EMR command line interface. By default this is ``~/.aws/credentials``.
-s, --size INT		Size. This sets the maximum file size (in bytes) of each interleaved file [60MB].
--snappy		Snappy compression. This enables snappy compression, this is faster than gzip but will require 
				more disk space. Snappy requires less disk space than lz4 and is comparable in compression speed. 
				The snappy compression library needs to be enabled in the Hadoop distribution for this to work.
--sse			Server side encryption. Turns on Server side encryption (SSE) when transferring the data to the
 				Amazon S3 storage.
-t INT			Threads. This sets the number of threads used to preprocess the input data.


