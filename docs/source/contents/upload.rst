Uploading the references
========================

The reference data needs to be available to all nodes in the cluster, which is why they should be available on the distributed filesystem. When running Halvade, the references will be copied to local scratch on every node when they need to be accessed to increase the performance of subsequent accessing of the file. 
 
.. note:: The reference files shouldn't be uploaded to the distributed filesystem if a single node Hadoop environment is used. The tool would download them to local scratch to use. Instead we put the files on local scratch and add some additional files so that Halvade can find the correct references. Additionally, the ``refdir`` option should be set that points to the directory with all reference files when running Halvade. There are four files that are used to find the corresponding reference files and directories, these should be added to correspond with the reference names:

	.. code-block:: bash
		:linenos:

		touch ucsc.hg19.bwa_ref
		touch ucsc.hg19.gatk_ref
		touch STAR_ref/.star_ref
		touch dbsnp/.dbsnp

HDFS
----

The reference files need to be copied to the HDFS so that Halvade can distribute them to every node to be used locally. Here we will create a directory on HDFS where all the files will be collected, execute the following commands to do this:

.. code-block:: bash
	:linenos:

	hdfs dfs -mkdir -p /user/ddecap/halvade/ref/dbsnp
	hdfs dfs -put ucsc.hg19.* /user/ddecap/halvade/ref/
	hdfs dfs -put dbsnp/dbsnp_138.hg19.* /user/ddecap/halvade/ref/dbsnp/

	# for the RNA pipeline copy the STAR ref:
	hdfs dfs -put STAR_ref/ /user/ddecap/halvade/ref/


Amazon S3
---------

To copy the files to Amazon AWS with the terminal the AWS Command Line Interface needs to be installed using `this <http://docs.aws.amazon.com/cli/latest/userguide/cli-chap-welcome.html>`_ documentation. If the bucket you want to use is called ``halv_bucket``, execute the following commands:

.. code-block:: bash
	:linenos:

	aws s3 cp  ./ s3://halv_bucket/user/ddecap/halvade/ref/ --include "ucsc.hg19.*" 
	aws s3 cp  dbsnp/ s3://halv_bucket/user/ddecap/halvade/ref/dbsnp/ --include "dbsnp_138.hg19.*"

	# for the RNA pipeline copy the STAR ref:
	aws s3 cp  STAR_ref/ s3://halv_bucket/user/ddecap/halvade/ref/ --recursive




GPFS & Lustre
-------------

Typically GPFS or Lustre are mounted on the directory on every node, the reference files simply need to be copied to that directory. If ``/mnt/dfs`` is the mounted distributed filesystem, execute the following commands: 

.. code-block:: bash
	:linenos:

	mkdir -p /mnt/dfs/halvade/ref/dbsnp
	cp ucsc.hg19.* /mnt/dfs/halvade/ref/
	cp -r dbsnp/dbsnp_138.hg19.* /mnt/dfs/halvade/ref/dbsnp/

	# for the RNA pipeline copy the STAR ref:
	cp -r STAR_ref/ /mnt/dfs/halvade/ref/
