Run Halvade
===========

To run Halvade the GATK binary needs to be added to the ``bin.tar.gz`` file by executing the following commands:

.. code-block:: bash
	:linenos:

	tar -xvf bin.tar.gz
	rm bin.tar.gz
	cp GenomeAnalysisTK.jar bin/
	tar -cvzf bin.tar.gz bin/*

Similar to the reference files, this file needs to be uploaded to the distributed filesystem if a cluster with more than one node is used. Run this command when using HDFS as distributed storage:

.. code-block:: bash
	:linenos:

	hdfs dfs -put bin.tar.gz /user/ddecap/halvade/


Configuration
-------------
Halvade is started using a python script ``runHalvade.py``, this script reads the configuration from a file given in the first argument. This file contains all options you intend to give to the Halvade command. The configuration file uses a *=* character when a value needs to be provided to the option and the value should be quoted if it is a string. To add an options without arguments add a new line with just the option name. Commented lines, starting with *#*, are ignored by the script.
The ``example.config`` file contains the most basic example, in which the necessary options are provided. The file looks like this:

.. code-block:: bash
	:linenos:

	N=5
	M=64
	C=16
	B="/user/ddecap/halvade/bin.tar.gz"
	D="/user/ddecap/halvade/ref/dbsnp/dbsnp_138.hg19.vcf"
	R="/user/ddecap/halvade/ref/ucsc.hg19"
	I="/user/ddecap/halvade/in/"
	O="/user/ddecap/halvade/out/"

To run the RNA-seq pipeline two additional options need to be provided:

.. code-block:: bash
	:linenos:

	star="/user/ddecap/halvade/ref/STAR_ref/"
	rna

If the nodes in the cluster have hyperthreading enabled, add the ``smt`` option to improve performance. To run the pipeline with a bam file as input, add the ``bam`` option.

.. note:: When running the Halvade job on a single node, it is not requried to upload the reference files to the distributed filesystem. However, the input data should still be preprocessed with the Halvade Uploader tool and put on the distributed filesystem. Running Halvade in this setup, some additional files should be present to allow Halvade to find the references, these should have been added in a previous step. To show Halvade where to find the reference files, add the directory where the required files can be found like this:
	
	.. code-block:: bash
		:linenos:

		refdir="/user/ddecap/halvade/ref/"

	This folder is expected to be on local scratch or a mounted distributed filesystem so this doesn't require a prefix.


Run
---

When all desired configuration for Halvade have been added to the config file, simply run the following command to start Halvade:

.. code-block:: bash
	:linenos:

	python runHalvade.py

This will start Halvade, which in turn will start the necessary Hadoop jobs. The script will return the ID of the process (*PID*) which is used in the filenames to store the standard out and error logs, **halvadePID.stdout** and **halvadePID.stderr**. The output of Halvade will be a single VCF file which can be found in the subdirectory ``merge`` of the provided output directory.

Amazon AWS
----------

To run Halvade on an Amazon EMR cluster, the AWS Command Line Interface needs to be installed, installation instructions can be found  `here <http://docs.aws.amazon.com/cli/latest/userguide/cli-chap-welcome.html>`_. To run Halvade on Amazon EMR, some additional configurations need to be added so the ``runHalvade.py`` script knows Halvade should be started on Amazon EMR. As the Halvade jar isn't available on every node yet, this needs to be uploaded to Amazon S3 first. Similarly, the *bootstrap* script, which creates the ``halvade/`` directory on the mounted SSD's for intermediate data, needs to be uploaded as well.

.. code-block:: bash
	:linenos:

	aws s3 cp  HalvadeWithLibs.jar s3://halv_bucket/user/ddecap/halvade/ref/
	aws s3 cp  halvade_bootstrap.sh s3://halv_bucket/user/ddecap/halvade/ref/

To use Halvade on Amazon EMR an AMI version of 3.1.0 or newer should be used. Add the following EMR configuration to run Halvade on Amazon EMR:

.. code-block:: bash
	:linenos:

	emr_jar="s3://halv_bucket/user/ddecap/halvade/HalvadeWithLibs.jar"
	emr_script="s3://halv_bucket/user/ddecap/halvade/halvade_bootstrap.sh"
	emr_type="c3.8xlarge"
	emr_ami_v="3.1.0"
	tmp="/mnt/halvade/"

The ``tmp`` option is updated to point to the local SSD's on the Amazon EMR nodes, which are mounted in the ``/mnt/`` folder.

