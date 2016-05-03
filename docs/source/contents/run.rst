Run Halvade
===========

To run Halvade the GATK binary needs to be added to the ``bin.tar.gz`` file by executing the following commands:

.. code-block:: bash
	:linenos:

	tar -xvf bin.tar.gz
	rm bin.tar.gz
	cp GenomeAnalysisTK.jar bin/
	tar -cvzf bin.tar.gz bin/*

Similar to the reference files this needs to be uploaded to the distributed filesystem if a cluster is used. 


Configuration
-------------
Halvade is run using a python script ``runHalvade.py``, this script reads the configuration from a file given in the first argument. This file contains all options you intend to give to the Halvade command. The configuration file uses a *=* character when a value needs to be provided to the option and the value should be quoted if it is a string. To add an options without arguments add a new line with the option name. Commented lines, starting with *#*, are ignored by the script.
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

If the nodes in the cluster have hyperthreading enabled, add the ``smt`` option to improve performance. 

.. note:: When running the Halvade job on a single node, it is not requried to upload the reference files to the distributed filesystem. While the input data should still be preprocessed with the Halvade Uploader tool and put on the distributed filesystem. Running Halvade in this setup, some additional files should be present to allow Halvade to find the references, these should have been added in a previous step. Additionaly add the directory where the reference files can be found like this:
	
	.. code-block:: bash
		:linenos:

		refdir="/user/ddecap/halvade/ref/"

To run the pipeline with a bam file as input simple add the bam options:

.. code-block:: bash
	:linenos:

	bam

Run
---

When all desired configuration for Halvade have been added to the config file, simply run the following command to start Halvade:

.. code-block:: bash
	:linenos:

	python runHalvade.py

This will start Halvade, which in turn will start the necessary Hadoop jobs. The script will return the ID of the process (*PID*) which is used in the filenames to store the standard out and error logs, namely in the files **halvadePID.stdout** and **halvadePID.stderr**.

Amazon AWS
----------

To run Halvade using on an Amazon AWS cluster, the AWS Command Line Interface needs to be installed, installation instructions can be found  `here <http://docs.aws.amazon.com/cli/latest/userguide/cli-chap-welcome.html>`_.
