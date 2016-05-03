Installation
============

Every Halvade release is available at `github <https://github.com/biointec/halvade/releases>`_. Download and extract the latest release, currently v1.2.0:

.. code-block:: bash
	:linenos:

	wget https://github.com/ddcap/halvade/releases/download/v1.2.0/Halvade_v1.2.0.tar.gz
	tar -xvf Halvade_v1.2.0.tar.gz

The files that are provided in this package are the following:

.. code-block:: bash
	:linenos:

	example.config
	runHalvade.py
	halvade_bootstrap.sh
	HalvadeWithLibs.jar
	HalvadeUploaderWithLibs.jar
	bin.tar.gz


Build from source
-----------------

Halvade can also be built from the source files. To do this, first you need to clone the github repository and built the package with *ant* as follows:

.. code-block:: bash
	:linenos:

	git clone https://github.com/biointec/halvade.git
	cd halvade/halvade/
	ant
	cd ../halvade_upload_tool/
	ant
	cd ../

This will build the two jar files in the respective directories. The scripts and example configuration files can be found in the scripts directory, move the jar files to the script directory so the scripts can run the jar file:

.. code-block:: bash
	:linenos:

	cp halvade/dist/HalvadeWithLibs.jar scripts/
	cp halvade_upload_tool/dist/HalvadeUploaderWithLibs.jar scripts/


The next step is to download the human genome reference files and prepare them to use with Halvade.

