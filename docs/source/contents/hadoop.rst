Hadoop setup
============

Halvade runs on the Hadoop MapReduce framework, if Hadoop MapReduce version 2.0 or newer is already installed on your cluster, you can continue to the `Hadoop configuration`_ section to make sure the advised configuration is enabled. Halvade uses GATK, which requires a specific eversion of Java, currently version 1.7. To make sure GATK works as expected the correct version of Java needs to be installed on every node in the cluster and set as the default Java instance, in Ubuntu use these commands:

.. code-block:: bash
	:linenos:

	sudo apt-get install openjdk-7-jre
	sudo update-alternatives --config java

Single node
-----------

To run Hadoop on a single node, it is advised to install Hadoop in psuedo-distributed mode. The following instructions are based on `this  <https://hadoop.apache.org/docs/r2.7.2/hadoop-project-dist/hadoop-common/SingleCluster.html>`_ tutorial and can be used for additional information. Hadoop requires ssh and rsync to run, to install these on your system, run these commands (on Ubuntu): 

.. code-block:: bash
	:linenos:

	sudo apt-get install ssh rsync

Download and unzip the Hadoop distribution (here 2.7.2):

.. code-block:: bash
	:linenos:

	wget http://www.eu.apache.org/dist/hadoop/common/hadoop-2.7.2/hadoop-2.7.2.tar.gz
	tar -xvf hadoop-2.7.2


To configure the Hadoop installation to run in psuedo-distributed mode edit these files as follows, creating the file or replacing the line if necessary:

``etc/hadoop/hadoop-env.sh``:

.. code-block:: bash
	:linenos:

	export JAVA_HOME=/your/java/bin/directory

``etc/hadoop/core-site.xml``:

.. code-block:: bash
	:linenos:

	<configuration>
	    <property>
	        <name>fs.defaultFS</name>
	        <value>hdfs://localhost:9000</value>
	    </property>
	</configuration>

``etc/hadoop/hdfs-site.xml``:

.. code-block:: bash
	:linenos:

	<configuration>
		<property>
		    <name>dfs.replication</name>
		    <value>1</value>
		</property>
	</configuration>

``etc/hadoop/mapred-site.xml``:

.. code-block:: bash
	:linenos:

	<configuration>
		<property>
		    <name>mapreduce.framework.name</name>
		    <value>yarn</value>
		</property>
	</configuration>

``etc/hadoop/yarn-site.xml``:

.. code-block:: bash
	:linenos:

	<configuration>
		<property>
		    <name>yarn.nodemanager.aux-services</name>
		    <value>mapreduce_shuffle</value>
		</property>
	</configuration>
	
Additionally we need to make sure that that the node can make a passwordless connection to localhost with ssh, check if ``ssh localhost`` works without a password. If this isn't the case run the following commands:

.. code-block:: bash
	:linenos:

	ssh-keygen -t dsa -P '' -f ~/.ssh/id_dsa
	cat ~/.ssh/id_dsa.pub >> ~/.ssh/authorized_keys
	chmod 0600 ~/.ssh/authorized_keys

Now we need to format the NameNode and start the HDFS and Yarn services, do this as follows:

.. code-block:: bash
	:linenos:

	bin/hdfs namenode -format
	sbin/start-dfs.sh
	sbin/start-yarn.sh
	bin/hdfs dfs -mkdir /user
	bin/hdfs dfs -mkdir /user/<username>

Now Hadoop can be run from the ``bin/hadoop`` command and for ease of use this directory can be added to the ``PATH`` variable by adding this line to your ``.bashrc`` file:

.. code-block:: bash
	:linenos:

	export PATH=$PATH:/hadoop/install/dir/bin


After the `Hadoop configuration`_ has been updated to run Halvade optimally on your node, the services will need to be restarted. To restart the pseudo-distributed Hadoop environment run these commands:

.. code-block:: bash
	:linenos:

	sbin/stop-dfs.sh
	sbin/stop-yarn.sh
	sbin/start-dfs.sh
	sbin/start-yarn.sh


Multi node
----------

For the Hadoop installation on a multi node cluster, we refer to the manual given by Cloudera to install CDH 5 or later and configure the Hadoop cluster.  You can find this detailed description online `here <http://www.cloudera.com/content/cloudera/en/documentation/cdh5/v5-0-0/CDH5-Installation-Guide/cdh5ig_cdh5_install.html>`_.


Hadoop configuration
--------------------

After Hadoop is installed, the configuration needs to be updated to run Halvade in an optimal environment. In Halvade, each task processes a portion of the input data. However, the execution time can vary to a certain degree. For this the task timeout needs to be set high enough, in ``mapred-site.xml`` change this property to 30 minutes:

.. code-block:: xml
	:linenos:

	<property>
	  <name>mapreduce.task.timeout</name>
	  <value>1800000</value>
	</property>

The Yarn scheduler needs to know how many cores and how much memory is available on the nodes, this is set in ``yarn-site.xml``. This is very important for the number of tasks that will be started on the cluster. In this example, nodes with 128 GBytes of memory and 24 cores are used. Because some of the tools used benefit from the hyperthreading capabilities of a CPU, the vcores is set to 48 if hyperthreading is available:

.. code-block:: xml
	:linenos:

	<property>
	  <name>yarn.nodemanager.resource.memory-mb</name>
	  <value>131072</value>
	</property>
	<property>
	  <name>yarn.nodemanager.resource.cpu-vcores</name>
	  <value>48</value>
	</property>
	<property>
	  <name>yarn.scheduler.maximum-allocation-mb</name>
	  <value>131072</value>
	</property>
	<property>
	  <name>yarn.scheduler.minimum-allocation-mb</name>
	  <value>512</value>
	</property>
	<property>
	  <name>yarn.scheduler.maximum-allocation-vcores</name>
	  <value>48</value>
	</property>
	<property>
	  <name>yarn.scheduler.minimum-allocation-vcores</name>
	  <value>1</value>
	</property>

After this, the configuration needs to be pushed to all nodes and certain running services restarted. On a single node cluster with Hadoop in pseudo-distributed mode run: 

.. code-block:: bash
	:linenos:

	sbin/stop-dfs.sh
	sbin/stop-yarn.sh
	sbin/start-dfs.sh
	sbin/start-yarn.sh

On a multi node cluster the services running on different nodes need to be restarted after distributing the configuration files, these following commands assume a CDH 5 installation according to the guide shown before:

.. code-block:: bash
	:linenos:

	scp *-site.xml myuser@myCDHnode-<n>.mycompany.com:/etc/hadoop/conf.my_cluster/

On the ResourceManager run:

.. code-block:: bash
	:linenos:

	sudo service hadoop-yarn-resourcemanager restart

On each NodeManager run:

.. code-block:: bash
	:linenos:

	sudo service hadoop-yarn-nodemanager restart

On the JobHistory server run:

.. code-block:: bash
	:linenos:

	sudo service hadoop-mapreduce-historyserver restart

For the RNA-seq pipeline, the memory check needs to be disabled because Halvade uses multiple instances of the STAR aligner when aligning the reads. The genome index files are first loaded into shared memory so every instance can access this instead of loading the reference itself. However, due to the way Hadoop checks physical memory, which includes the shared memory, this check should be disabled. To do this, add these properties to the ``yarn-site.xml`` file.

.. code-block:: bash
	:linenos:

	<property>
	  <name>yarn.nodemanager.vmem-check-enabled</name>
	  <value>false</value>
	</property>
	<property>
	  <name>yarn.nodemanager.pmem-check-enabled</name>
	  <value>false</value>
	</property>

Intel’s Hadoop Adapter for Lustre
---------------------------------
When using Lustre as the filesystem instead of HDFS, using Intel's adapter for Lustre will increase the performance of Halvade. To enable the Adapter for Lustre you need to change some configurations in your Hadoop installation. In ``core-site.xml`` you need to point to the location of Lustre and set the Lustre FileSystem class, if Lustre is mounted on ``/mnt/lustre/``, add these to the file:

.. code-block:: bash
	:linenos:

	<property>
		<name>fs.defaultFS</name>
		<value>lustre:///</value>
	</property>
	<property>
		<name>fs.lustre.impl</name>
		<value>org.apache.hadoop.fs.LustreFileSystem</value>
	</property>
	<property>
		<name>fs.AbstractFileSystem.lustre.impl</name>
		<value>org.apache.hadoop.fs.LustreFileSystem$LustreFs</value>
	</property>
	<property>
		<name>fs.root.dir</name>
		<value>/mnt/lustre/hadoop</value>
	</property>

Additionally, you need to set the Shuffle class in ``mapred-site.xml``:

.. code-block:: bash
	:linenos:

	<property>
		<name>mapreduce.job.map.output.collector.class</name>
		<value>org.apache.hadoop.mapred.SharedFsPlugins$MapOutputBuffer</value>
	</property>
	<property>
		<name>mapreduce.job.reduce.shuffle.consumer.plugin.class</name>
		<value>org.apache.hadoop.mapred.SharedFsPlugins$Shuffle</value>
	</property>

After adding these settings to the configuration, the files need to be pushed to all nodes again and all services restarted, see above. Additionally the jar containing Intel's Adapter for Lustre should be available on all nodes and added to the classpath of Hadoop. To do this you can find the directories that are currently in your hadoop classpath and add the jar to one of these on every node. To find the directories, run this command:

.. code-block:: bash
	:linenos:

	hadoop classpath

