.. halvade documentation master file, created by
   sphinx-quickstart on Mon May  2 10:02:50 2016.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Halvade documentation
=====================

Halvade implements the best-practices pipelines from Broad Institute using the MapReduce framework. Halvade supports pipelines for WGS and Exome sequencing data as well as RNA-seq data. The best-practices pipeline is parallelized using MapReduce where the Map phase performs the read mapping and the Reduce phase takes care of the variant calling with the necessary preprocessnig steps. By using other existing, reliable tools like BWA, STAR, samtools, picard and GATK we ensure that the variants are correctly called. Additionally these tools get frequent updates and Halvade allows simple replacement of the binary file to update the tool. 

Contents:

.. toctree::
   :maxdepth: 2

   contents/introduction
   contents/installation
   contents/references
   contents/hadoop
   contents/upload
   contents/preprocessing
   contents/samples
   contents/run
   contents/options

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

