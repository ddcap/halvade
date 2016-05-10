Halvade Options
===============

Any directory given in the command line option needs to be accessible by all nodes. This can be either on HDFS, GPFS, Amazon S3 or any other distributed filesystem. When using one node this can also be local scratch. If no prefix is used, HDFS will be used by default. However, the default file system can be changed with the ``fs.defaultFS`` configuration of Hadoop.  When this is changed the directories can simply be given without any prefix, else a prefix ``file:///`` needs to be given for local scratch and mounted GPFS directories. For data stored on S3 when using the Amazon EMR service, the directories need to contain the bucket name as a prefix, e.g. ``S3://bucketname/``. 
A script ``runHalvade.py`` is provided to gather all options in a simple config file which then calls Halvade with all provided options.


Required options
----------------

-B STR			Binary location. This string gives the location where bin.tar.gz is located. 
-D STR			DBSNP file location. This string gives the absolute filename of the DBSNP file, this file needs 
				to be compatible with the reference FASTA file provided by the –R option.
-I STR			Input directory. The string points to the directory containing the preprocessed input or BAM file
				on the used file system.
-O STR			Output directory. This string points to the directory which will contain the output VCF file 
				of Halvade. 
-R STR			Reference Location. This string gives the prefix (without .fasta) of the absolute filename of 
				the reference in FASTA format. The corresponding index files, built with BWA, need to be in 
				this directory having the same prefix as the reference FASTA file. The STAR genome index can 
				be located in a different folder.
-M, --mem <INT>		Memory size. This gives the total memory each node in the cluster has. The memory size is given in GB.
-N, --nodes INT		Node count. This gives the total number of nodes in the local cluster or the number of nodes you want to request when using Amazon EMR. Amazon AWS has a limit of 20 nodes unless the nodes are reserved for an extended period of time.
-C, --vcores INT	Vcores count. This gives the number of cores that can be used per node on the cluster (to enable simultaneous multithreading use the *-smt* option).

Optional options
----------------
-A, --justalign		Just align. This option is used to only align the data. The aligned reads are written to the output folder set with the *–O* option.
--aln INT			Select Aligner. Sets the aligner used in Halvade. Possible values are 0 (bwa aln+sampe)[default], 1 (bwa mem), 2 (bowtie2), 3 (cushaw2). Note that these tools need to be present in the bin.tar.gz file.
--bam				Bam input. This option enables reading aligned BAM input, using this will avoid realigning. If a realignment is required, the data needs to be transformed to FASTQ files, shuffled and preprocessed for Halvade.
--bed STR			Bed region. This option uses a BED file to split the genome in genomic regions that will be processed by one reduce task. This is used when feature count is enabled and the bed region give the known gene boundaries to avoid counting double.
--CA <STR=STR>		Custom arguments. This options allows the tools run with Halvade to be run with additional arguments. The arguments are given in this form: toolname=extra arguments. All options must be correct for the tool in question, multiple arguments can be added by giving a quoted string and separating the arguments with a space. Possible toolnames are bwa_aln, bwa_mem, bwa_sampe, star, elprep, samtools_view, bedtools_bdsnp, bedtools_exome, picard_buildbamindex, picard_addorreplacereadgroup, picard_markduplicates, picard_cleansam, gatk_realignertargetcreator, gatk_indelrealigner, gatk_baserecalibrator, gatk_printreads, gatk_combinevariants, gatk_variantcaller, gatk_variantannotator, gatk_variantfiltration, gatk_splitncigarreads.
--combine			Combine VCF. With this option Halvade will combine VCF files in the input directory and not perform variant calling if the revelant files are found. This is done by default after the variant calling.
--count				Count reads. This counts the reads per Halvade region, this is only used for debugging purposes.
--drop				Drop. Halvade will drop all paired-end reads where the pairs are aligned to different chromosomes.
--dryrun			Dry run. This will initialize Halvade, which calculates the task sizes and region sizes of the chromosomes, but Halvade will not execute the Hadoop jobs.
--fbed STR			Filter on bed. This option will enable the reads to be filtered on the given bed file before performing the GATK steps. This is typically used in an exome dataset where only reads in a known bed file are expected.
--filter_dbsnp		Filter dbsnp. This flag turns on filtering of the dbSNP file before using it in the GATK. This can improve performance in some cases but typically the overhead of converting is too big. 
--gff STR			GFF file. This sets the GFF file that will be used by Featurecounts to count the number of reads per exon.
-H, --haplotypecaller		HaplotypeCaller. With this option Halvade will use the HaplotypeCaller tool from GATK instead of the UnifiedGenotyper tool, which is used by default. This is the newer variant caller which is slower but more accurate.
--id STR			Read Group ID. This string sets the Read Group ID which will be used when adding Read Group information to the intermediate results. [GROUP1]
--illumina			Convert Illumina scores. This Option forces Halvade to convert every basepair quality to the Illumina format. 
-J STR				Java. This string sets the location of the Java binary, this file should be present on every node in the cluster. If this is not set Halvade with use the default Java. This can be used if the default Java is 1.6 and GATK requires version 1.7.
--keep				Keep intermediate files. This option enables all intermediate files to be kept in the temporary folder set by –tmp. This allows the user to check the data after processing.
--lb STR			Read Group Library. This string sets the Read Group Library which will be used when adding Read Group information to the intermediate results. [LIB1]
--mapmem INT		Map Memory. This sets the memory available for the containers assigned for the map tasks. 
--merge_bam			Merge BAM output.  With this option set, Halvade will not perform variant calling but only read alignment. All alignments will be merged into 1 output BAM file.
--mpn INT			Maps per node. This overrides the number of map tasks that are run simultaneously on each node. Only use this when the number of map containers per node does not make sense for your cluster.
-P					Picard. Use Picard in the preprocessing steps, by default elPrep is used which is a more efficient execution of the algorithms called in Picard. This, however, requires less memory and can be useful on some clusters.
--pl STR			Read Group Platform. This string sets the Read Group Platform which will be used when adding Read Group information to the intermediate results. [ILLUMINA]
--pu STR			Read Group Platform Unit. This string sets the Read Group Platform Unit which will be used when adding Read Group information to the intermediate results. [UNIT1]
--redistribute		Redistribute Cores. This is an optimization to better utilize the CPU cores at the end of the map phase, to improve load balancing. Only use when the cores per container is less than 4.
--redmem INT		Reduce Memory. This sets the memory available for the containers assigned for the reduce tasks. 
--refdir STR		Reference directory. This sets the reference directory, Halvade will use this directory to find existing references on each node. This directory needs to be accessible by all nodes, but can be a local disk or a network disk. Halvade finds the reference files by looking for files in the directory or subdirectory with these suffixes: .bwa_ref, .gatk_ref, .star_ref, .dbsnp.
--remove_dups		Remove Duplicates. This will remove the found PCR duplicates in the corresponding step.
--report_all		Report all output. This option will give all VCF output records in the merged output file. By default the VCF record with the highest score will be kept if multiple records are found at the same location.
--rna				RNA pipeline. This options enables Halvade to run the RNA-seq pipeline instead of the default DNA pipeline. This option requires an additional argument *S* which points to the STAR genome directory.
--rpn INT			Reduces per node. This overrides the number of reduce tasks that are run simultaneously on each node. Only use this when the number of reduce containers per node does not make sense for your cluster.
-S, --star STR		Star genome. This gives the directory of the STAR genome reference. 
--scc INT			stand_call_conf. The value of this option will be used for the stand_call_conf when calling the GATK Variant Caller.
--sec INT			stand_emit_conf. The value of this option will be used for the stand_emit_conf when calling the GATK Variant Caller.
--single			Single-end reads. This option sets the input to be single-ended reads. By default, Halvade reads in paired-end interleaved FASTQ files.
--sm STR			Read Group Sample Name. This string sets the Read Group Sample Name which will be used when adding Read Group information to the intermediate results. [SAMPLE1]
--smt				Simultaneous multithreading. This option enables Halvade to use simultaneous multithreading on each node.
--stargtf STR		GFF for STAR. This option point to the GFF/GTF file to be used when rebuilding the STAR genome, this can improve accuracy when finding splice sites.
--tmp STR			Temporary directory. This string gives the location where intermediate files will be stored. This should be on a local disk for every node for optimal performance.
--update_rg			Update read group. This forces the readgroup to be updated to the one provided by the options, even if the input is read from a BAM file with a read group present.
-v INT				Verbosity. This sets the verbosity level for debugging, default is [2].


