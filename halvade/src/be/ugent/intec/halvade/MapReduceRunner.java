/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package be.ugent.intec.halvade;

import fi.tkk.ics.hadoop.bam.FastqInputFormat;
import fi.tkk.ics.hadoop.bam.SAMRecordWritable;
import fi.tkk.ics.hadoop.bam.VariantContextWritable;
import be.ugent.intec.halvade.hadoop.datatypes.ChromosomeRegion;
import be.ugent.intec.halvade.hadoop.partitioners.*;
import java.io.EOFException;
import java.io.IOException;
import java.net.URISyntaxException;
import net.sf.samtools.SAMSequenceDictionary;
import net.sf.samtools.SAMSequenceRecord;
import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import be.ugent.intec.halvade.utils.Logger;
import be.ugent.intec.halvade.utils.MyConf;
import be.ugent.intec.halvade.utils.Timer;
import fi.tkk.ics.hadoop.bam.VCFInputFormat;
import java.io.File;
import java.net.URI;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;

/**
 *
 * @author ddecap
 */
public class MapReduceRunner extends Configured implements Tool  {
    protected Options options = new Options();
    protected String in;
    protected String out;
    protected String ref;
    protected String java = null;
    protected String tmpDir = "/tmp/halvade/";
    protected String localRef = tmpDir + "ref.fa";
    protected String sites;
    protected String manifest = null;
    protected int nodes, vcores, mem;
    protected int mappers = 1, reducers = 1, mthreads = 1, GATKdataThreads = 1, GATKCPUThreads = 1;
    protected int regionSize = -1;
    protected String[] hdfsSites;
    protected String[] FASTQ_ENCODING = {"sanger", "illumina"};
    protected boolean paired = true;
    protected boolean aln = true;
    protected boolean justPut = false;
    protected boolean justCombine = false;
    protected boolean useBedTools = false;
    protected boolean useGenotyper = true;
    protected String RGID = "GROUP1";
    protected String RGLB = "LIB1";
    protected String RGPL = "ILLUMINA";
    protected String RGPU = "UNIT1";
    protected String RGSM = "SAMPLE1";
    protected boolean useIPrep = true;
    protected boolean keepFiles = false;
    protected final long MAXFILESIZE = 251658240L; // 240MB
    protected final long MINFILESIZE = 65011712L; // 62MB
    protected final long INCFILESIZE = 5242880L; // 5MB
    protected int stand_call_conf = -1;
    protected int stand_emit_conf = -1;
    protected int MAP_M = 10; // # files per mapper as input!
    protected int RED_M = 4; // # keys per reducer
    protected SAMSequenceDictionary dict;
    protected String chr = null;
    protected int multiplier;
    protected int minChrLength;
    protected int reducersPerContainer;
    protected int mapsPerContainer;
    protected boolean reuseJVM = false;
    protected boolean justAlign = false;
    protected String exomeBedFile = null;
    protected int coverage = 50; 
    protected String halvadeDir = "/halvade/";
    protected String bin;
            
    @Override
    public int run(String[] strings) throws Exception {
        int ret = 1;
        try {
            parseArguments(strings);
            // initialise MapReduce
            Configuration halvadeConf = getConf();
            // add parameters to configuration:
            localRef = tmpDir + "ref.fa";
            getBestDistribution(halvadeConf);
            MyConf.setTasksPerNode(halvadeConf, reducersPerContainer);
            MyConf.setScratchTempDir(halvadeConf, tmpDir);
            MyConf.setRefOnHDFS(halvadeConf, ref);
            MyConf.setRefOnScratch(halvadeConf, localRef);
            MyConf.setKnownSitesOnHDFS(halvadeConf, hdfsSites);
            MyConf.setNumThreads(halvadeConf, mthreads);
            MyConf.setGATKNumDataThreads(halvadeConf, GATKdataThreads);
            MyConf.setGATKNumCPUThreads(halvadeConf, GATKCPUThreads);
            MyConf.setNumNodes(halvadeConf, mappers);
            MyConf.setIsPaired(halvadeConf, paired);
            if(exomeBedFile != null)
                MyConf.setExomeBed(halvadeConf, exomeBedFile);
//            MyConf.setBinDir(conf, bin);
            MyConf.setFastqEncoding(halvadeConf, FASTQ_ENCODING[0]);
            MyConf.setOutDir(halvadeConf, out);
            MyConf.setKeepFiles(halvadeConf, keepFiles);
            MyConf.setUseBedTools(halvadeConf, useBedTools);
            MyConf.clearTaskFiles(halvadeConf);
            MyConf.setUseIPrep(halvadeConf, useIPrep);
            MyConf.setUseUnifiedGenotyper(halvadeConf, useGenotyper);
            MyConf.setReuseJVM(halvadeConf, reuseJVM);
            MyConf.setReadGroup(halvadeConf, "ID:" + RGID + " LB:" + RGLB + " PL:" + RGPL + " PU:" + RGPU + " SM:" + RGSM);
            if(chr != null )
                MyConf.setChrList(halvadeConf, chr);
            if(java != null)
                MyConf.setJava(halvadeConf, java);
                    
            if(stand_call_conf > 0) 
                MyConf.setSCC(halvadeConf, stand_call_conf);
            if(stand_emit_conf > 0) 
                MyConf.setSEC(halvadeConf, stand_emit_conf);
            // check if output is cleared
            FileSystem fs = FileSystem.get(new URI(out), halvadeConf);
            if (fs.exists(new Path(out)) && !justCombine) {
                Logger.INFO("The output directory \'" + out + "\' already exists.");
                Logger.INFO("WARNING: Deleting the previous output directory!");
                fs.delete(new Path(out), true);
//                System.err.println("Please remove this directory before trying again.");
//                System.exit(-2);
            }
            // prepare the sequence dictionary for the reducer:
            parseANNFile(halvadeConf);
            setKeysPerChromosome();
            MyConf.setMinChrLength(halvadeConf, minChrLength);
            MyConf.setMultiplier(halvadeConf, multiplier);
            getNumberOfRegions(halvadeConf);
           
            // only put files or continue?
            if(justPut)
                return 0;
            
            if(!halvadeDir.endsWith("/"))
                halvadeDir += "/";
            URI binTar = new URI(halvadeDir + "bin.tar.gz");
            
            // add to dist cache with conf
            // try to use by copying cache to every node
//            DistributedCache.addCacheArchive(binTar, conf);
            
            Job halvadeJob = new Job(halvadeConf, "Halvade");
            // add to dist cache with job
            halvadeJob.addCacheArchive(binTar);   
            
            
            halvadeJob.setJarByClass(be.ugent.intec.halvade.hadoop.mapreduce.BWAMemMapper.class);
            // specify input and output dirs
            // check if input is a file or directory
            
            // s3 gives error here that isDirectory() doesn't exist....
            try {
                if (fs.getFileStatus(new Path(in)).isDirectory()) {
                    // add every file in directory
                    FileStatus[] files = fs.listStatus(new Path(in));
                    for(FileStatus file : files) {
                        if (!file.isDirectory()) {
                            FileInputFormat.addInputPath(halvadeJob, file.getPath());
                        }
                    }
                } else {
                    FileInputFormat.addInputPath(halvadeJob, new Path(in));
                }
                
            } catch (Exception e) {
                Logger.EXCEPTION(e);
            }
            FileOutputFormat.setOutputPath(halvadeJob, new Path(out));
            
            // specify a mapper       
            if (aln) halvadeJob.setMapperClass(be.ugent.intec.halvade.hadoop.mapreduce.BWAAlnMapper.class);
            else halvadeJob.setMapperClass(be.ugent.intec.halvade.hadoop.mapreduce.BWAMemMapper.class);
            halvadeJob.setMapOutputKeyClass(ChromosomeRegion.class);
            halvadeJob.setMapOutputValueClass(SAMRecordWritable.class);
            halvadeJob.setInputFormatClass(FastqInputFormat.class);
            
            // per chromosome && region
            halvadeJob.setPartitionerClass(ChrRgPartitioner.class);
            halvadeJob.setSortComparatorClass(ChrRgPositionComparator.class);
            halvadeJob.setGroupingComparatorClass(ChrRgRegionComparator.class);
            
            // # reducers
            if(justAlign)
                halvadeJob.setNumReduceTasks(0);
            else
                halvadeJob.setNumReduceTasks(reducers);
            // specify a reducer
            halvadeJob.setReducerClass(be.ugent.intec.halvade.hadoop.mapreduce.GATKReducer.class);
            halvadeJob.setOutputKeyClass(Text.class);
            halvadeJob.setOutputValueClass(VariantContextWritable.class);
//            job.setOutputFormatClass(VCFOutputFormat.class);
            
            Timer timer = new Timer();
            timer.start();
            ret = halvadeJob.waitForCompletion(true) ? 0 : 1;
            timer.stop();
            Logger.DEBUG("Running time of Halvade Job: " + timer);
            
            /**
             * combine resulting files:
             */
            Logger.DEBUG("combining output");            
            Configuration combineConf = getConf();  
            if(!out.endsWith("/")) out += "/";  
            MyConf.setInputDir(combineConf, out);
            MyConf.setOutDir(combineConf, out + "combinedVCF/");
            Job combineJob = new Job(combineConf, "HalvadeCombineVCF");            
            combineJob.setJarByClass(be.ugent.intec.halvade.hadoop.mapreduce.VCFCombineMapper.class);
            
            FileInputFormat.addInputPath(combineJob, new Path(out));
            FileOutputFormat.setOutputPath(combineJob, new Path(out + "combinedVCF/"));
            
            combineJob.setMapperClass(be.ugent.intec.halvade.hadoop.mapreduce.VCFCombineMapper.class);
            combineJob.setMapOutputKeyClass(LongWritable.class);
            combineJob.setMapOutputValueClass(VariantContextWritable.class);
            combineJob.setInputFormatClass(VCFInputFormat.class);
            combineJob.setNumReduceTasks(1); 
            combineJob.setReducerClass(be.ugent.intec.halvade.hadoop.mapreduce.VCFCombineReducer.class);
            combineJob.setOutputKeyClass(Text.class);
            combineJob.setOutputValueClass(VariantContextWritable.class);
//            combineJob.setOutputFormatClass(KeyIgnoringVCFOutputFormat.class);
            
            timer = new Timer();
            timer.start();
            ret = combineJob.waitForCompletion(true) ? 0 : 1;
            timer.stop();
            Logger.DEBUG("Running time of Combine Job: " + timer);
            
            
        } catch (ParseException e) {
            // automatically generate the help statement
            System.err.println("Error parsing: " + e.getMessage());
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp( "hadoop jar Halvade -I <IN> -O <OUT> " +
                    "-R <REF> -D <SITES> -B <BIN> -nodes <nodes> -mem <mem> -vcores <cores> [options]", options );
        } catch (Exception e) {
            Logger.EXCEPTION(e);
        }
        return ret;
    }
    
    public void createOptions() {    //setup options
        Option optIn = OptionBuilder.withArgName( "input" )
                                .hasArg()
                                .isRequired(true)
                                .withDescription(  "Input directory on hdfs containing fastq files." )
                                .create( "I" );
        Option optOut = OptionBuilder.withArgName( "output" )
                                .hasArg()
                                .isRequired(true)
                                .withDescription(  "Output directory on hdfs." )
                                .create( "O" );
        Option optBin = OptionBuilder.withArgName( "binary directory" )
                                .hasArg()
                                .isRequired(true)
                                .withDescription(  "The directory where the BWA binary file is located on HDFS [/halvade/]." )
                                .create( "B" );
        Option optRef = OptionBuilder.withArgName( "reference.fa" )
                                .hasArg()
                                .isRequired(true)
                                .withDescription(  "Name of the fastq file name of the reference. Make sure the index has the same prefix." )
                                .create( "R" );
        Option optSites = OptionBuilder.withArgName( "dbsnps" )
                                .hasArg()
                                .isRequired(true)
                                .withDescription(  "Name of dbsnp files for the genome. If multiple separate with \',\'." )
                                .create( "D" );
        Option optTmp = OptionBuilder.withArgName( "tempdir" )
                                .hasArg()
                                .withDescription(  "Sets the location for temporary files on every node [default is /tmp/halvade/]." )
                                .create( "tmp" );
        Option optSingle = OptionBuilder.withArgName( "single end reads" )
                                .withDescription(  "Sets the input files to single reads [default is paired-end reads]." )
                                .create( "s" );
        Option optBmem = OptionBuilder.withArgName( "BWA mem" )
                                .withDescription(  "Use BWA mem instead of default BWA aln & sampe/samse (better for longer reads)." )
                                .create( "bwamem" );
        Option optRSize = OptionBuilder.withArgName( "region size" )
                                .hasArg()
                                .withDescription(  "Sets region size to use when splitting sam files." )
                                .create( "r" );
        Option optMan = OptionBuilder.withArgName( "manifest" )
                                .hasArg()
                                .withDescription(  "Filename containing the input files to be put on HDFS." )
                                .create( "M" );
        Option optJava = OptionBuilder.withArgName( "java" )
                                .hasArg()
                                .withDescription(  "Set location of java binary to use [must be 1.7+]." )
                                .create( "J" );
        Option optPut = OptionBuilder.withArgName( "put" )
                                .withDescription(  "Just puts the data on HDFS and doesn't run the hadoop job." )
                                .create( "p" );
        Option optCombine = OptionBuilder.withArgName( "combine" )
                                .withDescription(  "Just Combines the vcf on HDFS [out dir] and doesn't run the hadoop job." )
                                .create( "c" );
        // "ID:" + RGID + " LB:" + RGLB + " PL:" + RGPL + " PU:" + RGPU + " SM:" + RGSM
        Option optID = OptionBuilder.withArgName( "RGID" )
                                .hasArg()
                                .withDescription(  "sets the RGID for the read-group." )
                                .create( "id" );
        Option optLB = OptionBuilder.withArgName( "RGLB" )
                                .hasArg()
                                .withDescription(  "sets the RGLB for the read-group." )
                                .create( "lb" );
        Option optPL = OptionBuilder.withArgName( "RGPL" )
                                .hasArg()
                                .withDescription(  "sets the RGPL for the read-group." )
                                .create( "pl" );
        Option optPU = OptionBuilder.withArgName( "RGPU" )
                                .hasArg()
                                .withDescription(  "sets the RGPU for the read-group." )
                                .create( "pu" );
        Option optSM = OptionBuilder.withArgName( "RGSM" )
                                .hasArg()
                                .withDescription(  "sets the RGSM for the read-group." )
                                .create( "sm" );
        Option optPp = OptionBuilder.withArgName( "picard preprocess" )
                                .withDescription(  "Uses Picard to preprocess the data for GATK." )
                                .create( "P" );
        Option optBed = OptionBuilder.withArgName( "bedtools dbsnp" )
                                .withDescription(  "Use Bedtools to select an interval of dbsnp." )
                                .create( "b" );
        Option optJVM = OptionBuilder.withArgName( "reuse JVM" )
                                .withDescription(  "Set this to enable reusing JVM (avoids loading reference multiple times)." )
                                .create( "rjvm" );
        Option optJustAlign = OptionBuilder.withArgName( "justalign" )
                                .withDescription(  "Only align the reads." )
                                .create( "justalign" );
        Option optKeep = OptionBuilder.withArgName( "keep tmp files" )
                                .withDescription(  "Keep intermediate files." )
                                .create( "keep" );
        Option optHap = OptionBuilder.withArgName( "use haplotypecaller" )
                                .withDescription(  "Use HaplotypeCaller instead of UnifiedGenotyper for Variant Detection." )
                                .create( "hc" );
        Option optCov = OptionBuilder.withArgName( "coverage" )
                                .hasArg()
                                .withDescription(  "Sets the coverage to better distribute the tasks.")
                                .create( "cov" );
        Option optScc = OptionBuilder.withArgName( "stand_call_conf" )
                                .hasArg()
                                .withDescription(  "Sets stand_call_conf for gatk Variant Caller." )
                                .create( "scc" );
        Option optSec = OptionBuilder.withArgName( "stand_emit_conf" )
                                .hasArg()
                                .withDescription(  "Sets stand_emit_conf for gatk Variant Caller." )
                                .create( "sec" );
        Option optChr = OptionBuilder.withArgName( "chromosome" )
                                .hasArg()
                                .withDescription(  "Sets the chromosomes if reads don't cover the full reference (chrM,chr2,...)." +
                                        "This only changes how the regions will be distributed not the reference.")
                                .create( "chr" );
        Option optEx = OptionBuilder.withArgName( "exome bed file" )
                                .hasArg()
                                .withDescription(  "Gives the location of a bed file for exome target regions. Required for exome sequences. ")
                                .create( "exome" );
        /*
        Option optMapThreads = OptionBuilder.withArgName( "mapthreads" )
                                .hasArg()     
                                .isRequired(true)
                                .withDescription(  "Sets number of threads available on each mapper." )
                                .create( "mt" );
        Option optMapCount = OptionBuilder.withArgName( "mapcount" )
                                .hasArg()                
                                .isRequired(true)
                                .withDescription(  "Sets the maximum number of mappers in MapReduce." )
                                .create( "mc" );
        Option optReduceThreads = OptionBuilder.withArgName( "reducethreads" )
                                .hasArg()     
                                .withDescription(  "Sets number of threads available on each reducer if different from mapthreads." )
                                .create( "rt" );
        Option optReduceCount = OptionBuilder.withArgName( "reducecount" )
                                .hasArg()
                                .withDescription(  "Sets the maximum number of reducers in MapReduce if different from max mappers." )
                                .create( "rc" );
        */
        Option optNodes = OptionBuilder.withArgName( "nodes" )
                                .hasArg()                
                                .isRequired(true)
                                .withDescription(  "Sets the number of nodes in this cluster." )
                                .create( "nodes" );
        Option optVcores = OptionBuilder.withArgName( "cores" )
                                .hasArg()                
                                .isRequired(true)
                                .withDescription(  "Sets the available cpu cores per node in this cluster." )
                                .create( "vcores" );
        Option optMem = OptionBuilder.withArgName( "gb" )
                                .hasArg()                
                                .isRequired(true)
                                .withDescription(  "Sets the available memory [in GB] per node in this cluster." )
                                .create( "mem" );
        
        
        options.addOption(optIn);
        options.addOption(optOut);
        options.addOption(optRef);
        options.addOption(optSites);
        options.addOption(optBin);
        options.addOption(optTmp);
        options.addOption(optSingle);
        options.addOption(optBmem);
        options.addOption(optRSize);
        options.addOption(optMan);
        options.addOption(optPut);
        options.addOption(optID);
        options.addOption(optLB);
        options.addOption(optPL);
        options.addOption(optPU);
        options.addOption(optSM);
        options.addOption(optPp);
        options.addOption(optBed);
        options.addOption(optHap);
        options.addOption(optScc);
        options.addOption(optSec);
        options.addOption(optChr);
        options.addOption(optJVM);
        options.addOption(optJava);
        options.addOption(optCombine);
        options.addOption(optNodes);
        options.addOption(optVcores);
        options.addOption(optMem);
        options.addOption(optKeep);
        options.addOption(optJustAlign);
        options.addOption(optCov);
        options.addOption(optEx);
        /*
        options.addOption(optMapThreads);
        options.addOption(optMapCount);
        options.addOption(optReduceThreads);
        options.addOption(optReduceCount);
        */
    }
    
    public void parseArguments(String[] args) throws ParseException {
        createOptions();
        CommandLineParser parser = new GnuParser();
        CommandLine line = parser.parse(options, args);
        in = line.getOptionValue("I");
        out = line.getOptionValue("O");
        ref = line.getOptionValue("R");
        sites = line.getOptionValue("D");
        hdfsSites = sites.split(",");
        if(line.hasOption("tmp"))
            tmpDir = line.getOptionValue("tmp");
        
        if(line.hasOption("nodes"))
            nodes = Integer.parseInt(line.getOptionValue("nodes"));
        if(line.hasOption("vcores"))
            vcores = Integer.parseInt(line.getOptionValue("vcores"));
        if(line.hasOption("mem"))
            mem = Integer.parseInt(line.getOptionValue("mem"));
        /*
        if(line.hasOption("mc")) {
            mappers = Integer.parseInt(line.getOptionValue("mc"));
            reducers = mappers;
        }
        if(line.hasOption("rc"))
            reducers = Integer.parseInt(line.getOptionValue("rc"));
        if(line.hasOption("mt")) {
            mthreads = Integer.parseInt(line.getOptionValue("mt"));
            rthreads = mthreads;
        }
        if(line.hasOption("rt"))
            rthreads = Integer.parseInt(line.getOptionValue("rt"));
        */
        
        if(line.hasOption("B")){
            halvadeDir = line.getOptionValue("B");
        }
        if(line.hasOption("scc"))
            stand_call_conf = Integer.parseInt(line.getOptionValue("scc"));
        if(line.hasOption("sec"))
            stand_emit_conf = Integer.parseInt(line.getOptionValue("sec"));
        if(line.hasOption("keep"))
            keepFiles = true;
        if(line.hasOption("s"))
            paired = false;
        if(line.hasOption("justalign"))
            justAlign = true;
        if(line.hasOption("rjvm"))
            reuseJVM = true;
        if(line.hasOption("bwamem"))
            aln = false;
        if(line.hasOption("r"))
            regionSize = Integer.parseInt(line.getOptionValue("r"));
        if(line.hasOption("J"))
            java = line.getOptionValue("J");
        if(line.hasOption("M"))
            manifest = line.getOptionValue("M");
        if(line.hasOption("p"))
            justPut = true;
        if(line.hasOption("exome")) {
            exomeBedFile = line.getOptionValue("exome");
            coverage = 30;
        }
        if(line.hasOption("cov"))
            coverage = Integer.parseInt(line.getOptionValue("cov"));
        if(line.hasOption("c"))
            justCombine = true;
        if(line.hasOption("b"))
            useBedTools = true;
        if(line.hasOption("hc"))
            useGenotyper = false;
        if(line.hasOption("P"))
            useIPrep = false;
        if(line.hasOption("id"))
            RGID = line.getOptionValue("id");
        if(line.hasOption("lb"))
            RGLB = line.getOptionValue("lb");
        if(line.hasOption("pl"))
            RGPL = line.getOptionValue("pl");
        if(line.hasOption("pu"))
            RGPU = line.getOptionValue("pu");
        if(line.hasOption("sm"))
            RGSM = line.getOptionValue("sm");
        if(line.hasOption("chr"))
            chr = line.getOptionValue("chr");
    }
    
    private void setKeysPerChromosome() {
        int maxChrLength = dict.getSequence(0).getSequenceLength();
        int minRegions = 0;
        if(chr == null) {
            for(int i = 0; i < dict.size(); i++) 
                if(dict.getSequence(i).getSequenceLength() > maxChrLength)
                    maxChrLength = dict.getSequence(i).getSequenceLength();
            minChrLength = maxChrLength;
            for(int i = 0; i < dict.size(); i++) 
                if(dict.getSequence(i).getSequenceLength() < minChrLength &&
                        (100.0*dict.getSequence(i).getSequenceLength() / maxChrLength) > 25.0)
                    minChrLength = dict.getSequence(i).getSequenceLength();
            for(int i = 0; i < dict.size(); i++) 
                minRegions += (int)Math.ceil((double)dict.getSequence(i).getSequenceLength() / minChrLength);
        } else {
            String[] chrs = chr.split(",");
            for(String chr_ : chrs)
                if(dict.getSequence(chr_).getSequenceLength() > maxChrLength)
                    maxChrLength = dict.getSequence(chr_).getSequenceLength();
            minChrLength = maxChrLength;
            for(String chr_ : chrs)
                if(dict.getSequence(chr_).getSequenceLength() < minChrLength &&
                        (100.0*dict.getSequence(chr_).getSequenceLength() / maxChrLength) > 25.0)
                    minChrLength = dict.getSequence(chr_).getSequenceLength();
            for(String chr_ : chrs)
                minRegions += (int)Math.ceil((double)dict.getSequence(chr_).getSequenceLength() / minChrLength);
        }
        multiplier = 1;
        while(multiplier * minRegions < reducers) 
            multiplier++;
    }
        
    protected boolean removeLocalFile(String filename) {
        File f = new File(filename);
        return f.exists() && f.delete();
    } 
    
    private int getNumberOfRegions(Configuration conf) {
        // use keysPerChromosome
        int regions = 0;
        if(chr == null) {
            for(int i = 0; i < dict.size(); i++)  {
                int count;
                if (dict.getSequence(i).getSequenceLength() < minChrLength / multiplier)
                    count = 1;
                else
                   count = multiplier*(int)Math.ceil((double)dict.getSequence(i).getSequenceLength() / minChrLength);
                if(count > 0) Logger.DEBUG(dict.getSequence(i).getSequenceName() + ": " + count + 
                        " regions [" + (dict.getSequence(i).getSequenceLength() / count + 1) + "].");
                regions += count;
            }
        } else {
            String[] chrs = chr.split(",");
            for(String chr_ : chrs){
                int count;
                if (dict.getSequence(chr_).getSequenceLength() < minChrLength / multiplier)
                    count = 1;
                else
                    count = 
                        multiplier*(int)Math.ceil((double)dict.getSequence(chr_).getSequenceLength() / minChrLength);
                if(count > 0) Logger.DEBUG(dict.getSequence(chr_).getSequenceName() + ": " + count + " regions.");
                regions += count;
            }
        }
        Logger.DEBUG("found " + regions + " regions.");
        // set random shuffled regions
        reducers = regions;
        MyConf.setReducers(conf, reducers);
        return regions;
    }
    
    
    private static final int MEM_MAP_TASK = 8; // minimum requirement map task
    private static final int MEM_REDUCE_TASK = 14; // minimum requirement reduce task
    private static final int VCORES_MAP_TASK = 8; // set a minimum of cores so it take too many tasks per node
    private static final int OS_REQ = 2; // minimum requirement for OS
    private static final int SWAP_EXTRA = 20;
    
    private void getBestDistribution(Configuration conf) {
        mem = mem - OS_REQ;
        int memMapLimit = Math.max(mem / MEM_MAP_TASK,1);
        int vcoresMapLimit = Math.max(vcores / VCORES_MAP_TASK,1);
        if (vcoresMapLimit < memMapLimit)
            mapsPerContainer = vcoresMapLimit;
        else
            mapsPerContainer = memMapLimit;  
        
        int memReduceLimit = Math.max(mem / MEM_REDUCE_TASK,1);
        if(memReduceLimit > vcores)
            reducersPerContainer = vcores;
        else
            reducersPerContainer = memReduceLimit;
        
        mthreads = Math.max(1,vcores/mapsPerContainer);
        mappers = Math.max(1,nodes*mapsPerContainer);
        GATKCPUThreads = Math.max(1,vcores/reducersPerContainer);
        GATKdataThreads = Math.max(1,vcores/reducersPerContainer);
        Logger.DEBUG("using " + mapsPerContainer + " maps [" 
                + mthreads + " cpu , " + Math.min(mem*1024,mem*1024/mapsPerContainer) +
                " mb] per node and " + reducersPerContainer + " reducers ["
                + GATKCPUThreads + " cpu, " + Math.min(mem*1024,((SWAP_EXTRA + mem)*1024/reducersPerContainer)) +
                " mb] per node");
        conf.set("mapreduce.map.cpu.vcores", ""+mthreads);
        conf.set("mapreduce.map.memory.mb", ""+ Math.min(mem*1024,mem*1024/mapsPerContainer)); 
//        conf.set("mapreduce.map.memory.mb", ""+((SWAP_EXTRA + mem)*1024/mapsPerContainer)); 
        conf.set("mapreduce.reduce.cpu.vcores", ""+GATKCPUThreads);
        conf.set("mapreduce.reduce.memory.mb", ""+Math.min(mem*1024,((SWAP_EXTRA + mem)*1024/reducersPerContainer))); 
//        conf.set("mapreduce.reduce.memory.mb", ""+(mem*1024/reducersPerContainer)); 
        conf.set("mapreduce.job.reduce.slowstart.completedmaps", ""+1.0);
        
        // experimental - need more data
        reducers = (int) (coverage * 6.40 * reducersPerContainer);
        
    }

    private void parseANNFile(Configuration conf) {
        Logger.DEBUG("parsing ANN file...");
        try {
            // seq data is stored in ${ref}.ann file
            FileSystem fs = FileSystem.get(new URI(ref + ".ann"), conf);
            FSDataInputStream stream = fs.open(new Path(ref + ".ann"));
            String line = getLine(stream);
            // skip first line, has 3 numbers: length nseq seed 
            dict = new SAMSequenceDictionary();
            while(line != null) {
                // read first line of new sequence: gi (number) name(string) (either to end of file or a space)
                line = getLine(stream);
                if(line != null) {
                    // extract data: 
                    String seqName = line.substring(line.indexOf(' ') + 1);
                    int nextidx = seqName.indexOf(' ');
                    if(nextidx != -1) seqName = seqName.substring(0, nextidx);
                    // read next line:
                    line = getLine(stream);
                    if (line != null) {
                        int idx1 = line.indexOf(' ') + 1;
                        int idx2 = line.indexOf(' ', idx1); 
                        int seqLength = 0;
                        try {
                            seqLength = Integer.parseInt(line.substring(idx1, idx2));
                        } catch(NumberFormatException ex) {
                            Logger.EXCEPTION(ex);
                        }
                        SAMSequenceRecord seq = new SAMSequenceRecord(seqName, seqLength);
//                        Logger.DEBUG("name: " + seq.getSequenceName() + " length: " + 
//                                seq.getSequenceLength());
                        dict.addSequence(seq);  
                    }                  
                }
            }
            MyConf.setSequenceDictionary(conf, dict);
        } catch (URISyntaxException ex) {
            Logger.EXCEPTION(ex);
        } catch (IOException ex) {
            Logger.EXCEPTION(ex);
        }
        
    }
    
    private String getLine(FSDataInputStream stream) throws IOException {
        String tmp = "";
        try {
            char c = (char)stream.readByte();
            while(c != '\n') {
                tmp = tmp + c;
                c = (char)stream.readByte();
            }
            return tmp;  
        } catch (EOFException ex) {
            // reached end of file, return null;
            return null;
        }
    }
}
