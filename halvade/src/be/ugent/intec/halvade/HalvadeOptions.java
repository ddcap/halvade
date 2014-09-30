/*
 * Copyright (C) 2014 ddecap
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package be.ugent.intec.halvade;

import be.ugent.intec.halvade.utils.Logger;
import be.ugent.intec.halvade.utils.MyConf;
import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import net.sf.samtools.SAMSequenceDictionary;
import net.sf.samtools.SAMSequenceRecord;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 *
 * @author ddecap
 */
public class HalvadeOptions {
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
    protected int stand_call_conf = -1;
    protected int stand_emit_conf = -1;
    protected int MAP_M = 10; // # files per mapper as input!
    protected int RED_M = 4; // # keys per reducer
    protected SAMSequenceDictionary dict;
    protected String chr = null;
    protected int multiplier;
    protected int minChrLength;
    protected int reducersPerContainer = -1;
    protected int mapsPerContainer = -1;
    protected boolean reuseJVM = false;
    protected boolean justAlign = false;
    protected String exomeBedFile = null;
    protected int coverage = 50; 
    protected String halvadeDir = "/halvade/";
    protected String bin;
    protected boolean combineVcf = true;
    protected boolean dryRun = false;
    // custom args!    
    protected String ca_bwa_aln = null;
    protected String ca_bwa_mem = null;
    protected String ca_bwa_samxe = null;
    protected String ca_elprep = null;
    protected String ca_samtools_view = null;
    protected String ca_bedtools_dbsnp = null;
    protected String ca_bedtools_exome = null;
    protected String ca_picard_bai = null;
    protected String ca_picard_rg = null;
    protected String ca_picard_dedup = null;
    protected String ca_picard_clean = null;
    protected String ca_gatk_rtc = null;
    protected String ca_gatk_ir = null;
    protected String ca_gatk_br = null;
    protected String ca_gatk_pr = null;
    protected String ca_gatk_cv = null;
    protected String ca_gatk_vc = null; 
    
    public int GetOptions(String[] args, Configuration halvadeConf) throws IOException, URISyntaxException {
        try {
            parseArguments(args);
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
//            MyConf.setBinDir(halvadeConf, bin);
            MyConf.setFastqEncoding(halvadeConf, FASTQ_ENCODING[0]);
            MyConf.setOutDir(halvadeConf, out);
            MyConf.setKeepFiles(halvadeConf, keepFiles);
            MyConf.setUseBedTools(halvadeConf, useBedTools);
            MyConf.clearTaskFiles(halvadeConf);
            MyConf.setUseIPrep(halvadeConf, useIPrep);
            MyConf.setUseUnifiedGenotyper(halvadeConf, useGenotyper);
            MyConf.setReuseJVM(halvadeConf, reuseJVM);
            MyConf.setReadGroup(halvadeConf, "ID:" + RGID + " LB:" + RGLB + " PL:" + RGPL + " PU:" + RGPU + " SM:" + RGSM);  
            // check for custom arguments for all tools
            if(ca_bwa_aln != null) MyConf.setBwaAlnArgs(halvadeConf, ca_bwa_aln);
            if(ca_bwa_mem != null) MyConf.setBwaMemArgs(halvadeConf, ca_bwa_mem);
            if(ca_bwa_samxe != null) MyConf.setBwaSamxeArgs(halvadeConf, ca_bwa_samxe);
            if(ca_elprep != null) MyConf.setElPrepArgs(halvadeConf, ca_elprep);
            if(ca_samtools_view != null) MyConf.setSamtoolsViewArgs(halvadeConf, ca_samtools_view);
            if(ca_bedtools_dbsnp != null) MyConf.setBedToolsDbSnpArgs(halvadeConf, ca_bedtools_dbsnp);
            if(ca_bedtools_exome != null) MyConf.setBedToolsExomeArgs(halvadeConf, ca_bedtools_exome);
            if(ca_picard_bai != null) MyConf.setPicardBaiArgs(halvadeConf, ca_picard_bai);
            if(ca_picard_rg != null) MyConf.setPicardAddReadGroupArgs(halvadeConf, ca_picard_rg);
            if(ca_picard_dedup != null) MyConf.setPicardMarkDupArgs(halvadeConf, ca_picard_dedup);
            if(ca_picard_clean != null) MyConf.setPicardCleanSamArgs(halvadeConf, ca_picard_clean);
            if(ca_gatk_rtc != null) MyConf.setGatkRealignerTargetCreatorArgs(halvadeConf, ca_gatk_rtc);
            if(ca_gatk_ir != null) MyConf.setGatkIndelRealignerArgs(halvadeConf, ca_gatk_ir);
            if(ca_gatk_br != null) MyConf.setGatkBaseRecalibratorArgs(halvadeConf, ca_gatk_br);
            if(ca_gatk_pr != null) MyConf.setGatkPrintReadsArgs(halvadeConf, ca_gatk_pr);
            if(ca_gatk_cv != null) MyConf.setGatkCombineVariantsArgs(halvadeConf, ca_gatk_cv);
            if(ca_gatk_vc != null) MyConf.setGatkVariantCallerArgs(halvadeConf, ca_gatk_vc);
            
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
                       
            if(!halvadeDir.endsWith("/"))
                halvadeDir += "/";
            
        } catch (ParseException e) {
            // automatically generate the help statement
            System.err.println("Error parsing: " + e.getMessage());
            HelpFormatter formatter = new HelpFormatter();
            formatter.setWidth(80);
            formatter.printHelp( "hadoop jar HalvadeWithLibs.jar -I <IN> -O <OUT> " +
                    "-R <REF> -D <SITES> -B <BIN> -nodes <nodes> -mem <mem> -vcores <cores> [options]", options);
            return 1;
        }
        return 0;
    }
    
    private String[] getChromosomeNames(SAMSequenceDictionary dict) {
        String[] chrs = new String[dict.size()];
        for(int i = 0; i < dict.size(); i++) 
            chrs[i] = dict.getSequence(i).getSequenceName();
        return chrs;
    }
    
    private void setKeysPerChromosome() {
        int maxChrLength = dict.getSequence(0).getSequenceLength();
        int minRegions = 0;
        String[] chrs;
        if(chr == null) 
            chrs = getChromosomeNames(dict);
        else
            chrs = chr.split(",");
        
        for(String chr_ : chrs)
            if(dict.getSequence(chr_).getSequenceLength() > maxChrLength)
                maxChrLength = dict.getSequence(chr_).getSequenceLength();
        minChrLength = maxChrLength;
        for(String chr_ : chrs)
            if(dict.getSequence(chr_).getSequenceLength() < minChrLength &&
                    (100.0*dict.getSequence(chr_).getSequenceLength() / maxChrLength) > 25.0)
                minChrLength = dict.getSequence(chr_).getSequenceLength();     
        
        for(String chr_ : chrs)
            if(dict.getSequence(chr_).getSequenceLength() > minChrLength)
                minRegions += (int)Math.ceil((double)dict.getSequence(chr_).getSequenceLength() / minChrLength);
        double restChr = 0;
        for(String chr_ : chrs)
            if(dict.getSequence(chr_).getSequenceLength() < minChrLength)
                restChr += (double)dict.getSequence(chr_).getSequenceLength() / minChrLength;
        minRegions += (int)Math.ceil(restChr / 1.0);
        multiplier = 1;
        while(multiplier * minRegions < reducers) 
            multiplier++;
        minChrLength = minChrLength / multiplier;
    }
        
    protected boolean removeLocalFile(String filename) {
        File f = new File(filename);
        return f.exists() && f.delete();
    } 
    
    private int getNumberOfRegions(Configuration conf) {
        // use keysPerChromosome
        int regions = 0;
        String[] chrs;
        if(chr == null) 
            chrs = getChromosomeNames(dict);
        else
            chrs = chr.split(",");
        
        for(String chr_ : chrs) {
            if(dict.getSequence(chr_).getSequenceLength() > minChrLength) {
                int count =  (int)Math.ceil((double)dict.getSequence(chr_).getSequenceLength() / minChrLength);
                Logger.DEBUG2(dict.getSequence(chr_).getSequenceName() + ": " + count + 
                    " regions [" + (dict.getSequence(chr_).getSequenceLength() / count + 1) + "].");
                regions += count;
            }
        }
        double restChr = 0;
        for(String chr_ : chrs) {
            if(dict.getSequence(chr_).getSequenceLength() < minChrLength) {
                restChr += (double)dict.getSequence(chr_).getSequenceLength() / minChrLength;
//                Logger.DEBUG("shared chromosome: " + dict.getSequence(chr_).getSequenceName() 
//                        + " [" + dict.getSequence(chr_).getSequenceLength() + "].");
            }
        }
        Logger.DEBUG("Regions with collection of chromosomes: " + (int)Math.ceil(restChr / 1.0));
        regions += (int)Math.ceil(restChr / 1.0);
        be.ugent.intec.halvade.utils.Logger.DEBUG("Total regions: " + regions);
        // set random shuffled regions
        reducers = regions;
        MyConf.setReducers(conf, reducers);
        
        return regions;
    }
    
    
    private static final int MEM_MAP_TASK = 15;
    private static final int MEM_REDUCE_TASK = 15;
    private static final int VCORES_MAP_TASK = 8;
    private static final int VCORES_REDUCE_TASK = 8;
    private static final int SWAP_EXTRA = 20;
    
    private void getBestDistribution(Configuration conf) {
        if (mapsPerContainer == -1) mapsPerContainer = Math.min(Math.max(vcores / VCORES_MAP_TASK,1), Math.max(mem / MEM_MAP_TASK,1));
        if (reducersPerContainer == -1) reducersPerContainer = Math.min(Math.max(vcores / VCORES_REDUCE_TASK, 1), Math.max(mem / MEM_REDUCE_TASK,1));
        
        mappers = Math.max(1,nodes*mapsPerContainer);        
        mthreads = Math.max(1,vcores/mapsPerContainer);
        GATKCPUThreads = Math.max(1,vcores/reducersPerContainer);
        GATKdataThreads = Math.max(1,vcores/reducersPerContainer);
        int mmem = Math.min(mem*1024,mem*1024/mapsPerContainer);
        int rmem = Math.min(mem*1024,((SWAP_EXTRA + mem)*1024/reducersPerContainer));
        
        be.ugent.intec.halvade.utils.Logger.DEBUG("using " + mapsPerContainer + " maps [" 
                + mthreads + " cpu , " + mmem +
                " mb] per node and " + reducersPerContainer + " reducers ["
                + GATKCPUThreads + " cpu, " + rmem +
                " mb] per node");
        conf.set("mapreduce.map.cpu.vcores", "" + mthreads);
        conf.set("mapreduce.map.memory.mb", "" + mmem); 
        conf.set("mapreduce.reduce.cpu.vcores", "" + GATKCPUThreads);
        conf.set("mapreduce.reduce.memory.mb", "" + rmem); 
        conf.set("mapreduce.job.reduce.slowstart.completedmaps", "" + 1.0);
        
        // experimental - need more data
        reducers = (int) (coverage * 6.40 * reducersPerContainer);
        
    }

    private void parseANNFile(Configuration conf) {
        be.ugent.intec.halvade.utils.Logger.DEBUG("parsing ANN file...");
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
                            be.ugent.intec.halvade.utils.Logger.EXCEPTION(ex);
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
            be.ugent.intec.halvade.utils.Logger.EXCEPTION(ex);
        } catch (IOException ex) {
            be.ugent.intec.halvade.utils.Logger.EXCEPTION(ex);
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
    
    protected void createOptions() {    //setup options
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
        Option optScc = OptionBuilder.withArgName( "scc" )
                                .hasArg()
                                .withDescription(  "Sets stand_call_conf for gatk Variant Caller." )
                                .create( "scc" );
        Option optSec = OptionBuilder.withArgName( "sec" )
                                .hasArg()
                                .withDescription(  "Sets stand_emit_conf for gatk Variant Caller." )
                                .create( "sec" );
        Option optChr = OptionBuilder.withArgName( "chr1,chr2,..." )
                                .hasArg()
                                .withDescription(  "Sets the chromosomes if reads don't cover the full reference (chrM,chr2,...)." +
                                        "This only changes how the regions will be distributed not the reference.")
                                .create( "chr" );
        Option optEx = OptionBuilder.withArgName( "bed file" )
                                .hasArg()
                                .withDescription(  "Gives the location of a bed file for exome target regions. Required for exome sequences. ")
                                .create( "exome" );
        
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
        
        Option optMpn = OptionBuilder.withArgName( "tasks" )
                                .hasArg()
                                .withDescription(  "Overrides the number of map tasks running simultaneously on each node. ")
                                .create( "mpn" );
        Option optRpn = OptionBuilder.withArgName( "tasks" )
                                .hasArg()
                                .withDescription(  "Overrides the number of reduce tasks running simultaneously on each node. ")
                                .create( "rpn" );
        // custom arguments
        Option optCABwaAln = OptionBuilder.withArgName( "args" )
                                .hasArg()
                                .withDescription(  "A string containing all additional arguments, white space sepparated, for the Bwa aln command. ")
                                .create( "ca_bwa_aln" );
        Option optCABwaMem = OptionBuilder.withArgName( "args" )
                                .hasArg()
                                .withDescription(  "A string containing all additional arguments, white space sepparated, for the Bwa mem command. ")
                                .create( "ca_bwa_mem" );
        Option optCABwaSampe = OptionBuilder.withArgName( "args" )
                                .hasArg()
                                .withDescription(  "A string containing all additional arguments, white space sepparated, for the Bwa sampe/samse command. ")
                                .create( "ca_bwa_sampe" );
        Option optCAElprep = OptionBuilder.withArgName( "args" )
                                .hasArg()
                                .withDescription(  "A string containing all additional arguments, white space sepparated, for the Elprep command. Will not be used if Picard is used for data preparation ")
                                .create( "ca_elprep" );
        Option optCASamtools = OptionBuilder.withArgName( "args" )
                                .hasArg()
                                .withDescription(  "A string containing all additional arguments, white space sepparated, for the Samtools view command.")
                                .create( "ca_samtools" );
        Option optCABedtoolsDb = OptionBuilder.withArgName( "args" )
                                .hasArg()
                                .withDescription(  "A string containing all additional arguments, white space sepparated, for the Bedtools command applied to dbSnp.")
                                .create( "ca_bed_dbsnp" );
        Option optCABedtoolsEx = OptionBuilder.withArgName( "args" )
                                .hasArg()
                                .withDescription(  "A string containing all additional arguments, white space sepparated, for the Bedtools command applied to exome Bed file.")
                                .create( "ca_bed_exome" );
        Option optCAPicardBai = OptionBuilder.withArgName( "args" )
                                .hasArg()
                                .withDescription(  "A string containing all additional arguments, white space sepparated, for the Picard BuildBamIndex command.")
                                .create( "ca_picard_bai" );
        Option optCAPicardRg = OptionBuilder.withArgName( "args" )
                                .hasArg()
                                .withDescription(  "A string containing all additional arguments, white space sepparated, for the Picard AddOrReplaceReadGroups command.")
                                .create( "ca_picard_rg" );
        Option optCAPicardDedup = OptionBuilder.withArgName( "args" )
                                .hasArg()
                                .withDescription(  "A string containing all additional arguments, white space sepparated, for the Picard MarkDuplicates command.")
                                .create( "ca_picard_dedup" );
        Option optCAPicardClean = OptionBuilder.withArgName( "args" )
                                .hasArg()
                                .withDescription(  "A string containing all additional arguments, white space sepparated, for the Picard CleanSam command.")
                                .create( "ca_picard_clean" );
        Option optCAGatkRtc = OptionBuilder.withArgName( "args" )
                                .hasArg()
                                .withDescription(  "A string containing all additional arguments, white space sepparated, for the GATK RealignerTargetCreator command.")
                                .create( "ca_gatk_rtc" );
        Option optCAGatkIr = OptionBuilder.withArgName( "args" )
                                .hasArg()
                                .withDescription(  "A string containing all additional arguments, white space sepparated, for the GATK IndelRealigner command.")
                                .create( "ca_gatk_ir" );
        Option optCAGatkBr = OptionBuilder.withArgName( "args" )
                                .hasArg()
                                .withDescription(  "A string containing all additional arguments, white space sepparated, for the GATK BaseRecalibrator command.")
                                .create( "ca_gatk_br" );
        Option optCAGatkPr = OptionBuilder.withArgName( "args" )
                                .hasArg()
                                .withDescription(  "A string containing all additional arguments, white space sepparated, for the GATK PrintReads command.")
                                .create( "ca_gatk_pr" );
        Option optCAGatkCv = OptionBuilder.withArgName( "args" )
                                .hasArg()
                                .withDescription(  "A string containing all additional arguments, white space sepparated, for the GATK CombineVariants command.")
                                .create( "ca_gatk_cv" );
        Option optCAGatkVc = OptionBuilder.withArgName( "args" )
                                .hasArg()
                                .withDescription(  "A string containing all additional arguments, white space sepparated, for the GATK HaplotypeCaller or UnifiedGenotyper command, depending on which is used.")
                                .create( "ca_gatk_vc" );
        
        Option optDry = OptionBuilder.withArgName( "dryrun" )
                                .withDescription(  "execute a dryrun, will calculate task size, split for regions etc, but not execute the MapReduce job.")
                                .create( "dryrun" );
        
        
        options.addOption(optIn);
        options.addOption(optOut);
        options.addOption(optRef);
        options.addOption(optSites);
        options.addOption(optBin);
        options.addOption(optTmp);
        options.addOption(optSingle);
        options.addOption(optBmem);
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
        options.addOption(optMpn);
        options.addOption(optRpn);
        options.addOption(optDry);
        
        // custom arguments
        options.addOption(optCABwaAln);
        options.addOption(optCABwaMem);
        options.addOption(optCABwaSampe);
        options.addOption(optCAElprep);
        options.addOption(optCASamtools);
        options.addOption(optCABedtoolsDb);
        options.addOption(optCABedtoolsEx);
        options.addOption(optCAPicardBai);
        options.addOption(optCAPicardRg);
        options.addOption(optCAPicardDedup);
        options.addOption(optCAPicardClean);
        options.addOption(optCAGatkRtc);
        options.addOption(optCAGatkIr);
        options.addOption(optCAGatkBr);
        options.addOption(optCAGatkPr);
        options.addOption(optCAGatkCv);
        options.addOption(optCAGatkVc);
    }
    
    protected void parseArguments(String[] args) throws ParseException {
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
        if(line.hasOption("mpn"))
            mapsPerContainer = Integer.parseInt(line.getOptionValue("mpn"));
        if(line.hasOption("rpn"))
            mapsPerContainer = Integer.parseInt(line.getOptionValue("rpn"));
        
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
        if(line.hasOption("dryrun"))
            dryRun = true;
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
        
        // add custom arguments
        if(line.hasOption("ca_bwa_aln"))
            ca_bwa_aln = line.getOptionValue("ca_bwa_aln");
        if(line.hasOption("ca_bwa_mem"))
            ca_bwa_mem = line.getOptionValue("ca_bwa_mem");
        if(line.hasOption("ca_bwa_sampe"))
            ca_bwa_samxe = line.getOptionValue("ca_bwa_sampe");
        if(line.hasOption("ca_elprep"))
            ca_elprep = line.getOptionValue("ca_elprep");
        if(line.hasOption("ca_samtools_view"))
            ca_samtools_view = line.getOptionValue("ca_samtools");
        if(line.hasOption("ca_bedtools_dbsnp"))
            ca_bedtools_dbsnp = line.getOptionValue("ca_bed_dbsnp");
        if(line.hasOption("ca_bedtools_exome"))
            ca_bedtools_exome = line.getOptionValue("ca_bed_exome");
        if(line.hasOption("ca_picard_bai"))
            ca_picard_bai = line.getOptionValue("ca_picard_bai");
        if(line.hasOption("ca_picard_rg"))
            ca_picard_rg = line.getOptionValue("ca_picard_rg");
        if(line.hasOption("ca_picard_dedup"))
            ca_picard_dedup = line.getOptionValue("ca_picard_dedup");
        if(line.hasOption("ca_picard_clean"))
            ca_picard_clean = line.getOptionValue("ca_picard_clean");
        if(line.hasOption("ca_gatk_rtc"))
            ca_gatk_rtc = line.getOptionValue("ca_gatk_rtc");
        if(line.hasOption("ca_gatk_ir"))
            ca_gatk_ir = line.getOptionValue("ca_gatk_ir");
        if(line.hasOption("ca_gatk_br"))
            ca_gatk_br = line.getOptionValue("ca_gatk_br");
        if(line.hasOption("ca_gatk_pr"))
            ca_gatk_pr = line.getOptionValue("ca_gatk_pr");
        if(line.hasOption("ca_gatk_cv"))
            ca_gatk_cv = line.getOptionValue("ca_gatk_cv");
        if(line.hasOption("ca_gatk_vc"))
            ca_gatk_vc = line.getOptionValue("ca_gatk_vc");
    }
}
