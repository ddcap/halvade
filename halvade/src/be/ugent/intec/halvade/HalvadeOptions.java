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

import be.ugent.intec.halvade.utils.ChromosomeSplitter;
import be.ugent.intec.halvade.utils.Logger;
import be.ugent.intec.halvade.utils.HalvadeConf;
import java.io.EOFException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.DecimalFormat;
import java.util.Enumeration;
import java.util.Properties;
import htsjdk.samtools.SAMSequenceDictionary;
import htsjdk.samtools.SAMSequenceRecord;
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
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author ddecap
 */
public class HalvadeOptions {

    public Options options = new Options();
    public String in;
    public String out;
    public String ref;
    public String STARGenome = null;
    public String java = null;
    public String gff = null;
    public String tmpDir = "/tmp/halvade/";
    public String localRefDir = null;
    public String sites;
    public int nodes, vcores;
    public double mem;
    public int maps = 1, reduces = 1, parallel_reducers = 1, mthreads = 1, rthreads = 1;
    public String[] hdfsSites;
    public boolean paired = true;
    public int aln = 0;
    public Class<? extends Mapper>[] alignmentTools = new Class[]{
        be.ugent.intec.halvade.hadoop.mapreduce.BWAAlnMapper.class,
        be.ugent.intec.halvade.hadoop.mapreduce.BWAMemMapper.class,
        be.ugent.intec.halvade.hadoop.mapreduce.Bowtie2Mapper.class,
        be.ugent.intec.halvade.hadoop.mapreduce.Cushaw2Mapper.class
    };
    public boolean justCombine = false;
    public boolean filterDBSnp = false;
    public boolean useGenotyper = true;
    public String RGID = "GROUP1";
    public String RGLB = "LIB1";
    public String RGPL = "ILLUMINA";
    public String RGPU = "UNIT1";
    public String RGSM = "SAMPLE1";
    public boolean useElPrep = true;
    public boolean keepFiles = false;
    public int stand_call_conf = -1;
    public int stand_emit_conf = -1;
    public SAMSequenceDictionary dict;
    public String chr = null;
    public int reducerContainersPerNode = -1;
    public int mapContainersPerNode = -1;
    public boolean justAlign = false;;
    public boolean mergeBam = false;
    public String bedFile = null;
    public String filterBed = null;
    public String bedRegion = null;
//    public double coverage = -1.0;
    public String halvadeBinaries;
    public String bin;
    public String readCountsPerRegionFile = null;
    public boolean combineVcf = true;
    public boolean dryRun = false;
    public boolean keepChrSplitPairs = true;
    public boolean startupJob = true;
    public boolean rnaPipeline = false;
    public boolean reportAll = false;
    public boolean useBamInput = false;
    public boolean setMapContainers = true, setReduceContainers = true;
    public boolean redistribute = false;
    public boolean smtEnabled = false;
    public boolean reorderRegions = false;
    public boolean updateRG = false;
    public int overrideMapMem = -1;
    public int overrideRedMem = -1;
    public boolean countOnly = false;
    public boolean keepDups = true;
    public boolean fixQualEnc = false;
    public String stargtf = null;
    
    protected DecimalFormat onedec;
    protected static final double REDUCE_TASKS_FACTOR = 1.68 * 15;
    protected static final double DEFAULT_COVERAGE = 50;
    protected static final double DEFAULT_COVERAGE_SIZE = 86;
    protected static final String DICT_SUFFIX = ".dict";

    public int GetOptions(String[] args, Configuration hConf) throws IOException, URISyntaxException {
        try {
            boolean result = parseArguments(args, hConf);
            if (!result) {
                HelpFormatter formatter = new HelpFormatter();
                formatter.setWidth(80);
                formatter.printHelp("hadoop jar HalvadeWithLibs.jar -I <IN> -O <OUT> "
                        + "-R <REF> -D <SITES> -B <BIN> -N <nodes> -M <mem> -C <cores> [options]", options);
                return 1;
            }
            onedec = new DecimalFormat("###0.0");
            // add parameters to configuration:
            HalvadeConf.setRefDirIsSet(hConf, localRefDir != null);
            if (localRefDir == null) {
                localRefDir = tmpDir;
            }
            HalvadeConf.setScratchTempDir(hConf, tmpDir);
            HalvadeConf.setRefDirOnScratch(hConf, localRefDir);
            HalvadeConf.setRefOnHDFS(hConf, ref);
            if (STARGenome != null) {
                HalvadeConf.setStarDirOnHDFS(hConf, STARGenome);
            }
            HalvadeConf.setKnownSitesOnHDFS(hConf, hdfsSites);
            HalvadeConf.setIsPaired(hConf, paired);
            HalvadeConf.setIsRNA(hConf, rnaPipeline);
            if (bedFile != null) {            
                HalvadeConf.setBed(hConf, bedFile);
            }
            if (filterBed != null) {            
                HalvadeConf.setFilterBed(hConf, filterBed);
            }
            HalvadeConf.setInputIsBam(hConf, useBamInput);
            HalvadeConf.setFixQualEnc(hConf, fixQualEnc);
            HalvadeConf.setOutDir(hConf, out);
            HalvadeConf.setKeepFiles(hConf, keepFiles);
            HalvadeConf.setFilterDBSnp(hConf, filterDBSnp);
            HalvadeConf.clearTaskFiles(hConf);
            HalvadeConf.setUseElPrep(hConf, useElPrep);
            HalvadeConf.setUpdateReadGroup(hConf, updateRG);
            HalvadeConf.setUseUnifiedGenotyper(hConf, useGenotyper);
            HalvadeConf.setMergeBam(hConf, mergeBam);
            HalvadeConf.setKeepDups(hConf, keepDups);
            HalvadeConf.setRedistribute(hConf, redistribute);
            HalvadeConf.setReadGroup(hConf, "ID:" + RGID + " LB:" + RGLB + " PL:" + RGPL + " PU:" + RGPU + " SM:" + RGSM);
            HalvadeConf.setkeepChrSplitPairs(hConf, keepChrSplitPairs);
            if (STARGenome != null) {
                HalvadeConf.setStarDirPass2HDFS(hConf, out);
            }
            if (stargtf != null) { 
                HalvadeConf.setStarGtf(hConf, stargtf);
            }

            if (chr != null) {
                HalvadeConf.setChrList(hConf, chr);
            }
            if (java != null) {
                HalvadeConf.setJava(hConf, java);
            }
            if (gff != null) {
                HalvadeConf.setGff(hConf, gff);
            }

            if (stand_call_conf > 0) {
                HalvadeConf.setSCC(hConf, stand_call_conf);
            }
            if (stand_emit_conf > 0) {
                HalvadeConf.setSEC(hConf, stand_emit_conf);
            }


        } catch (ParseException e) {
            Logger.DEBUG(e.getMessage());
            HelpFormatter formatter = new HelpFormatter();
            formatter.setWidth(80);
            formatter.printHelp("hadoop jar HalvadeWithLibs.jar -I <input> -O <output> "
                    + "-R <ref> -D <dbsnp> -B <bin> -nodes <nodes> -mem <mem> -vcores <cores> [options]", options);
            return 1;
        }
        return 0;
    }
    
    protected void splitChromosomes(Configuration hConf, int nReduces) throws URISyntaxException, IOException {        
        parseDictFile(hConf);
//        double inputSize = getInputSize(in, hConf);
//        if (coverage == -1.0) {
//            coverage = Math.max(1.0, DEFAULT_COVERAGE * (inputSize / DEFAULT_COVERAGE_SIZE));
//        }
//        Logger.DEBUG("Estimated coverage: " + roundOneDecimal(coverage));
        // set a minimum first where the real amount is based on
//            reduces = (int) (coverage * REDUCE_TASKS_FACTOR);
        if(nReduces == 0 )
            reduces = (int) (1.75*nodes*reducerContainersPerNode);
        else
            reduces = nReduces;
        if(reduces < 26) reduces = 26; // set a minimum of 26 reduces so at least all big chromosomes have one reduce task.
        int tmpReduces = reduces + 1;
        Logger.DEBUG("requested # reducers: " + reduces);
        double factor = 0.95;
        int factoredReduces = reduces;
        ChromosomeSplitter splitter = null;
        while(tmpReduces > reduces) {
            if(bedFile != null)
                splitter = new ChromosomeSplitter(dict, bedFile, factoredReduces);
            else if (readCountsPerRegionFile != null)
                splitter = new ChromosomeSplitter(dict, readCountsPerRegionFile, factoredReduces, reorderRegions);
            else
                splitter = new ChromosomeSplitter(dict, factoredReduces);
            tmpReduces = splitter.getRegionCount();
            factoredReduces = (int)(factoredReduces * factor);
        }

        String bedRegions = out + "HalvadeRegions.bed";
        splitter.exportSplitter(bedRegions, hConf);
        reduces = splitter.getRegionCount();
        Logger.DEBUG("final # reducers: " + reduces);
        HalvadeConf.setBedRegions(hConf, bedRegions);
    }

    protected String roundOneDecimal(double val) {
        return onedec.format(val);
    }

    protected double getInputSize(String input, Configuration conf) throws URISyntaxException, IOException {
        double size = 0;
        FileSystem fs = FileSystem.get(new URI(input), conf);
        if (fs.getFileStatus(new Path(input)).isDirectory()) {
            // add every file in directory
            FileStatus[] files = fs.listStatus(new Path(input));
            for (FileStatus file : files) {
                if (!file.isDirectory()) {
                    size += file.getLen();
                }
            }
        } else {
            size += fs.getFileStatus(new Path(input)).getLen();
        }
        return (size / (1024 * 1024 * 1024));
    }


    protected void parseDictFile(Configuration conf) {
        be.ugent.intec.halvade.utils.Logger.DEBUG("parsing dictionary " + ref + DICT_SUFFIX);
        try {
            FileSystem fs = FileSystem.get(new URI(ref + DICT_SUFFIX), conf);
            FSDataInputStream stream = fs.open(new Path(ref + DICT_SUFFIX));
            String line = getLine(stream); // header
            dict = new SAMSequenceDictionary();
            line = getLine(stream);
            while (line != null) {
                String[] lineData = line.split("\\s+");
                String seqName = lineData[1].substring(lineData[1].indexOf(':') + 1);
                int seqLength = 0;
                try {
                    seqLength = Integer.parseInt(lineData[2].substring(lineData[2].indexOf(':') + 1));
                } catch (NumberFormatException ex) {
                    be.ugent.intec.halvade.utils.Logger.EXCEPTION(ex);
                }
                SAMSequenceRecord seq = new SAMSequenceRecord(seqName, seqLength);
//                Logger.DEBUG("name: " + seq.getSequenceName() + " length: " + seq.getSequenceLength());
                dict.addSequence(seq);
                line = getLine(stream);
            }
            HalvadeConf.setSequenceDictionary(conf, dict);
        } catch (URISyntaxException | IOException ex) {
            be.ugent.intec.halvade.utils.Logger.EXCEPTION(ex);
        }

    }

    protected String getLine(FSDataInputStream stream) throws IOException {
        String tmp = "";
        try {
            char c = (char) stream.readByte();
            while (c != '\n') {
                tmp = tmp + c;
                c = (char) stream.readByte();
            }
            return tmp;
        } catch (EOFException ex) {
            // reached end of file, return null;
            return null;
        }
    }

    protected void createOptions() {
        Option optIn = OptionBuilder.withArgName("input")
                .hasArg()
                .isRequired(true)
                .withDescription("Input directory on hdfs containing fastq files.")
                .create("I");
        Option optOut = OptionBuilder.withArgName("output")
                .hasArg()
                .isRequired(true)
                .withDescription("Output directory on hdfs.")
                .create("O");
        Option optBin = OptionBuilder.withArgName("bin.tar.gz")
                .hasArg()
                .isRequired(true)
                .withDescription("The tarred file containing all binary files located on HDFS.")
                .create("B");
        Option optRef = OptionBuilder.withArgName("reference")
                .hasArg()
                .isRequired(true)
                .withDescription("Name of the fasta file name of the reference (without extension) on HDFS. Make sure the BWA index has the same prefix.")
                .create("R");
        Option optNodes = OptionBuilder.withArgName("nodes")
                .hasArg()
                .isRequired(true)
                .withDescription("Sets the number of nodes in this cluster.")
                .withLongOpt("nodes")
                .create("N");
        Option optVcores = OptionBuilder.withArgName("cores")
                .hasArg()
                .isRequired(true)
                .withDescription("Sets the available cpu cores per node in this cluster.")
                .withLongOpt("vcores")
                .create("C");
        Option optMem = OptionBuilder.withArgName("gb")
                .hasArg()
                .isRequired(true)
                .withDescription("Sets the available memory [in GB] per node in this cluster.")
                .withLongOpt("mem")
                .create("M");
        Option optMmem = OptionBuilder.withArgName("gb")
                .hasArg()
                .withDescription("Overrides the maximum map container memory [in GB].")
                .withLongOpt("mapmem")
                .create( );
        Option optRmem = OptionBuilder.withArgName("gb")
                .hasArg()
                .withDescription("Overrides the maximum reduce container memory [in GB].")
                .withLongOpt("redmem")
                .create( );
        Option optSites = OptionBuilder.withArgName("snpDBa,snpDBb")
                .hasArg()
                .isRequired(true)
                .withDescription("Name of snpDB files for the genome on HDFS. If multiple separate with \',\'.")
                .create("D");
        Option optStarGenome = OptionBuilder.withArgName("stargenome")
                .hasArg()
                .withDescription("Directory on HDFS containing all STAR genome files")
                .withLongOpt("star")
                .create("S");
        Option optTmp = OptionBuilder.withArgName("dir")
                .hasArg()
                .withDescription("Sets the location for temporary files on every node [/tmp/halvade/].")
                .withLongOpt("tmp")
                .create();
        Option optrefdir = OptionBuilder.withArgName("dir")
                .hasArg()
                .withDescription("Sets the folder containing all the reference files for BWA or STAR and GATK on every node [tmp directory].")
                .withLongOpt("refdir")
                .create();
        Option optJava = OptionBuilder.withArgName("java")
                .hasArg()
                .withDescription("Set location of java binary to use [must be 1.7+].")
                .create("J");
        // "ID:" + RGID + " LB:" + RGLB + " PL:" + RGPL + " PU:" + RGPU + " SM:" + RGSM
        Option optID = OptionBuilder.withArgName("RGID")
                .hasArg()
                .withDescription("sets the RGID for the read-group.")
                .withLongOpt("id")
                .create( );
        Option optLB = OptionBuilder.withArgName("RGLB")
                .hasArg()
                .withDescription("sets the RGLB for the read-group.")
                .withLongOpt("lb")
                .create();
        Option optPL = OptionBuilder.withArgName("RGPL")
                .hasArg()
                .withDescription("sets the RGPL for the read-group.")
                .withLongOpt("pl")
                .create();
        Option optPU = OptionBuilder.withArgName("RGPU")
                .hasArg()
                .withDescription("sets the RGPU for the read-group.")
                .withLongOpt("pu")
                .create();
        Option optSM = OptionBuilder.withArgName("RGSM")
                .hasArg()
                .withDescription("sets the RGSM for the read-group.")
                .withLongOpt("sm")
                .create();
        Option optCov = OptionBuilder.withArgName("coverage")
                .hasArg()
                .withDescription("Sets the coverage to better distribute the tasks.")
                .withLongOpt("cov")
                .create();
        Option optScc = OptionBuilder.withArgName("scc")
                .hasArg()
                .withDescription("Sets stand_call_conf for gatk Variant Caller.")
                .withLongOpt("scc")
                .create();
        Option optSec = OptionBuilder.withArgName("sec")
                .hasArg()
                .withDescription("Sets stand_emit_conf for gatk Variant Caller.")
                .withLongOpt("sec")
                .create();
        Option optBed = OptionBuilder.withArgName("bedfile")
                .hasArg()
                .withDescription("Sets the bed file containing relevant (Genes) regions which "
                        + "will be used to split the genome into genomic regions.")
                .withLongOpt("bed")
                .create();
        Option optFilterBed = OptionBuilder.withArgName("bedfile")
                .hasArg()
                .withDescription("Sets the bed file containing relevant (Exome) regions which "
                        + " will be used to filter in the GATK steps.")
                .withLongOpt("fbed")
                .create();
        Option optGff = OptionBuilder.withArgName("gff")
                .hasArg()
                .withDescription("Sets the gff file to be used with FeautreCounts. This is required to count reads per exon.")
                .withLongOpt("gff")
                .create();
        Option optstargtf = OptionBuilder.withArgName("gtf")
                .hasArg()
                .withDescription("Sets the gtf file to be used within the STAR alignment.")
                .withLongOpt("stargtf")
                .create();
        Option optMpn = OptionBuilder.withArgName("tasks")
                .hasArg()
                .withDescription("Overrides the number of map tasks running simultaneously on each node. ")
                .withLongOpt("mpn")
                .create();
        Option optRpn = OptionBuilder.withArgName("tasks")
                .hasArg()
                .withDescription("Overrides the number of reduce tasks running simultaneously on each node. ")
                .withLongOpt("rpn")
                .create();
        Option optCustomArgs = OptionBuilder.withLongOpt("custom_args")
                .withArgName("tool=args")
                .hasArgs(2)
                .withValueSeparator()
                .withDescription("Adds custom arguments for a tool. If a module in a tool is used, add the name after an underscore. "
                        + "Possible values: " + getProgramNames())
                .withLongOpt("CA")
                .create();
        Option optVerbose = OptionBuilder.withArgName("num")
                .hasArg()
                .withDescription("Sets verbosity of debugging [2].")
                .create("v");
        Option optAln = OptionBuilder.withArgName("num")
                .hasArg()
                .withDescription("Sets the aligner used in Halvade. Possible values are 0 (bwa aln+sampe)[default], 1 (bwa mem), 2 (bowtie2), 3 (cushaw2).")
                .withLongOpt("aln")
                .create();
        Option optReadsPerRegion = OptionBuilder.withArgName("file")
                .hasArg()
                .withDescription("Give a file with read counts per region to better distribute the regions (split by readcount [default] or reorder regions by size [-reorder_regions]).")
                .withLongOpt("rpr")
                .create();

        //flags
        Option optSingle = OptionBuilder.withDescription("Sets the input files to single reads [default is paired-end reads].")
                .withLongOpt("single")
                .create();
        Option optCombine = OptionBuilder.withDescription("Just Combines the vcf on HDFS [out dir] and doesn't run the hadoop job.")
                .withLongOpt("combine")
                .create();
        Option optPp = OptionBuilder.withDescription("Uses Picard to preprocess the data for GATK.")
                .create("P");
        Option optFilterDBsnp = OptionBuilder.withDescription("Use Bedtools to select the needed interval of dbsnp.")
                .withLongOpt("filter_dbsnp")
                .create();
        Option optJustAlign = OptionBuilder.withDescription("Only align the reads.")
                .withLongOpt("justalign")
                .create("A");
        Option optSmt = OptionBuilder.withDescription("Enable simultaneous multithreading.")
                .withLongOpt("smt")
                .create();
        Option optKeep = OptionBuilder.withDescription("Keep intermediate files.")
                .withLongOpt("keep")
                .create();
        Option optHap = OptionBuilder.withDescription("Use HaplotypeCaller instead of UnifiedGenotyper for Variant Detection.")
                .withLongOpt("haplotypecaller")
                .create("H");
        Option optRna = OptionBuilder.withDescription("Run the RNA Best Practices pipeline by Broad [default is DNA pipeline]. SG needs to be set for this.")
                .withLongOpt("rna")
                .create();
        Option optDry = OptionBuilder.withDescription("Execute a dryrun, will calculate task size, split for regions etc, but not execute the MapReduce job.")
                .withLongOpt("dryrun")
                .create();
        Option optDrop = OptionBuilder.withDescription("Drop all paired-end reads where the pairs are aligned to different chromosomes.")
                .withLongOpt("drop")
                .create();
        Option optReportAll = OptionBuilder.withDescription("Reports all variants at the same location when combining variants.")
                .withLongOpt("report_all")
                .create();
        Option optBamIn = OptionBuilder.withDescription("Uses aligned bam as input files instead of unaligned fastq files.")
                .withLongOpt("bam")
                .create();
        Option optRedis = OptionBuilder.withDescription("This will enable Halvade to redistribute resources when possible when not all containers are used.")
                .withLongOpt("redistribute")
                .create();
        Option optMergeBam = OptionBuilder.withDescription("Merges all bam output from either bam input or the aligned reads from the fastq input files.")
                .withLongOpt("merge_bam")
                .create();
        Option optReorderRegions = OptionBuilder.withDescription("Use the default split way but reorder tasks by size based on the read count file given by -rpr option.")
                .withLongOpt("reorder_regions")
                .create();
        Option optupdateRG = OptionBuilder.withDescription("Update the readgroup when reading from a BAM input file.")
                .withLongOpt("update_rg")
                .create();
        Option optCount = OptionBuilder.withDescription("Count the number of reads per reduce task (for debug purposes).")
                .withLongOpt("count")
                .create();
        Option optRemDup = OptionBuilder.withDescription("Remove PCR duplicates for GATK.")
                .withLongOpt("remove_dups")
                .create();
        Option optFixEnc = OptionBuilder.withDescription("Fix the quality encoding of old (pre 1.8) Illumina fastq files to the new encoding.")
                .withLongOpt("illumina")
                .create();

        options.addOption(optIn);
        options.addOption(optOut);
        options.addOption(optRef);
        options.addOption(optSites);
        options.addOption(optBin);
        options.addOption(optTmp);
        options.addOption(optrefdir);
        options.addOption(optSingle);
        options.addOption(optAln);
        options.addOption(optID);
        options.addOption(optLB);
        options.addOption(optPL);
        options.addOption(optPU);
        options.addOption(optSM);
        options.addOption(optPp);
        options.addOption(optFilterDBsnp);
        options.addOption(optHap);
        options.addOption(optScc);
        options.addOption(optSec);
        options.addOption(optBed);
        options.addOption(optFilterBed);
        options.addOption(optJava);
        options.addOption(optCombine);
        options.addOption(optNodes);
        options.addOption(optVcores);
        options.addOption(optMem);
        options.addOption(optKeep);
        options.addOption(optJustAlign);
//        options.addOption(optCov);
        options.addOption(optMpn);
        options.addOption(optGff);
        options.addOption(optRpn);
        options.addOption(optDry);
        options.addOption(optDrop);
        options.addOption(optReportAll);
        options.addOption(optSmt);
        options.addOption(optRna);
//        options.addOption(optReadsPerRegion);
        options.addOption(optStarGenome);
        options.addOption(optBamIn);
        options.addOption(optCustomArgs);
        options.addOption(optRedis);
        options.addOption(optRmem);
        options.addOption(optMmem);
        options.addOption(optMergeBam);
        options.addOption(optVerbose);
//        options.addOption(optReorderRegions);
        options.addOption(optupdateRG);
        options.addOption(optCount);
        options.addOption(optRemDup);
        options.addOption(optstargtf);
        options.addOption(optFixEnc);
    }

    protected boolean parseArguments(String[] args, Configuration halvadeConf) throws ParseException {
        createOptions();
        CommandLineParser parser = new GnuParser();
        CommandLine line = parser.parse(options, args);

        in = line.getOptionValue("I");
        out = line.getOptionValue("O");
        if(!out.endsWith("/")) out += "/";
        ref = line.getOptionValue("R");
        sites = line.getOptionValue("D");
        halvadeBinaries = line.getOptionValue("B");
        hdfsSites = sites.split(",");
        if (line.hasOption("bam")) {
            useBamInput = true;
        }
        if (line.hasOption("rna")) {
            rnaPipeline = true;
            if (line.hasOption("star"))
                STARGenome = line.getOptionValue("star");
            if(!useBamInput && STARGenome == null) {
                throw new ParseException("the '-rna' option requires --star pointing to the location of the STAR reference directory if alignment with STAR aligner is required (fastq).");
            }
        }

        if (line.hasOption("tmp")) {
            tmpDir = line.getOptionValue("tmp");
        }
        if (line.hasOption("stargtf")) {
            stargtf = line.getOptionValue("stargtf");
        }
        if (line.hasOption("refdir")) {
            localRefDir = line.getOptionValue("refdir");
        }
        if (line.hasOption("nodes")) {
            nodes = Integer.parseInt(line.getOptionValue("nodes"));
        }
        if (line.hasOption("vcores")) {
            vcores = Integer.parseInt(line.getOptionValue("vcores"));
        }
        if (line.hasOption("smt")) {
            smtEnabled = true;
        }
        if(line.hasOption("remove_dups")) {
            keepDups = false;
        }
        if (line.hasOption("mem")) {
            mem = Double.parseDouble(line.getOptionValue("mem"));
        }
        if (line.hasOption("rpr")) {
            readCountsPerRegionFile = line.getOptionValue("rpr");
        }
        if (line.hasOption("mpn")) {
            setMapContainers = false;
            mapContainersPerNode = Integer.parseInt(line.getOptionValue("mpn"));
        }
        if (line.hasOption("rpn")) {
            setReduceContainers = false;
            reducerContainersPerNode = Integer.parseInt(line.getOptionValue("rpn"));
        }
        if (line.hasOption("mapmem")) {
            overrideMapMem = (int) (Double.parseDouble(line.getOptionValue("mapmem")) * 1024);
        }
        if (line.hasOption("redmem")) {
            overrideRedMem = (int) (Double.parseDouble(line.getOptionValue("redmem")) * 1024);
        }
        if (line.hasOption("scc")) {
            stand_call_conf = Integer.parseInt(line.getOptionValue("scc"));
        }
        if (line.hasOption("sec")) {
            stand_emit_conf = Integer.parseInt(line.getOptionValue("sec"));
        }
        if (line.hasOption("count")) {
            countOnly = true;
        }
        if (line.hasOption("report_all")) {
            reportAll = true;
        }
        if (line.hasOption("illumina")) {
            fixQualEnc = true;
        }
        if (line.hasOption("keep")) {
            keepFiles = true;
        }
        if (line.hasOption("update_rg")) {
            updateRG = true;
        }
        if (line.hasOption("redistribute")) {
            redistribute = true;
        }
        if (line.hasOption("single")) {
            paired = false;
        }
        if (line.hasOption("justalign")) {
            justAlign = true;
            combineVcf = false;
        }
        if (line.hasOption("aln")) {
            aln = Integer.parseInt(line.getOptionValue("aln"));
            if(aln < 0 || aln > 3)
                aln = 0; // default value
        }
        if (line.hasOption("J")) {
            java = line.getOptionValue("J");
        }
        if (line.hasOption("gff")) {
            gff = line.getOptionValue("gff");
        }
        if (line.hasOption("dryrun")) {
            dryRun = true;
            combineVcf = false;
        }
        if (line.hasOption("drop")) {
            keepChrSplitPairs = false;
        }
//        if (line.hasOption("cov")) {
//            coverage = Double.parseDouble(line.getOptionValue("cov"));
//        }
        if (line.hasOption("merge_bam")) {
            mergeBam = true;
        }
        if (line.hasOption("c")) {
            justCombine = true;
            combineVcf = true;
        }
        if (line.hasOption("filter_dbsnp")) {
            filterDBSnp = true;
        }
        if (line.hasOption("reorder_regions")) {
            reorderRegions = true;
        }
        if (line.hasOption("haplotypecaller")) {
            useGenotyper = false;
        }
        if (line.hasOption("P")) {
            useElPrep = false;
        }
        if (line.hasOption("id")) {
            RGID = line.getOptionValue("id");
        }
        if (line.hasOption("lb")) {
            RGLB = line.getOptionValue("lb");
        }
        if (line.hasOption("pl")) {
            RGPL = line.getOptionValue("pl");
        }
        if (line.hasOption("pu")) {
            RGPU = line.getOptionValue("pu");
        }
        if (line.hasOption("sm")) {
            RGSM = line.getOptionValue("sm");
        }
        if (line.hasOption("bed")) {
            bedFile = line.getOptionValue("bed");
        }
        if (line.hasOption("fbed")) {
            filterBed = line.getOptionValue("fbed");
        }
        if (line.hasOption("v")) {
            Logger.SETLEVEL(Integer.parseInt(line.getOptionValue("v")));
        }

        if (line.hasOption("CA")) {
            Properties props = line.getOptionProperties("CA");
            Enumeration names = props.propertyNames();
            while (names.hasMoreElements()) {
                String name = (String) names.nextElement();
                addCustomArguments(halvadeConf, name, props.getProperty(name));
            }
        }
        return true;
    }

    protected String[] programNames = {
        "bwa_aln", "bwa_mem", "bwa_sampe",
        "star",
        "elprep",
        "java",
        "samtools_view",
        "bedtools_bdsnp", "bedtools_exome",
        "picard_buildbamindex", "picard_addorreplacereadgroup", "picard_markduplicates", "picard_cleansam",
        "gatk_realignertargetcreator", "gatk_indelrealigner", "gatk_baserecalibrator", "gatk_printreads", "gatk_combinevariants",
        "gatk_variantcaller", "gatk_variantannotator", "gatk_variantfiltration", "gatk_splitncigarreads"};

    protected String getProgramNames() {
        String names = programNames[0];
        for (int i = 1; i < programNames.length; i++) {
            names += ", " + programNames[i];
        }
        return names;
    }

    protected void addCustomArguments(Configuration halvadeConf, String name, String property) throws ParseException {
        boolean found = false;
        int i = 0;
        while (!found && i < programNames.length) {
            if (name.equalsIgnoreCase(programNames[i])) {
                found = true;
            }
            i++;
        }
        if (found) {
            String[] split = name.split("_");
            String program = split[0];
            String tool = "";
            if (split.length > 1) {
                tool = split[1];
            }
            HalvadeConf.setCustomArgs(halvadeConf, program, tool, property);
            Logger.DEBUG("Custom arguments for " + name + ": \"" + property + "\"");
        } else {
            Logger.DEBUG("Unknown program: " + name + ", custom arguments [" + property + "] ignored");
            throw new ParseException("Program " + name + " not found, please use a valid name.");
        }
    }
}
