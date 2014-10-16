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

package be.ugent.intec.halvade.utils;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import net.sf.samtools.SAMSequenceDictionary;
import net.sf.samtools.SAMSequenceRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 *
 * @author ddecap
 */
public class MyConf {
    /*
     * Custom configuration
     * helps to set some fixed parameters in shared configuration
     */
    // names of variables
    private static final String tasksDone = "tasksDone/";
    private static final String scratchTempDirName = "tempdir";
    private static final String refOnScratchName = "scratchref";
    private static final String sitesOnHDFSName = "hdfssites";
    private static final String numberOfSites = "numsites";
    private static final String refOnHDFSName = "hdfsref";
    private static final String dictionarySequenceName = "seqdictionary_";
    private static final String dictionarySequenceLength = "seqdictionarylength_";
    private static final String dictionaryCount = "seqcount";
    private static final String threadcount = "threads";
    private static final String gatkdatathreadcount = "gatkdatathreads";
    private static final String gatkcputhreadcount = "gatkcputhreads";
    private static final String corescount = "cores";
    private static final String fastqEncoding = "hbam.fastq-input.base-quality-encoding";
    private static final String paired = "ispaired";
    private static final String bedtools = "usebedtools";
    private static final String outdir = "outputdir";
    private static final String readgroup = "readgroup";
    private static final String refSize = "refsize";
    private static final String useIPrep = "useiprep";
    private static final String reuseJVM = "reuseJVM";
    private static final String java = "Java";
    private static final String keepFiles = "keepFiles";
    private static final String useGenotyper = "usegeno";
    private static final long HUMAN_REF_SIZE = 3137161264L; // based on ucsc.hg19.fasta (see gatk bundle)
    


    public static void setJava(Configuration conf, String val) {
        conf.set(java, val);
    } 
    public static String getJava(Configuration conf) {
        return conf.get(java);
    }
    
    public static void setUseBedTools(Configuration conf, boolean useBedTools) {
        if(useBedTools)
            conf.set(bedtools, "true");
        else 
            conf.set(bedtools, "false");
    }
    
    public static boolean getUseBedTools(Configuration conf) {
        String s = conf.get(bedtools);
        if(s.equalsIgnoreCase("true"))
            return true;
        else 
            return false;
    }
    
    public static void setUseUnifiedGenotyper(Configuration conf, boolean use) {
        if(use)
            conf.set(useGenotyper, "true");
        else 
            conf.set(useGenotyper, "false");
    }

    public static boolean getUseUnifiedGenotyper(Configuration conf) {
        String s = conf.get(useGenotyper);
        if(s.equalsIgnoreCase("true"))
            return true;
        else 
            return false;
    }
    
    public static void setKeepFiles(Configuration conf, boolean use) {
        if(use)
            conf.set(keepFiles, "true");
        else 
            conf.set(keepFiles, "false");
    }

    public static boolean getKeepFiles(Configuration conf) {
        String s = conf.get(keepFiles);
        if(s.equalsIgnoreCase("true"))
            return true;
        else 
            return false;
    }
    
    public static void setUseIPrep(Configuration conf, boolean use) {
        if(use)
            conf.set(useIPrep, "true");
        else 
            conf.set(useIPrep, "false");
    }

    public static boolean getUseIPrep(Configuration conf) {
        String s = conf.get(useIPrep);
        if(s.equalsIgnoreCase("true"))
            return true;
        else 
            return false;
    }
    
    public static void setReuseJVM(Configuration conf, boolean val) {
        if(val)
            conf.set(reuseJVM, "true");
        else 
            conf.set(reuseJVM, "false");
    }

    public static boolean getReuseJVM(Configuration conf) {
        String s = conf.get(reuseJVM);
        if(s.equalsIgnoreCase("true"))
            return true;
        else 
            return false;
    }
    
    public static void setIsPaired(Configuration conf, boolean isPaired) {
        if(isPaired)
            conf.set(paired, "true");
        else 
            conf.set(paired, "false");
    }
    
    public static boolean getIsPaired(Configuration conf) {
        String s = conf.get(paired);
        if(s.equalsIgnoreCase("true"))
            return true;
        else 
            return false;
    }

    public static void setOutDir(Configuration conf, String val) {
        if(!val.endsWith("/"))
            conf.set(outdir, val + "/");
        else
            conf.set(outdir, val);
    } 
    public static String getOutDir(Configuration conf) {
        return conf.get(outdir);
    }
    
    public static void setRefSize(Configuration conf, long val) {
        conf.setLong(refSize, val);
    }
    public static long getRefSize(Configuration conf) {
        return conf.getLong(refSize, HUMAN_REF_SIZE);
    }
    
    public static void setFastqEncoding(Configuration conf, String val) {
        conf.set(fastqEncoding, val);
    }    
    public static String getFastqEncoding(Configuration conf) {
        return conf.get(fastqEncoding);
    }
    public static void setNumThreads(Configuration conf, int val) {
        conf.setInt(threadcount, val);
    }
    public static int getNumThreads(Configuration conf) {
        return conf.getInt(threadcount, 1);
    }
    
    public static void setGATKNumDataThreads(Configuration conf, int val) {
        conf.setInt(gatkdatathreadcount, val);
    }
    public static int getGATKNumDataThreads(Configuration conf) {
        return conf.getInt(gatkdatathreadcount, 1);
    }
    public static void setGATKNumCPUThreads(Configuration conf, int val) {
        conf.setInt(gatkcputhreadcount, val);
    }
    public static int getGATKNumCPUThreads(Configuration conf) {
        return conf.getInt(gatkcputhreadcount, 1);
    }

    public static void setNumNodes(Configuration conf, int val) {
        conf.setInt(corescount, val);
    }
    public static int getNumNodes(Configuration conf) {
        return conf.getInt(corescount, 1);
    }
    
    public static void setScratchTempDir(Configuration conf, String val) {
        if(!val.endsWith("/"))
            conf.set(scratchTempDirName, val + "/");
        else
            conf.set(scratchTempDirName, val);
    }
    public static String getScratchTempDir(Configuration conf) {
        return conf.get(scratchTempDirName);
    }
    
    public static void setReadGroup(Configuration conf, String val) {
        conf.set(readgroup, val);
    }
    public static String getReadGroup(Configuration conf) {
        return conf.get(readgroup);
    }
    
    public static String findRefOnScratch(Configuration conf) {
        String refBase = null;
        File dir  = new File(conf.get(scratchTempDirName));
        for (File file : dir.listFiles()) {
            if (file.getName().endsWith(".fa__")) {
                refBase = file.getAbsolutePath().substring(0, file.getAbsolutePath().length() - 2);
                Logger.DEBUG("found existing ref: \"" + refBase + "\"");
            }
        }
        return refBase;
    }
    
    public static void setRefOnScratch(Configuration conf, String val) {
        conf.set(refOnScratchName, val);
    }
    public static String getRefOnScratch(Configuration conf) {
        return conf.get(refOnScratchName);
    }
    
    public static void setKnownSitesOnHDFS(Configuration conf, String[] val) throws IOException, URISyntaxException {
        // todo improve performance, takes over 1 minute?
        conf.setInt(numberOfSites, val.length);
        FileSystem fs;
        for(int i = 0; i < val.length;i ++) {
            // check if dir add all files!
            fs = FileSystem.get(new URI(val[i]), conf);
            if(fs.isFile(new Path(val[i]))) {
                conf.set(sitesOnHDFSName + i, val[i]);
            } else {
                FileStatus[] files = fs.listStatus(new Path(val[i]));
                for(FileStatus file : files) {
                    if (!file.isDir()) {
                        conf.set(sitesOnHDFSName + i, file.getPath().toString());
                    }
                }
            }
        }
    }    
    
    public static String[] getKnownSitesOnHDFS(Configuration conf) {        
        int size = conf.getInt(numberOfSites, 0);
        String[] sites = new String[size];
        for(int i = 0; i < size;i ++) {
            sites[i] = conf.get(sitesOnHDFSName + i);
        }
        return sites;
    }
    
    public static String[] findKnownSitesOnScratch(Configuration conf, String id) {
        String snpsBase = conf.get(scratchTempDirName) + id + ".dbsnps";
        File dir  = new File(conf.get(scratchTempDirName));
        for (File file : dir.listFiles()) {
            if (file.getName().endsWith(".dbsnps__")) {
                snpsBase = file.getAbsolutePath().substring(0, file.getAbsolutePath().length() - 2);
                Logger.DEBUG("found existing snps: \"" + snpsBase + "\"");
            }
        }
        int size = conf.getInt(numberOfSites, 0);
        String[] sites = new String[size];
        // number of files starting with base: should be between size + 1 and 2*size + 1 (.idx not needed!)
        for(int i = 0; i < size; i ++) {
            sites[i] = snpsBase + i + ".vcf";
        }
        return sites;
    }
    
    public static void clearTaskFiles(Configuration conf) throws IOException, URISyntaxException {
        String filepath = conf.get(outdir) + tasksDone;
        FileSystem fs = FileSystem.get(new URI(filepath), conf);
        fs.delete(new Path(filepath), true);
    }
    
    public static boolean addTaskRunning(Configuration conf, String val) throws IOException, URISyntaxException {
        val = val.substring(0, val.lastIndexOf("_"));
        String filepath = conf.get(outdir) + tasksDone + val;
        FileSystem fs = FileSystem.get(new URI(filepath), conf);
        return fs.createNewFile(new Path(filepath));
    }
    
    public static boolean allTasksCompleted(Configuration conf) throws IOException, URISyntaxException {
        int tasks = 0;
        String filedir = conf.get(outdir) + tasksDone;
        FileSystem fs = FileSystem.get(new URI(filedir), conf);
        FileStatus[] files = fs.listStatus(new Path(filedir));
        for(FileStatus file : files) {
            if (!file.isDir()) {
                tasks++;
            }
        }
        Logger.DEBUG("tasks started: " + tasks);
        return tasks >= Integer.parseInt(conf.get("mapred.map.tasks"));        
    }
    
    public static void setRefOnHDFS(Configuration conf, String val) {
        conf.set(refOnHDFSName, val);
    }    
    public static String getRefOnHDFS(Configuration conf) {
        return conf.get(refOnHDFSName);
    }
    
    public static int getNumberOfFiles(Configuration conf) {
        return Integer.parseInt(conf.get("mapred.map.tasks"));
    }
    
    public static void setSequenceDictionary(Configuration conf, SAMSequenceDictionary dict) throws IOException, URISyntaxException {
        int counter = 0;
        for(SAMSequenceRecord seq : dict.getSequences()) {
            conf.set(dictionarySequenceName + counter, seq.getSequenceName());
            conf.setInt(dictionarySequenceLength + counter, seq.getSequenceLength());
            counter++;
        }
        conf.setInt(dictionaryCount, counter);
    }
    
    public static SAMSequenceDictionary getSequenceDictionary(Configuration conf) throws IOException {
        int counter = conf.getInt(dictionaryCount, 0);
        SAMSequenceDictionary dict = new SAMSequenceDictionary();
        for(int i = 0; i < counter; i++) {
            String seqName = conf.get(dictionarySequenceName + i);
            int seqLength = conf.getInt(dictionarySequenceLength + i, 0);
            SAMSequenceRecord seq = new SAMSequenceRecord(seqName, seqLength);
            dict.addSequence(seq);
        }
        return dict;
    }
    
    private static final String multiplier = "multiplier";
    private static final int DEFAULT_MULTIPLIER = 1;
    private static final String scc = "scc";
    private static final float DEFAULT_SCC = 30.0f;
    private static final String sec = "sec";
    private static final float DEFAULT_SEC = 30.0f;
    private static final String minChrSize = "minchrSize";
    private static final int DEFAULT_MIN_CHR_SIZE = 63025520;
    private static final String chrList = "chrlist";
    private static final String tasksPerNode = "tpn";
    private static final String nReducers = "reducersCount";
    private static final float DEFAULT_TPN = 1f;
    
    public static void setChrList(Configuration conf, String val) {
        conf.set(chrList, val);
    }    
    public static String getChrList(Configuration conf) {
        return conf.get(chrList);
    }
    public static void setReducers(Configuration conf, int val) {
        conf.setInt(nReducers, val);
    }    
    public static int getReducers(Configuration conf) {
        return conf.getInt(nReducers, 1);
    }
    public static void setMinChrLength(Configuration conf, int val) {
        Logger.DEBUG2("min chr size set to " + val);
        conf.setInt(minChrSize, val);
    }    
    public static int getMinChrLength(Configuration conf) {
        return conf.getInt(minChrSize, DEFAULT_MIN_CHR_SIZE);
    }
    public static void setMultiplier(Configuration conf, int val) {
        Logger.DEBUG2("multiplier set to " + val);
        conf.setInt(multiplier, val);
    }    
    public static int getMultiplier(Configuration conf) {
        return conf.getInt(multiplier, DEFAULT_MULTIPLIER);
    }
    public static void setSCC(Configuration conf, double val) {
        conf.setFloat(scc, (float) val);
    }    
    public static double getSCC(Configuration conf) {
        return conf.getFloat(scc, DEFAULT_SCC);
    }
    public static void setSEC(Configuration conf, double val) {
        conf.setFloat(sec, (float)val);
    }    
    public static double getSEC(Configuration conf) {
        return conf.getFloat(sec, DEFAULT_SEC);
    }

    public static void setTasksPerNode(Configuration conf, double val) {
        conf.setFloat(tasksPerNode, (float)val);
    }
    public static double getTasksPerNode(Configuration conf) {
        return conf.getFloat(tasksPerNode, DEFAULT_TPN);
    }

    private static final String bindir = "binDir";
    public static void setBinDir(Configuration conf, String bin) {
        conf.set(bindir, bin);
    }
    public static String getBinDir(Configuration conf) {
        return conf.get(bindir);
    }

    private static final String exomebed = "exomeBed";
    public static void setExomeBed(Configuration conf, String bed) {
        conf.set(exomebed, bed);
    }
    public static String getExomeBed(Configuration conf) {
        return conf.get(exomebed);
    }
    
    private static final String reportAllVariant = "reportAllVariant";
    public static void setReportAllVariant(Configuration conf, boolean val) {
        if(val)
            conf.set(reportAllVariant, "true");
        else 
            conf.set(reportAllVariant, "false");
    }    
    public static boolean getReportAllVariant(Configuration conf) {
        String s = conf.get(reportAllVariant, "false");
        if(s.equalsIgnoreCase("true"))
            return true;
        else 
            return false;
    }
    
    private static final String keepChrSplitPairs = "keepChrSplitPairs";
    public static void setkeepChrSplitPairs(Configuration conf, boolean val) {
        if(val)
            conf.set(keepChrSplitPairs, "true");
        else 
            conf.set(keepChrSplitPairs, "false");
    }    
    public static boolean getkeepChrSplitPairs(Configuration conf) {
        String s = conf.get(keepChrSplitPairs);
        if(s.equalsIgnoreCase("true"))
            return true;
        else 
            return false;
    }
    
    private static final String inputDir = "InputDir";
    public static void setInputDir(Configuration conf, String val) {
        conf.set(inputDir, val);
    }    
    public static String getInputDir(Configuration conf) {
        return conf.get(inputDir);
    }

    
    /*
    * Custom Arguments for all commands used in Halvade
    *
    */
    // Alignment
    private static final String ca_bwa_aln = "customArgsBwaAln";
    private static final String ca_bwa_mem = "customArgsBwaMem";
    private static final String ca_bwa_samxe = "customArgsBwaSamxe";
    public static void setBwaAlnArgs(Configuration conf, String val) {
        conf.set(ca_bwa_aln, val);
    }    
    public static String getBwaAlnArgs(Configuration conf) {
        return conf.get(ca_bwa_aln, "");
    }
    public static void setBwaMemArgs(Configuration conf, String val) {
        conf.set(ca_bwa_mem, val);
    }    
    public static String getBwaMemArgs(Configuration conf) {
        return conf.get(ca_bwa_mem, "");
    }
    public static void setBwaSamxeArgs(Configuration conf, String val) {
        conf.set(ca_bwa_samxe, val);
    }    
    public static String getBwaSamxeArgs(Configuration conf) {
        return conf.get(ca_bwa_samxe, "");
    }
    // Data Preparation
    private static final String ca_elprep = "customArgsElPrep";
    private static final String ca_samtools_view = "customArgsSamView";
    private static final String ca_bedtools_dbsnp = "customArgsBedToolsDbSnp";
    private static final String ca_bedtools_exome = "customArgsBedToolsExome";
    private static final String ca_picard_bai = "customArgsPicardBai";
    private static final String ca_picard_rg = "customArgsPicardReadGroup";
    private static final String ca_picard_dedup = "customArgsPicardDedup";
    private static final String ca_picard_clean = "customArgsPicardCleanSam";
    public static void setElPrepArgs(Configuration conf, String val) {
        conf.set(ca_elprep, val);
    }    
    public static String getElPrepArgs(Configuration conf) {
        return conf.get(ca_elprep, "");
    }
    public static void setSamtoolsViewArgs(Configuration conf, String val) {
        conf.set(ca_samtools_view, val);
    }    
    public static String getSamtoolsViewArgs(Configuration conf) {
        return conf.get(ca_samtools_view, "");
    }
    public static void setBedToolsDbSnpArgs(Configuration conf, String val) {
        conf.set(ca_bedtools_dbsnp, val);
    }    
    public static String getBedToolsDbSnpArgs(Configuration conf) {
        return conf.get(ca_bedtools_dbsnp, "");
    }
    public static void setBedToolsExomeArgs(Configuration conf, String val) {
        conf.set(ca_bedtools_exome, val);
    }    
    public static String getBedToolsExomeArgs(Configuration conf) {
        return conf.get(ca_bedtools_exome, "");
    }
    public static void setPicardBaiArgs(Configuration conf, String val) {
        conf.set(ca_picard_bai, val);
    }    
    public static String getPicardBaiArgs(Configuration conf) {
        return conf.get(ca_picard_bai, "");
    }
    public static void setPicardAddReadGroupArgs(Configuration conf, String val) {
        conf.set(ca_picard_rg, val);
    }    
    public static String getPicardAddReadGroupArgs(Configuration conf) {
        return conf.get(ca_picard_rg, "");
    }
    public static void setPicardMarkDupArgs(Configuration conf, String val) {
        conf.set(ca_picard_dedup, val);
    }    
    public static String getPicardMarkDupArgs(Configuration conf) {
        return conf.get(ca_picard_dedup, "");
    }
    public static void setPicardCleanSamArgs(Configuration conf, String val) {
        conf.set(ca_picard_clean, val);
    }    
    public static String getPicardCleanSamArgs(Configuration conf) {
        return conf.get(ca_picard_clean, "");
    }    
    // GATK tools
    private static final String ca_gatk_rtc = "customArgsGatkRtc"; //runRealignerTargetCreator
    private static final String ca_gatk_ir = "customArgsGatkIr"; //runIndelRealigner
    private static final String ca_gatk_br = "customArgsGatkBr"; //runBaseRecalibrator
    private static final String ca_gatk_pr = "customArgsGatkPr"; //runPrintReads
    private static final String ca_gatk_cv = "customArgsGatkCv"; //runCombineVariants
    private static final String ca_gatk_vc = "customArgsGatkVc"; //runVariantCaller    
    public static void setGatkRealignerTargetCreatorArgs(Configuration conf, String val) {
        conf.set(ca_gatk_rtc, val);
    }    
    public static String getGatkRealignerTargetCreatorArgs(Configuration conf) {
        return conf.get(ca_gatk_rtc, "");
    }
    public static void setGatkIndelRealignerArgs(Configuration conf, String val) {
        conf.set(ca_gatk_ir, val);
    }    
    public static String getGatkIndelRealignerArgs(Configuration conf) {
        return conf.get(ca_gatk_ir, "");
    }
    public static void setGatkBaseRecalibratorArgs(Configuration conf, String val) {
        conf.set(ca_gatk_br, val);
    }    
    public static String getGatkBaseRecalibratorArgs(Configuration conf) {
        return conf.get(ca_gatk_br, "");
    }
    public static void setGatkPrintReadsArgs(Configuration conf, String val) {
        conf.set(ca_gatk_pr, val);
    }    
    public static String getGatkPrintReadsArgs(Configuration conf) {
        return conf.get(ca_gatk_pr, "");
    }
    public static void setGatkCombineVariantsArgs(Configuration conf, String val) {
        conf.set(ca_gatk_cv, val);
    }    
    public static String getGatkCombineVariantsArgs(Configuration conf) {
        return conf.get(ca_gatk_cv, "");
    }
    public static void setGatkVariantCallerArgs(Configuration conf, String val) {
        conf.set(ca_gatk_vc, val);
    }    
    public static String getGatkVariantCallerArgs(Configuration conf) {
        return conf.get(ca_gatk_vc, "");
    }

}
