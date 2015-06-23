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

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import htsjdk.samtools.SAMSequenceDictionary;
import htsjdk.samtools.SAMSequenceRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 *
 * @author ddecap
 */
public class HalvadeConf {
    /*
     * Custom configuration
     * helps to set some fixed parameters in shared configuration
     */ 
    
    private static final String java = "Java";
    public static void setJava(Configuration conf, String val) {
        conf.set(java, val);
    } 
    public static String getJava(Configuration conf) {
        return conf.get(java);
    }
    
    private static final String bedtools = "usebedtools";
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
    
    private static final String useGenotyper = "usegeno";
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
    
    private static final String keepFiles = "keepFiles";
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
    
    private static final String useElPrep = "useiprep";
    public static void setUseElPrep(Configuration conf, boolean use) {
        if(use)
            conf.set(useElPrep, "true");
        else 
            conf.set(useElPrep, "false");
    }
    public static boolean getUseElPrep(Configuration conf) {
        String s = conf.get(useElPrep);
        if(s.equalsIgnoreCase("true"))
            return true;
        else 
            return false;
    }
    
    private static final String redist = "redistribute";
    public static void setRedistribute(Configuration conf, boolean val) {
        if(val)
            conf.set(redist, "true");
        else 
            conf.set(redist, "false");
    }
    public static boolean getRedistribute(Configuration conf) {
        String s = conf.get(redist);
        if(s.equalsIgnoreCase("true"))
            return true;
        else 
            return false;
    }
        
    private static final String paired = "ispaired";
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

    private static final String outdir = "outputdir";
    public static void setOutDir(Configuration conf, String val) {
        if(!val.endsWith("/"))
            conf.set(outdir, val + "/");
        else
            conf.set(outdir, val);
    } 
    public static String getOutDir(Configuration conf) {
        return conf.get(outdir);
    }
    
    private static final String refSize = "refsize";
    private static final long HUMAN_REF_SIZE = 3137161264L; // based on ucsc.hg19.fasta (see gatk bundle)
    public static void setRefSize(Configuration conf, long val) {
        conf.setLong(refSize, val);
    }
    public static long getRefSize(Configuration conf) {
        return conf.getLong(refSize, HUMAN_REF_SIZE);
    }
    
    private static final String vCores = "vcores";
    public static void setVcores(Configuration conf, int val) {
        conf.setInt(vCores, val);
    }
    public static int getVcores(Configuration conf) {
        return conf.getInt(vCores, 1);
    }
    
    private static final String mapThreads = "mapthreads";
    public static void setMapThreads(Configuration conf, int val) {
        conf.setInt(mapThreads, val);
    }
    public static int getMapThreads(Configuration conf) {
        return conf.getInt(mapThreads, 1);
    }
    
    private static final String reduceThreads = "reducethreads";
    public static void setReducerThreads(Configuration conf, int val) {
        conf.setInt(reduceThreads, val);
    }
    public static int getReducerThreads(Configuration conf) {
        return conf.getInt(reduceThreads, 1);
    }
    
    private static final String scratchTempDirName = "tempdir";
    public static void setScratchTempDir(Configuration conf, String val) {
        if(!val.endsWith("/"))
            conf.set(scratchTempDirName, val + "/");
        else
            conf.set(scratchTempDirName, val);
    }
    public static String getScratchTempDir(Configuration conf) {
        return conf.get(scratchTempDirName);
    }

    private static final String gff = "gff";
    public static String getGff(Configuration conf) {
        return conf.get(gff);
    }
    public static void setGff(Configuration conf, String val) {
        conf.set(gff, val);       
    }
    
    private static final String python = "python";
    public static String getPython(Configuration conf) {
        return conf.get(python, "python");
    }
    public static void setPython(Configuration conf, String val) {
        conf.set(python, val);       
    }
    
    private static final String readgroup = "readgroup";
    public static void setReadGroup(Configuration conf, String val) {
        conf.set(readgroup, val);
    }
    public static String getReadGroup(Configuration conf) {
        return conf.get(readgroup);
    }
    
    private static final String refOnScratchName = "scratchref";
    public static void setRefDirOnScratch(Configuration conf, String val) {
        conf.set(refOnScratchName, val);
    }
    public static String getRefDirOnScratch(Configuration conf) {
        return conf.get(refOnScratchName);
    }
    
    private static final String sitesOnHDFSName = "hdfssites";
    private static final String numberOfSites = "numsites";
    public static void setKnownSitesOnHDFS(Configuration conf, String[] val) throws IOException, URISyntaxException {
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
    
    private static final String tasksDone = "tasksDone/";
    public static void clearTaskFiles(Configuration conf) throws IOException, URISyntaxException {
        String filepath = conf.get(outdir) + tasksDone;
        FileSystem fs = FileSystem.get(new URI(filepath), conf);
        fs.delete(new Path(filepath), true);
    }
    
    public static boolean addTaskRunning(Configuration conf, String val) throws IOException, URISyntaxException {
        val = val.substring(0, val.lastIndexOf("_")); // rewrite file if second attempt
        String filepath = conf.get(outdir) + tasksDone + val;
        FileSystem fs = FileSystem.get(new URI(filepath), conf);
        return fs.createNewFile(new Path(filepath));
    }
    
    private static final String totalContainers = "containers";
    public static void setMapContainerCount(Configuration conf, int val) {
        conf.setInt(totalContainers, val);
    }
    public static int getMapContainerCount(Configuration conf) {
        return conf.getInt(totalContainers, 1);
    }
    public static int getMapTasksLeft(Configuration conf) throws IOException, URISyntaxException {
        int containers = conf.getInt(totalContainers, 1);        
        int tasks = 0;
        String filedir = conf.get(outdir) + tasksDone;
        FileSystem fs = FileSystem.get(new URI(filedir), conf);
        FileStatus[] files = fs.listStatus(new Path(filedir));
        for(FileStatus file : files) {
            if (!file.isDirectory()) {
                tasks++;
            }
        }
        Logger.DEBUG("containers left: " + (Integer.parseInt(conf.get("mapred.map.tasks")) - tasks));
        return Integer.parseInt(conf.get("mapred.map.tasks")) - tasks;        
    }
    
    public static boolean allTasksCompleted(Configuration conf) throws IOException, URISyntaxException {
        int tasks = 0;
        String filedir = conf.get(outdir) + tasksDone;
        FileSystem fs = FileSystem.get(new URI(filedir), conf);
        FileStatus[] files = fs.listStatus(new Path(filedir));
        for(FileStatus file : files) {
            if (!file.isDirectory()) {
                tasks++;
            }
        }
        Logger.DEBUG("tasks started: " + tasks);
        return tasks >= Integer.parseInt(conf.get("mapred.map.tasks"));        
    }
    
    private static final String refOnHDFSName = "hdfsref";
    public static void setRefOnHDFS(Configuration conf, String val) {
        conf.set(refOnHDFSName, val);
    }    
    public static String getRefOnHDFS(Configuration conf) {
        return conf.get(refOnHDFSName);
    }
    
    private static final String starDirOnHDFSName = "hdfsSTARref";
    public static void setStarDirOnHDFS(Configuration conf, String val) {
        conf.set(starDirOnHDFSName, val);
    }    
    public static String getStarDirOnHDFS(Configuration conf) {
        return conf.get(starDirOnHDFSName);
    }
    
    private static final String starDirPass2HDFSName = "hdfsSTARrefPass2";
    private static final String pass2GenomeDirName = "pass2STARGenome/";
    public static void setStarDirPass2HDFS(Configuration conf, String val) {
        if(!val.endsWith("/"))
            conf.set(starDirPass2HDFSName, val + "/" + pass2GenomeDirName);
        else
            conf.set(starDirPass2HDFSName, val + pass2GenomeDirName);
    }    
    public static String getStarDirPass2HDFS(Configuration conf) {
        return conf.get(starDirPass2HDFSName);
    }
    
    public static int getNumberOfFiles(Configuration conf) {
        return Integer.parseInt(conf.get("mapred.map.tasks"));
    }
    
    private static final String dictionarySequenceName = "seqdictionary_";
    private static final String dictionarySequenceLength = "seqdictionarylength_";
    private static final String dictionaryCount = "seqcount";
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
    
    
    private static final String chrList = "chrlist";
    public static void setChrList(Configuration conf, String val) {
        conf.set(chrList, val);
    }    
    public static String getChrList(Configuration conf) {
        return conf.get(chrList);
    }
    
    private static final String minChrSize = "minchrSize";
    private static final int DEFAULT_MIN_CHR_SIZE = 63025520;
    public static void setMinChrLength(Configuration conf, int val) {
        Logger.DEBUG("min chr size set to " + val, 3);
        conf.setInt(minChrSize, val);
    }    
    public static int getMinChrLength(Configuration conf) {
        return conf.getInt(minChrSize, DEFAULT_MIN_CHR_SIZE);
    }
        
    private static final String rna = "isRNA";
    public static void setIsRNA(Configuration conf, boolean isRNA) {
        if(isRNA)
            conf.set(rna, "true");
        else 
            conf.set(rna, "false");
    }    
    public static boolean getIsRNA(Configuration conf) {
        String s = conf.get(rna);
        if(s.equalsIgnoreCase("true"))
            return true;
        else 
            return false;
    }
    
    private static final String scc = "scc";
    private static final float DEFAULT_DNA_SCC = 30.0f;
    private static final float DEFAULT_RNA_SCC = 20.0f;
    public static void setSCC(Configuration conf, double val) {
        conf.setFloat(scc, (float) val);
    }    
    public static double getSCC(Configuration conf, boolean isRNA) {
        if(isRNA)
            return conf.getFloat(scc, DEFAULT_RNA_SCC);
        else 
            return conf.getFloat(scc, DEFAULT_DNA_SCC);
    }
    
    private static final String sec = "sec";
    private static final float DEFAULT_DNA_SEC = 30.0f;
    private static final float DEFAULT_RNA_SEC = 20.0f;
    public static void setSEC(Configuration conf, double val) {
        conf.setFloat(sec, (float)val);
    }    
    public static double getSEC(Configuration conf, boolean isRNA) {
        if(isRNA)
            return conf.getFloat(sec, DEFAULT_RNA_SEC);
        else 
            return conf.getFloat(sec, DEFAULT_DNA_SEC);
    }

    private static final String bed = "fullBed";
    public static void setBed(Configuration conf, String bed) {
        conf.set(bed, bed);
    }
    public static String getBed(Configuration conf) {
        return conf.get(bed);
    }
    private static final String bedRegions = "regionsBed";
    public static void setBedRegions(Configuration conf, String bed) {
        conf.set(bedRegions, bed);
    }
    public static String getBedRegions(Configuration conf) {
        return conf.get(bedRegions);
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
    private static final String headerFile = "headerf";
    public static void setHeaderFile(Configuration conf, String val) {
        conf.set(headerFile, val);
    }
    public static String getHeaderFile(Configuration conf) {
        return conf.get(headerFile);        
    }
    
    /*
    * Custom Arguments for all commands used in Halvade
    *
    */
    private static final String customArgs = "ca_";
    public static void setCustomArgs(Configuration conf, String programName, String toolName, String val) {
        conf.set(customArgs + programName.toLowerCase() + "_" + toolName.toLowerCase(), val);
    }
    public static String getCustomArgs(Configuration conf, String programName, String toolName) {
        return conf.get(customArgs + programName.toLowerCase() + "_" + toolName.toLowerCase());
    }

    private static final String starPass2 = "starPass2";
    public static void setIsPass2(Configuration conf, boolean val) {
        if(val)
            conf.set(starPass2, "true");
        else 
            conf.set(starPass2, "false");
    }    
    public static boolean getIsPass2(Configuration conf) {
        String s = conf.get(starPass2);
        if(s.equalsIgnoreCase("true"))
            return true;
        else 
            return false;
    }

    private static final String inputIsBam = "inputIsBam";
    public static void setInputIsBam(Configuration conf, boolean val) {
        if(val)
            conf.set(inputIsBam, "true");
        else 
            conf.set(inputIsBam, "false");
    }
    public static boolean inputIsBam(Configuration conf) {
        String s = conf.get(inputIsBam);
        if(s.equalsIgnoreCase("true"))
            return true;
        else 
            return false;
    }

}
