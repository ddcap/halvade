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

package be.ugent.intec.halvade.tools;

import be.ugent.intec.halvade.hadoop.mapreduce.HalvadeCounters;
import be.ugent.intec.halvade.utils.ChromosomeRange;
import be.ugent.intec.halvade.utils.CommandGenerator;
import be.ugent.intec.halvade.utils.Logger;
import be.ugent.intec.halvade.utils.HalvadeConf;
import be.ugent.intec.halvade.utils.HalvadeFileUtils;
import be.ugent.intec.halvade.utils.ProcessBuilderWrapper;
import be.ugent.intec.halvade.utils.SAMRecordIterator;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMFileWriter;
import htsjdk.samtools.SAMFileWriterFactory;
import htsjdk.samtools.SAMRecord;
import htsjdk.samtools.SAMTextHeaderCodec;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author ddecap
 */
public class PreprocessingTools {    
    String bin;
    private int bufferSize = 8*1024;
    Reducer.Context context = null;
    String java;
    String mem = "-Xmx2g";

    public void setContext(Reducer.Context context) {
        this.context = context;
        mem = "-Xmx" + context.getConfiguration().get("mapreduce.reduce.memory.mb") + "m";
    }
    
    public PreprocessingTools(String bin) {
        this.bin = bin;
        this.java = "java";
    }

    public String getJava() {
        return java;
    }

    public void setJava(String java) {
        this.java = java;
    }
    
    private static final int BED_OVERLAP = 301;
    public String filterDBSnps(String dict, String dbsnps, ChromosomeRange r, String prefix, int threads) throws IOException, InterruptedException {
        // write a bed file with the region!
        File bed = new File(prefix + "-filtered.bed");
        r.writeToBedRegionFile(bed.getAbsolutePath(), BED_OVERLAP);
        File regionVcf = new File(prefix + "-dbsnp-filtered.vcf");
        String sortedVcfName = prefix + "-dbsnp-sorted.vcf";
        FileOutputStream vcfStream = new FileOutputStream(regionVcf.getAbsoluteFile());
        
//        String customArgs = HalvadeConf.getCustomArgs(context.getConfiguration(), "snpsift", "intidx");  
        String[] command = CommandGenerator.snpSift(java, mem, bin, dbsnps, bed.getAbsolutePath(), threads);
        
        long startTime = System.currentTimeMillis();
        ProcessBuilderWrapper builder = new ProcessBuilderWrapper(command, null);
        builder.startProcess();
        InputStream is = builder.getSTDOUTStream();
        byte[] bytes = new byte[bufferSize];
        int read = 0;
        int count = 0;
        while ((read = is.read(bytes)) != -1) {
            vcfStream.write(bytes, 0, read);
            count++;
        }
            
        int error = builder.waitForCompletion();
        if(error != 0)
            throw new ProcessException("snpSift", error);
        
        vcfStream.close();
        runSortVcf(regionVcf.getAbsolutePath(), sortedVcfName);
        
        long estimatedTime = System.currentTimeMillis() - startTime;
        Logger.DEBUG("estimated time: " + estimatedTime / 1000);
        Logger.DEBUG("vcf filtered count: " + count);
        if(context != null)
            context.getCounter(HalvadeCounters.TIME_BEDTOOLS).increment(estimatedTime);
        
        bed.delete();
        regionVcf.delete();
        return sortedVcfName;
        
    }
    
    public String filterExomeBed(String exomebed, ChromosomeRange r) throws IOException, InterruptedException {
        // write a bed file with the region!
        File bed = new File(exomebed + "_tmp.bed");
        r.writeToBedRegionFile(bed.getAbsolutePath());
        // open a new file to write to which will have the vcf output
        File regionBed = new File(exomebed + "_" + r + ".bed");
        FileOutputStream vcfStream = new FileOutputStream(regionBed.getAbsoluteFile());
        int read = 0;
        byte[] bytes = new byte[bufferSize];
        
        String customArgs = HalvadeConf.getCustomArgs(context.getConfiguration(), "bedtools", "exome");  
        String[] command = CommandGenerator.bedTools(bin, exomebed, bed.getAbsolutePath(), customArgs);
        
        long startTime = System.currentTimeMillis();
        ProcessBuilderWrapper builder = new ProcessBuilderWrapper(command, null);
        builder.startProcess();
        // read from output and write to regionVcf file!
        InputStream is = builder.getSTDOUTStream();
        int totalwritten = 0;
        while ((read = is.read(bytes)) != -1) {
                vcfStream.write(bytes, 0, read);
                totalwritten += read;
        }            
            
        int error = builder.waitForCompletion();
        if(error != 0)
            throw new ProcessException("BedTools", error);
        long estimatedTime = System.currentTimeMillis() - startTime;
        Logger.DEBUG("estimated time: " + estimatedTime / 1000);
        if(context != null)
            context.getCounter(HalvadeCounters.TIME_BEDTOOLS).increment(estimatedTime);
        
        
        vcfStream.close();
        // remove bed file?
        bed.delete();
        if (totalwritten == 0 ) 
            return null;
        else
            return regionBed.getAbsolutePath();
    }
            
    public int callElPrep(String input, String output, String rg, int threads, 
            SAMRecordIterator SAMit,
            SAMFileHeader header, String dictFile) throws InterruptedException, QualityException {
        
        SAMRecord sam;
        SAMFileWriterFactory factory = new SAMFileWriterFactory();
        SAMFileWriter Swriter = factory.makeSAMWriter(header, true, new File(input));
        
        while(SAMit.hasNext()) {
            sam = SAMit.next();
            Swriter.addAlignment(sam);
        }
        int reads = SAMit.getCount();
        Swriter.close();
        
        String customArgs = HalvadeConf.getCustomArgs(context.getConfiguration(), "elprep", "");  
        String[] command = CommandGenerator.elPrep(bin, input, output, threads, true, rg, null, customArgs);
        long estimatedTime = runProcessAndWait("elPrep", command);
        if(context != null)
            context.getCounter(HalvadeCounters.TIME_ELPREP).increment(estimatedTime);
        
        return reads;
    }
        
    public int streamElPrep(Reducer.Context context, String output, String rg, 
            int threads, SAMRecordIterator SAMit, 
            SAMFileHeader header, String dictFile) throws InterruptedException, IOException, QualityException {
        long startTime = System.currentTimeMillis();
        String customArgs = HalvadeConf.getCustomArgs(context.getConfiguration(), "elprep", "");  
        String[] command = CommandGenerator.elPrep(bin, "/dev/stdin", output, threads, true, rg, null, customArgs);
//        runProcessAndWait(command);
        ProcessBuilderWrapper builder = new ProcessBuilderWrapper(command, null);
        builder.startProcess(true);        
        BufferedWriter localWriter = builder.getSTDINWriter();
        
        // write header
        final StringWriter headerTextBuffer = new StringWriter();
        new SAMTextHeaderCodec().encode(headerTextBuffer, header);
        final String headerText = headerTextBuffer.toString();
        localWriter.write(headerText, 0, headerText.length());
        
        
        SAMRecord sam;
        while(SAMit.hasNext()) {
            sam = SAMit.next();
            String samString = sam.getSAMString();
            localWriter.write(samString, 0, samString.length());
        }
        int reads = SAMit.getCount();
        localWriter.flush();
        localWriter.close();
                
        int error = builder.waitForCompletion();
        if(error != 0)
            throw new ProcessException("elPrep", error);
        long estimatedTime = System.currentTimeMillis() - startTime;
        Logger.DEBUG("estimated time: " + estimatedTime / 1000);
        if(context != null)
            context.getCounter(HalvadeCounters.TIME_ELPREP).increment(estimatedTime);
        return reads;
    }
    
    public void callSAMToBAM(String input, String output, int threads) throws InterruptedException {
        String customArgs = HalvadeConf.getCustomArgs(context.getConfiguration(), "samtools", "view");  
        String[] command = CommandGenerator.SAMToolsView(bin, input, output, threads, customArgs);
        long estimatedTime = runProcessAndWait("SAMtools view", command); 
        if(context != null)
            context.getCounter(HalvadeCounters.TIME_SAMTOBAM).increment(estimatedTime);
    }
    
    private long runProcessAndWait(String name, String[] command) throws InterruptedException {
        long startTime = System.currentTimeMillis();
        ProcessBuilderWrapper builder = new ProcessBuilderWrapper(command, null);
        builder.startProcess();
        int error = builder.waitForCompletion();
        if(error != 0)
            throw new ProcessException(name, error);
        long estimatedTime = System.currentTimeMillis() - startTime;
        Logger.DEBUG("estimated time: " + estimatedTime / 1000);
        return estimatedTime;
    }
    
    String[] PicardTools = {
        "BuildBamIndex.jar", 
        "AddOrReplaceReadGroups.jar",
        "MarkDuplicates.jar",
        "CleanSam.jar",
        "picard.jar"
    };
    
    private static String[] GetStringVector(Collection<String> c) {
        Object[] ObjectList = c.toArray();
        return Arrays.copyOf(ObjectList,ObjectList.length,String[].class);        
    }
    
    public int runBuildBamIndex(String input) throws InterruptedException {
        String tool;
        if(bin.endsWith("/")) 
            tool = bin + PicardTools[0];
        else
            tool = bin + "/" + PicardTools[0];     
              
        ArrayList<String> command = new ArrayList<>();
        command.add(java);
        command.add(mem);
        command.add("-jar");
        command.add(tool);
        command.add("INPUT=" + input);
        String customArgs = HalvadeConf.getCustomArgs(context.getConfiguration(), "picard", "buildbamindex");  
        command = CommandGenerator.addToCommand(command, customArgs);        
        long estimatedTime = runProcessAndWait("Picard BuildBamIndex", GetStringVector(command));
        if(context != null)
            context.getCounter(HalvadeCounters.TIME_PICARD_BAI).increment(estimatedTime);
        return 0;
    }    
    public int runAddOrReplaceReadGroups(String input, String output,
            String RGID, String RGLB, String RGPL, 
            String RGPU, String RGSM) throws InterruptedException {
        String tool;
        if(bin.endsWith("/")) 
            tool = bin + PicardTools[1];
        else
            tool = bin + "/" + PicardTools[1];      
        ArrayList<String> command = new ArrayList<>();
        command.add(java);
        command.add(mem);
        command.add("-jar");
        command.add(tool);
        command.add("INPUT=" + input);
        command.add("OUTPUT=" + output);
        command.add("RGID=" + RGID);
        command.add("RGLB=" + RGLB);
        command.add("RGPL=" + RGPL);
        command.add("RGPU=" + RGPU);
        command.add("RGSM=" + RGSM);                         
        String customArgs = HalvadeConf.getCustomArgs(context.getConfiguration(), "picard", "addorreplacereadgroup");  
        command = CommandGenerator.addToCommand(command, customArgs);        
        long estimatedTime = runProcessAndWait("Picard AddOrReplaceReadGroup", GetStringVector(command));
        if(context != null)
            context.getCounter(HalvadeCounters.TIME_PICARD_ADDGRP).increment(estimatedTime);
        return 0;
    }

    public int runSortVcf(String input, String output) throws InterruptedException {
        String tool;
        if(bin.endsWith("/")) 
            tool = bin + PicardTools[4];
        else
            tool = bin + "/" + PicardTools[4];  
        ArrayList<String> command = new ArrayList<>();
        command.add(java);
        command.add(mem);
        command.add("-jar");
        command.add(tool);
        command.add("SortVcf");
        command.add("I=" + input);
        command.add("O=" + output);                     
        String customArgs = HalvadeConf.getCustomArgs(context.getConfiguration(), "picard", "sortvcf");  
        command = CommandGenerator.addToCommand(command, customArgs);        
        runProcessAndWait("Picard SortVcf", GetStringVector(command));
        // rm idx -> isnt correct???
        HalvadeFileUtils.removeLocalFile(output + ".idx");
        return 0;
    }
    public int runMarkDuplicates(String input, String output, String metrics) throws InterruptedException {
        String tool;
        if(bin.endsWith("/")) 
            tool = bin + PicardTools[2];
        else
            tool = bin + "/" + PicardTools[2];  
        ArrayList<String> command = new ArrayList<>();
        command.add(java);
        command.add(mem);
        command.add("-jar");
        command.add(tool);
        command.add("INPUT=" + input);
        command.add("OUTPUT=" + output);
        command.add("METRICS_FILE=" + metrics);                     
        String customArgs = HalvadeConf.getCustomArgs(context.getConfiguration(), "picard", "markduplicates");  
        command = CommandGenerator.addToCommand(command, customArgs);        
        long estimatedTime = runProcessAndWait("Picard MarkDuplicates", GetStringVector(command));
        if(context != null)
            context.getCounter(HalvadeCounters.TIME_PICARD_MARKDUP).increment(estimatedTime);
        return 0;
    }
    public int runCleanSam(String input, String output) throws InterruptedException {
        String tool;
        if(bin.endsWith("/")) 
            tool = bin + PicardTools[3];
        else
            tool = bin + "/" + PicardTools[3];   
        ArrayList<String> command = new ArrayList<>();
        command.add(java);
        command.add(mem);
        command.add("-jar");
        command.add(tool);
        command.add("INPUT=" + input);
        command.add("OUTPUT=" + output);                   
        String customArgs = HalvadeConf.getCustomArgs(context.getConfiguration(), "picard", "cleansam");  
        command = CommandGenerator.addToCommand(command, customArgs);        
        long estimatedTime = runProcessAndWait("Picard CleanSam", GetStringVector(command));  
        if(context != null)
            context.getCounter(HalvadeCounters.TIME_PICARD_CLEANSAM).increment(estimatedTime);
        return 0;
    }

    public void runFeatureCounts(String gff, String bam, String count) throws InterruptedException, IOException {
        long startTime = System.currentTimeMillis();
        String customArgs = HalvadeConf.getCustomArgs(context.getConfiguration(), "featureCounts", "");  
        String[] command = CommandGenerator.featureCounts(bin, gff ,bam, count, customArgs);
        
        long estimatedTime = runProcessAndWait("FeatureCounts", GetStringVector(command));
        if(context != null)
            context.getCounter(HalvadeCounters.TIME_FEATURECOUNTS).increment(estimatedTime);
        
    }
}
