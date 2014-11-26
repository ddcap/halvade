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
import be.ugent.intec.halvade.utils.ProcessBuilderWrapper;
import be.ugent.intec.halvade.utils.SAMRecordIterator;
import fi.tkk.ics.hadoop.bam.SAMRecordWritable;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import net.sf.samtools.SAMFileHeader;
import net.sf.samtools.SAMFileWriter;
import net.sf.samtools.SAMFileWriterFactory;
import net.sf.samtools.SAMRecord;
import net.sf.samtools.SAMTextHeaderCodec;
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

    /**
     * class that wraps several picard commands
     * and calls them from java itself
     */
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
    
    public String filterDBSnps(String dbsnps, ChromosomeRange r) throws IOException, InterruptedException {
        // write a bed file with the region!
        String prefix = dbsnps.substring(0, dbsnps.lastIndexOf(".vcf")) + "_";
        File bed = new File(prefix + r + ".bed");
        r.writeToBedRegionFile(bed.getAbsolutePath());
        // open a new file to write to which will have the vcf output
        File regionVcf = new File(prefix + r + ".vcf");
        FileOutputStream vcfStream = new FileOutputStream(regionVcf.getAbsoluteFile());
        int read = 0;
        byte[] bytes = new byte[bufferSize];
        
        String customArgs = HalvadeConf.getBedToolsDbSnpArgs(context.getConfiguration());  
        String[] command = CommandGenerator.bedTools(bin, dbsnps, bed.getAbsolutePath(), customArgs);
        
        long startTime = System.currentTimeMillis();
        ProcessBuilderWrapper builder = new ProcessBuilderWrapper(command, null);
        builder.startProcess();
        // read from output and write to regionVcf file!
        InputStream is = builder.getSTDOUTStream();
        while ((read = is.read(bytes)) != -1) {
                vcfStream.write(bytes, 0, read);
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
        return regionVcf.getAbsolutePath();
        
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
        
        String customArgs = HalvadeConf.getBedToolsExomeArgs(context.getConfiguration());  
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
        
        String customArgs = HalvadeConf.getElPrepArgs(context.getConfiguration());  
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
        String customArgs = HalvadeConf.getElPrepArgs(context.getConfiguration());  
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
    
    public void callSAMToBAM(String input, String output) throws InterruptedException {
        String customArgs = HalvadeConf.getSamtoolsViewArgs(context.getConfiguration());  
        String[] command = CommandGenerator.SAMToolsView(bin, input, output, customArgs);
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
        "CleanSam.jar"
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
        String customArgs = HalvadeConf.getPicardBaiArgs(context.getConfiguration());  
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
        String customArgs = HalvadeConf.getPicardAddReadGroupArgs(context.getConfiguration());  
        command = CommandGenerator.addToCommand(command, customArgs);        
        long estimatedTime = runProcessAndWait("Picard AddOrReplaceReadGroup", GetStringVector(command));
        if(context != null)
            context.getCounter(HalvadeCounters.TIME_PICARD_ADDGRP).increment(estimatedTime);
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
        String customArgs = HalvadeConf.getPicardMarkDupArgs(context.getConfiguration());  
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
        String customArgs = HalvadeConf.getPicardCleanSamArgs(context.getConfiguration());  
        command = CommandGenerator.addToCommand(command, customArgs);        
        long estimatedTime = runProcessAndWait("Picard CleanSam", GetStringVector(command));  
        if(context != null)
            context.getCounter(HalvadeCounters.TIME_PICARD_CLEANSAM).increment(estimatedTime);
        return 0;
    }
}
