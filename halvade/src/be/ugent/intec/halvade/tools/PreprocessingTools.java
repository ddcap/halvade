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
import be.ugent.intec.halvade.utils.MyConf;
import be.ugent.intec.halvade.utils.ProcessBuilderWrapper;
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
import net.sf.picard.sam.*;
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
        
        String customArgs = MyConf.getBedToolsDbSnpArgs(context.getConfiguration());  
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
            throw new ProcessException("Preprocessing Tool", error);
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
        
        String customArgs = MyConf.getBedToolsExomeArgs(context.getConfiguration());  
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
            throw new ProcessException("Preprocessing Tool", error);
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
    
    public int callElPrep(String input, String output, String rg, int threads, Iterator<SAMRecordWritable> it,
            SAMFileHeader header, String dictFile, ChromosomeRange r) throws InterruptedException {
        int srcount = 0; 
        
        SAMFileWriterFactory factory = new SAMFileWriterFactory();
        SAMFileWriter Swriter = factory.makeSAMWriter(header, true, new File(input));
        SAMRecord sam = null;
        int currentStart = -1, currentEnd = -1;
        if(it.hasNext()) {
            srcount++;
            sam = it.next().get();
            sam.setHeader(header);
            Swriter.addAlignment(sam);
            currentStart = sam.getAlignmentStart();
            currentEnd = sam.getAlignmentEnd();
        }
        while(it.hasNext()) {
            srcount++;
            sam = it.next().get();
            sam.setHeader(header);
            Swriter.addAlignment(sam);
            if(sam.getAlignmentStart() <= currentEnd){
                if (sam.getAlignmentEnd() > currentEnd) {
                    currentEnd = sam.getAlignmentEnd();
                }
            } else {
                // new region to start here, add current!
                r.addRange(currentStart, currentEnd);
                currentStart = sam.getAlignmentStart();
                currentEnd = sam.getAlignmentEnd();
            }
        }
        if(sam != null) {
            r.addRange(currentStart, currentEnd);
        }
        Swriter.close();
        
        String customArgs = MyConf.getElPrepArgs(context.getConfiguration());  
        String[] command = CommandGenerator.elPrep(bin, input, output, threads, true, rg, dictFile, customArgs);
        long estimatedTime = runProcessAndWait(command);
        if(context != null)
            context.getCounter(HalvadeCounters.TIME_IPREP).increment(estimatedTime);
        
        return srcount;
    }
        
    public int streamElPrep(Reducer.Context context, String output, String rg, int threads, Iterator<SAMRecordWritable> it, 
            SAMFileHeader header, String dictFile, ChromosomeRange r) throws InterruptedException, IOException {
        long startTime = System.currentTimeMillis();
        String customArgs = MyConf.getElPrepArgs(context.getConfiguration());  
        String[] command = CommandGenerator.elPrep(bin, "/dev/stdin", output, threads, true, rg, dictFile, customArgs);
//        runProcessAndWait(command);
        ProcessBuilderWrapper builder = new ProcessBuilderWrapper(command, null);
        builder.startProcess(true);        
        BufferedWriter localWriter = builder.getSTDINWriter();
        // get the header
        
        final StringWriter headerTextBuffer = new StringWriter();
        new SAMTextHeaderCodec().encode(headerTextBuffer, header);
        final String headerText = headerTextBuffer.toString();
        localWriter.write(headerText, 0, headerText.length());
        SAMRecord sam = null;
        int reads = 0;
        int currentStart = -1, currentEnd = -1;
        if(it.hasNext()) {
            sam = it.next().get();
            sam.setHeader(header);
//            Logger.DEBUG(sam.getReferenceName() + "[" + sam.getAlignmentStart() + "-" + sam.getAlignmentEnd() + "] -- " +
//                    sam.getMateReferenceName() + "[" + sam.getMateAlignmentStart() + "]");
            String samString = sam.getSAMString();
            localWriter.write(samString, 0, samString.length());
            reads++;
            currentStart = sam.getAlignmentStart();
            currentEnd = sam.getAlignmentEnd();
//            Logger.DEBUG("new region: " + currentStart + " - " + currentEnd);
        }
        while(it.hasNext()) {
            sam = it.next().get();
            sam.setHeader(header);
//            Logger.DEBUG(sam.getReferenceName() + "[" + sam.getAlignmentStart() + "-" + sam.getAlignmentEnd() + "] -- " +
//                    sam.getMateReferenceName() + "[" + sam.getMateAlignmentStart() + "]");
            String samString = sam.getSAMString();
            localWriter.write(samString, 0, samString.length());
            reads++;
            if(sam.getAlignmentStart() <= currentEnd){
                if (sam.getAlignmentEnd() > currentEnd) {
                    currentEnd = sam.getAlignmentEnd();
//                    Logger.DEBUG("extending region: " + currentEnd);
                }
            } else {
                // new region to start here, add current!
                r.addRange(currentStart, currentEnd);
//                Logger.DEBUG("write region: " + currentStart + " - " + currentEnd);
                currentStart = sam.getAlignmentStart();
                currentEnd = sam.getAlignmentEnd();
//                Logger.DEBUG("new region: " + currentStart + " - " + currentEnd);
            }
        }
        if(sam != null){
//            Logger.DEBUG("write region: " + currentStart + " - " + currentEnd);
            r.addRange(currentStart, currentEnd);
//            Logger.DEBUG(sam.getAlignmentEnd() + " end of region");
        }
        localWriter.flush();
        localWriter.close();
        
        int error = builder.waitForCompletion();
        if(error != 0)
            throw new ProcessException("Preprocessing Tool", error);
        long estimatedTime = System.currentTimeMillis() - startTime;
        Logger.DEBUG("estimated time: " + estimatedTime / 1000);
        if(context != null)
            context.getCounter(HalvadeCounters.TIME_IPREP).increment(estimatedTime);
        return reads;
    }
    
    public void callSAMToBAM(String input, String output) throws InterruptedException {
        String customArgs = MyConf.getSamtoolsViewArgs(context.getConfiguration());  
        String[] command = CommandGenerator.SAMToolsView(bin, input, output, customArgs);
        long estimatedTime = runProcessAndWait(command); 
        if(context != null)
            context.getCounter(HalvadeCounters.TIME_SAMTOBAM).increment(estimatedTime);
    }
    
    private long runProcessAndWait(String[] command) throws InterruptedException {
        long startTime = System.currentTimeMillis();
        ProcessBuilderWrapper builder = new ProcessBuilderWrapper(command, null);
        builder.startProcess();
        int error = builder.waitForCompletion();
        if(error != 0)
            throw new ProcessException("Preprocessing Tool", error);
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
        String customArgs = MyConf.getPicardBaiArgs(context.getConfiguration());  
        command.addAll(CommandGenerator.GetArguments(customArgs));        
        long estimatedTime = runProcessAndWait(GetStringVector(command));
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
        String customArgs = MyConf.getPicardAddReadGroupArgs(context.getConfiguration());  
        command.addAll(CommandGenerator.GetArguments(customArgs));        
        long estimatedTime = runProcessAndWait(GetStringVector(command));
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
        String customArgs = MyConf.getPicardMarkDupArgs(context.getConfiguration());  
        command.addAll(CommandGenerator.GetArguments(customArgs));        
        long estimatedTime = runProcessAndWait(GetStringVector(command));
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
        String customArgs = MyConf.getPicardCleanSamArgs(context.getConfiguration());  
        command.addAll(CommandGenerator.GetArguments(customArgs)); 
        long estimatedTime = runProcessAndWait(GetStringVector(command));  
        if(context != null)
            context.getCounter(HalvadeCounters.TIME_PICARD_CLEANSAM).increment(estimatedTime);
        return 0;
    }
}
