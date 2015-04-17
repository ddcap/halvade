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

import be.ugent.intec.halvade.hadoop.datatypes.ChromosomeRegion;
import be.ugent.intec.halvade.hadoop.datatypes.GenomeSJ;
import be.ugent.intec.halvade.hadoop.mapreduce.HalvadeCounters;
import be.ugent.intec.halvade.utils.ChromosomeSplitter;
import be.ugent.intec.halvade.utils.Logger;
import be.ugent.intec.halvade.utils.HalvadeConf;
import be.ugent.intec.halvade.utils.ProcessBuilderWrapper;
import fi.tkk.ics.hadoop.bam.SAMRecordWritable;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.util.Arrays;
import net.sf.samtools.SAMFileHeader;
import net.sf.samtools.SAMRecord;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author ddecap
 */
public abstract class AlignerInstance {
    protected SAMFileHeader header;
    protected SAMRecordWritable writableRecord;
    protected ChromosomeRegion writableRegion;
    protected GenomeSJ writeableCompactRegion;
    protected Text stub;
    protected String read1File = "reads1_";
    protected String read2File = "reads2_";
    protected String tmpdir;
    protected String ref;
    protected String bin;
    protected int threads;
//    protected static TaskInputOutputContext<LongWritable, Text, ChromosomeRegion, SAMRecordWritable> context;
    protected static Mapper.Context context;
    protected boolean isPaired = true;
    protected String chr;
    protected int minChrLength;
    protected boolean keepChrSplitPairs;
    protected boolean keep = false;
    protected ChromosomeSplitter splitter;
    protected int containerMinusTasksLeft;
    protected int containers;
    protected boolean redistribute;
    
    
    protected AlignerInstance(Mapper.Context context, String bin) throws IOException, URISyntaxException {
        AlignerInstance.context = context;
        header = null;
        containers = HalvadeConf.getMapContainerCount(context.getConfiguration());
        containerMinusTasksLeft = HalvadeConf.lessTasksLeftThanContainers(context.getConfiguration());
        redistribute = HalvadeConf.getRedistribute(context.getConfiguration());
        writableRecord = new SAMRecordWritable();
        writableRegion = new ChromosomeRegion();
        writeableCompactRegion = new GenomeSJ();
        stub = new Text();
        minChrLength = HalvadeConf.getMinChrLength(context.getConfiguration());
        chr = HalvadeConf.getChrList(context.getConfiguration());
        
        tmpdir = HalvadeConf.getScratchTempDir(context.getConfiguration());
        if(!tmpdir.endsWith("/")) tmpdir = tmpdir + "/";
        File tmp = new File(tmpdir);
        tmp.mkdirs();   
        this.bin = bin;
        threads = HalvadeConf.getMapThreads(context.getConfiguration());
        isPaired = HalvadeConf.getIsPaired(context.getConfiguration());
        Logger.DEBUG("paired? " + isPaired);
        splitter = new ChromosomeSplitter(HalvadeConf.getSequenceDictionary(context.getConfiguration()), minChrLength, chr);
        keepChrSplitPairs = HalvadeConf.getkeepChrSplitPairs(context.getConfiguration());
        keep = HalvadeConf.getKeepFiles(context.getConfiguration());
    }
    
    protected void getIdleCores(Mapper.Context context) throws IOException {
//        int totalCores = HalvadeConf.getVcores(context.getConfiguration());
//        int usedCores = 0;
//        // run "ps -eo args"  and search for any process with bwa aln
//        // count the -t options of all bwa aln instances this is used cores;
//        String line;
//        Process p = Runtime.getRuntime().exec("ps -eo args");
//        BufferedReader input = new BufferedReader(new InputStreamReader(p.getInputStream()));
//        while ((line = input.readLine()) != null) {
//            if(line.contains("bwa")) {
//                System.out.println(line); //<-- Parse data here.
//                if(line.contains("sampe") || line.contains("samse"))
//                    usedCores++;
//                else if (line.contains("aln")) {
//                    String[] cmd = line.split("\\s+");
//                    int i = 0;
//                    while (i < cmd.length && !cmd[i].equals("-t"))
//                        i++;
//                    int alnCores = 1;
//                    if (i + 1 < cmd.length)
//                        alnCores = Integer.parseInt(cmd[i+1]);
//                    Logger.DEBUG("Aln used cores: " + alnCores);
//                    usedCores += alnCores;
//                }
//            }
//        }
//        input.close();
        
//        threads = Math.max(threads, totalCores - usedCores);
        if (containerMinusTasksLeft > 0 ) threads = 6;
//        if (containerMinusTasksLeft > containers/2) threads = 8;
    }
    
    protected int feedLine(String line, ProcessBuilderWrapper proc) throws IOException  {
        if (proc.getState() != 1) {
            Logger.DEBUG("writing \'" + line +"\' to process with state " + proc.getState());
            throw new IOException("Error when writing to process with current state " + proc.getState());
        }
        proc.getSTDINWriter().write(line, 0, line.length());
        proc.getSTDINWriter().newLine();
        return 0;
    }
    
    public int writePairedSAMRecordToContext(SAMRecord sam, boolean useCompact) throws IOException, InterruptedException {
        int count = 0;
        int read1Ref = sam.getReferenceIndex();
        int read2Ref = sam.getMateReferenceIndex();
        if (!sam.getReadUnmappedFlag() && (read1Ref == read2Ref || keepChrSplitPairs) && (read1Ref > 0 || read2Ref > 0)) {
            context.getCounter(HalvadeCounters.OUT_BWA_READS).increment(1);
            writableRecord.set(sam);
            int[] keys = new int[4];
            int readLength = sam.getReadLength();
            int beginpos1 = sam.getAlignmentStart();
            int beginpos2 = sam.getMateAlignmentStart();
            int keyrange_start = 0;
            int keyrange_end = 4;
            if(read1Ref > 0) {
                keys[0] = splitter.getKey(splitter.getRegion(beginpos1, read1Ref), read1Ref);
                if(splitter.checkUpperBound(beginpos1 + readLength, read1Ref)) // check if it goes out the chr range
                    keys[1] = splitter.getKey(splitter.getRegion(beginpos1 + readLength, read1Ref), read1Ref);
                else 
                    keys[1] = keys[0];
            } else
                keyrange_start = 2;
            
            if(read2Ref > 0) {
                keys[2] = splitter.getKey(splitter.getRegion(beginpos2, read2Ref), read2Ref);
                if(splitter.checkUpperBound(beginpos2 + readLength, read2Ref)) // check if it goes out the chr range
                    keys[3] = splitter.getKey(splitter.getRegion(beginpos2 + readLength, read2Ref), read2Ref);
                else 
                    keys[3] = keys[2];
            } else 
                keyrange_end = 2;
                
            Arrays.sort(keys);
            // add this read as to be sorted to all unique found keys (mate will be added when the mate is parsed)
            if(keyrange_start != keyrange_end) {
                if(useCompact) {
                    writeableCompactRegion.setRegion(keys[keyrange_start], beginpos1);
                    context.write(writeableCompactRegion, stub);
                } else {
                    writableRegion.setChromosomeRegion(read1Ref, beginpos1, keys[keyrange_start]);
                    context.write(writableRegion, writableRecord);
                }
                count++;
                for(int i = keyrange_start + 1; i < keyrange_end; i++) {
                    if(keys[i] != keys[i - 1]) {
                        context.getCounter(HalvadeCounters.OUT_OVERLAPPING_READS).increment(1);
                        if(useCompact) {
                            writeableCompactRegion.setRegion(keys[i], beginpos1);
                            context.write(writeableCompactRegion, stub);
                        } else {
                            writableRegion.setChromosomeRegion(read1Ref, beginpos1, keys[i]);
                            context.write(writableRegion, writableRecord);
                        }
                        count++;
                    }
                }
            }
        } else {
            if(sam.getReadUnmappedFlag()) 
                context.getCounter(HalvadeCounters.OUT_UNMAPPED_READS).increment(1);
            else
                context.getCounter(HalvadeCounters.OUT_DIFF_CHR_READS).increment(1);
        }
        return count;
    }
    
    public int writeSAMRecordToContext(SAMRecord sam, boolean useCompact) throws IOException, InterruptedException {
        int count = 0;
        if (!sam.getReadUnmappedFlag()){
            context.getCounter(HalvadeCounters.OUT_BWA_READS).increment(1);
            writableRecord.set(sam);            
            int beginpos = sam.getAlignmentStart();
            int endpos = sam.getAlignmentEnd();
            int beginregion = splitter.getRegion(beginpos, sam.getReferenceIndex());
            int endregion = splitter.getRegion(endpos, sam.getReferenceIndex());
            int key = splitter.getKey(beginregion, sam.getReferenceIndex());
            if(useCompact) {
                writeableCompactRegion.setRegion(key, sam.getAlignmentStart());
                context.write(writeableCompactRegion, stub);
            } else {
                writableRegion.setChromosomeRegion(sam.getReferenceIndex(), sam.getAlignmentStart(), key);
                context.write(writableRegion, writableRecord);
            }
            count++;
            if(beginregion != endregion) {
                context.getCounter(HalvadeCounters.OUT_OVERLAPPING_READS).increment(1);
                key = splitter.getKey(endregion, sam.getReferenceIndex());
                if(useCompact) {
                    writeableCompactRegion.setRegion(key, sam.getAlignmentStart());
                    context.write(writeableCompactRegion, stub);
                } else {
                    writableRegion.setChromosomeRegion(sam.getReferenceIndex(), sam.getAlignmentStart(), key);
                    context.write(writableRegion, writableRecord);
                }
                count++;
            }
        } else {
            context.getCounter(HalvadeCounters.OUT_UNMAPPED_READS).increment(1);
        }
        return count;
    }
    
    public SAMFileHeader getFileHeader() {
        return header;
    }
    
    public void setFileHeader(SAMFileHeader header) {
        this.header = header;
    }
    
    public abstract void flushStream();  
    public abstract int getState();
    public abstract InputStream getSTDOUTStream();
    public abstract void closeAligner() throws InterruptedException;
    protected abstract void startAligner(Mapper.Context context) throws IOException, InterruptedException;
}
