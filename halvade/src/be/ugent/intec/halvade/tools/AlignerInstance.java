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
import be.ugent.intec.halvade.hadoop.mapreduce.HalvadeCounters;
import be.ugent.intec.halvade.utils.Logger;
import be.ugent.intec.halvade.utils.MyConf;
import be.ugent.intec.halvade.utils.ProcessBuilderWrapper;
import fi.tkk.ics.hadoop.bam.SAMRecordWritable;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import net.sf.samtools.SAMFileHeader;
import net.sf.samtools.SAMRecord;
import net.sf.samtools.SAMSequenceDictionary;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author ddecap
 */
public abstract class AlignerInstance {
    protected static SAMFileHeader header;
    protected static SAMRecordWritable writableRecord;
    protected static ChromosomeRegion writableRegion;
    protected static String read1File = "reads1_";
    protected static String read2File = "reads2_";
    protected static String tmpdir;
    protected static String ref;
    protected static String bin;
    protected static int threads;
//    protected static TaskInputOutputContext<LongWritable, Text, ChromosomeRegion, SAMRecordWritable> context;
    protected static Mapper.Context context;
    protected boolean isPaired = true;
    protected int[] regionsPerChr;
    protected int[] regionSizePerChr;
    protected int[] chromosomeStartKey;
    protected int[] chromosomeSizes;
    protected String chr;
    protected int multiplier, minChrLength, reducers;
    protected boolean keepChrSplitPairs;
    
    
    protected AlignerInstance(Mapper.Context context, String bin) throws IOException {
        header = null;
        writableRecord = new SAMRecordWritable();
        writableRegion = new ChromosomeRegion();
        minChrLength = MyConf.getMinChrLength(context.getConfiguration());
        multiplier = MyConf.getMultiplier(context.getConfiguration());
        minChrLength = minChrLength / multiplier;
        chr = MyConf.getChrList(context.getConfiguration());
        reducers = MyConf.getReducers(context.getConfiguration());
        
        tmpdir = MyConf.getScratchTempDir(context.getConfiguration());
        ref = MyConf.getRefOnScratch(context.getConfiguration());
        this.bin = bin;
        threads = MyConf.getNumThreads(context.getConfiguration());
        isPaired = MyConf.getIsPaired(context.getConfiguration());
        Logger.DEBUG("paired? " + isPaired);
        Logger.DEBUG("called BWAInstance constructor");
        calculateRegionsPerChromosome(context.getConfiguration());
        keepChrSplitPairs = MyConf.getkeepChrSplitPairs(context.getConfiguration());
    }
    
    private String[] getChromosomeNames(SAMSequenceDictionary dict) {
        String[] chrs = new String[dict.size()];
        for(int i = 0; i < dict.size(); i++) 
            chrs[i] = dict.getSequence(i).getSequenceName();
        return chrs;
    }
    
    private void calculateRegionsPerChromosome(Configuration conf) throws IOException {
        SAMSequenceDictionary dict = MyConf.getSequenceDictionary(conf);
        regionsPerChr = new int[dict.size()];
        regionSizePerChr = new int[dict.size()];
        chromosomeStartKey = new int[dict.size()];
        chromosomeSizes = new int[dict.size()];
        int currentKey = 0;
        String[] chrs;
        if(chr == null) 
            chrs = getChromosomeNames(dict);
        else
            chrs = chr.split(",");
        
        // chr bigger than 
        int i = 0;
        for(String chr_ : chrs) {
            int seqlen = dict.getSequence(chr_).getSequenceLength();
            chromosomeSizes[i] = seqlen;
            if(seqlen >= minChrLength) {
                regionsPerChr[i] = (int)Math.ceil((double)seqlen / minChrLength);
                regionSizePerChr[i] = seqlen / regionsPerChr[i] + 1;
                chromosomeStartKey[i] = currentKey;
                currentKey += regionsPerChr[i];
            }
            i++;
        }
        
        // combine small chr
        int currentKeySize = 0;
        i = 0;
        for(String chr_ : chrs) {
            int seqlen = dict.getSequence(chr_).getSequenceLength();
            if(seqlen < minChrLength) {
                regionsPerChr[i] = 1;
                regionSizePerChr[i] = seqlen + 1;
                chromosomeStartKey[i] = currentKey;
                currentKeySize += seqlen;
                if(currentKeySize > minChrLength) {
                    currentKey++;
                    currentKeySize = 0;
                }
                
            }
            i++;
        }
        for(i = 0; i < chromosomeStartKey.length; i++)
            Logger.DEBUG(dict.getSequence(i).getSequenceName() + "[" + dict.getSequence(i).getSequenceLength() + 
                    "]: starts with key " + chromosomeStartKey[i] + " with " + regionsPerChr[i] + " regions of size " + regionSizePerChr[i]);
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
        
    int getKey(int region, int chromosome) {
        return (int) (chromosomeStartKey[chromosome] + region);
    }
    
    int getRegion(int position, int chromosome) {
        return position / regionSizePerChr[chromosome];
    }
    
    public int writePairedSAMRecordToContext(SAMRecord sam) throws IOException, InterruptedException {
        int count = 0;
        int read1Ref = sam.getReferenceIndex();
        int read2Ref = sam.getMateReferenceIndex();
        if (!sam.getReadUnmappedFlag() && (read1Ref == read2Ref || keepChrSplitPairs)) {
            context.getCounter(HalvadeCounters.OUT_BWA_READS).increment(1);
            writableRecord.set(sam);
            int[] keys = new int[4];
            int readLength = sam.getReadLength();
            int beginpos1 = sam.getAlignmentStart();
            int beginpos2 = sam.getMateAlignmentStart();
            keys[0] = getKey(getRegion(beginpos1, read1Ref), read1Ref);
            if(beginpos1 + readLength < chromosomeSizes[read1Ref]) // check if it goes out the chr range
                keys[1] = getKey(getRegion(beginpos1 + readLength, read1Ref), read1Ref);
            else 
                keys[1] = keys[0];
            
            keys[2] = getKey(getRegion(beginpos2, read2Ref), read2Ref);
            if(beginpos2 + readLength < chromosomeSizes[read2Ref]) // check if it goes out the chr range
                keys[3] = getKey(getRegion(beginpos2 + readLength, read2Ref), read2Ref);
            else 
                keys[3] = keys[2];
            Arrays.sort(keys);
            // add this read as to be sorted to all unique found keys (mate will be added when the mate is parsed)
            writableRegion.setChromosomeRegion(read1Ref, beginpos1, keys[0]);
            context.write(writableRegion, writableRecord);
            count++;
            for(int i = 1; i < 4; i++) {
                if(keys[i] != keys[i - 1]) {
                    context.getCounter(HalvadeCounters.OUT_OVERLAPPING_READS).increment(1);
                    writableRegion.setChromosomeRegion(read1Ref, beginpos1, keys[i]);
                    context.write(writableRegion, writableRecord);
                    count++;
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
    
    public int writeSAMRecordToContext(SAMRecord sam) throws IOException, InterruptedException {
        int count = 0;
        if (!sam.getReadUnmappedFlag()){
            context.getCounter(HalvadeCounters.OUT_BWA_READS).increment(1);
            writableRecord.set(sam);            
            int beginpos = sam.getAlignmentStart();
            int endpos = sam.getAlignmentEnd();
            int beginregion = getRegion(beginpos, sam.getReferenceIndex());
            int endregion = getRegion(endpos, sam.getReferenceIndex());
            writableRegion.setChromosomeRegion(sam.getReferenceIndex(),  
                    sam.getAlignmentStart(), getKey(beginregion, sam.getReferenceIndex()));
            context.write(writableRegion, writableRecord);
            count++;
            if(beginregion != endregion) {
                context.getCounter(HalvadeCounters.OUT_OVERLAPPING_READS).increment(1);
                writableRegion.setChromosomeRegion(sam.getReferenceIndex(),  
                        sam.getAlignmentStart(), getKey(endregion, sam.getReferenceIndex()));
                context.write(writableRegion, writableRecord);
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
