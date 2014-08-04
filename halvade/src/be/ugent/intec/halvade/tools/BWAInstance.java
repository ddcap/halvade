/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
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
import java.util.Random;
import net.sf.samtools.SAMFileHeader;
import net.sf.samtools.SAMRecord;
import net.sf.samtools.SAMSequenceDictionary;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author ddecap
 */
public abstract class BWAInstance {
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
    protected int[] regionSizePerChr;
    protected int[] keysPerChr;
    protected int[] randomShuffleReducers;
    protected String chr;
    protected int multiplier, minChrLength, reducers;
    protected static final long RND_SEED = 19940627L;
    
    
    protected BWAInstance(Mapper.Context context, String bin) throws IOException {
        header = null;
        writableRecord = new SAMRecordWritable();
        writableRegion = new ChromosomeRegion();
        minChrLength = MyConf.getMinChrLength(context.getConfiguration());
        multiplier = MyConf.getMultiplier(context.getConfiguration());
        chr = MyConf.getChrList(context.getConfiguration());
        
        tmpdir = MyConf.getScratchTempDir(context.getConfiguration());
        ref = MyConf.getRefOnScratch(context.getConfiguration());
        this.bin = bin;
        threads = MyConf.getNumThreads(context.getConfiguration());
        isPaired = MyConf.getIsPaired(context.getConfiguration());
        Logger.DEBUG("paired? " + isPaired);
        Logger.DEBUG("called BWAInstance constructor");
        calculateRegionsPerChromosome(context.getConfiguration());        
        //reducers = MyConf.getReducers(context.getConfiguration());
        //initiateShuffle();
    }
    
    private void initiateShuffle() {
        randomShuffleReducers = new int[reducers];
        for(int i = 0; i < reducers; i++)
            randomShuffleReducers[i] = i;
        Random rnd = new Random(RND_SEED);
        int idx, tmp;
        for(int i = randomShuffleReducers.length - 1; i > 0; i--) {
            idx = rnd.nextInt(i + 1);
            // swap
            tmp = randomShuffleReducers[idx];
            randomShuffleReducers[idx] = randomShuffleReducers[i];
            randomShuffleReducers[i] = tmp;
        }
    }
    
    private void calculateRegionsPerChromosome(Configuration conf) throws IOException {
        SAMSequenceDictionary dict = MyConf.getSequenceDictionary(conf);
        regionSizePerChr = new int[dict.size()];
        keysPerChr = new int[dict.size()];
        keysPerChr[0] = 0;
        int[] tmpRegionsPerChr = new int[dict.size()];
        for(int i = 0; i < dict.size(); i++)
            tmpRegionsPerChr[i] = 1; // assume one if not in the list
        
        if(chr == null) {
            for(int i = 0; i < dict.size(); i++)  {
                int seqlen = dict.getSequence(i).getSequenceLength();
                if (dict.getSequence(i).getSequenceLength() < minChrLength / multiplier)
                    tmpRegionsPerChr[i] = 1;
                else
                    tmpRegionsPerChr[i] = multiplier*(int)Math.ceil((double)seqlen / minChrLength);
                regionSizePerChr[i] = seqlen / tmpRegionsPerChr[i] + 1;
            }
        } else {
            String[] chrs = chr.split(",");
            for(int i = 0; i < dict.size(); i++)  {
                int seqlen = dict.getSequence(i).getSequenceLength();
                regionSizePerChr[i] = seqlen + 1;
            }
            for(String chr_ : chrs){
                int id = dict.getSequenceIndex(chr_);
                int seqlen = dict.getSequence(chr_).getSequenceLength();
                if (dict.getSequence(id).getSequenceLength() < minChrLength / multiplier)
                    tmpRegionsPerChr[id] = 1;
                else
                    tmpRegionsPerChr[id] = multiplier*(int)Math.ceil((double)seqlen / minChrLength);
                regionSizePerChr[id] = seqlen / tmpRegionsPerChr[id] + 1;
            }
        }
        for(int i = 1; i < dict.size(); i++)
            keysPerChr[i] = keysPerChr[i - 1] + tmpRegionsPerChr[i - 1];
        
        for(int i = 0; i < dict.size(); i++)
            Logger.DEBUG(i + ": " + keysPerChr[i] + " keys and regionsize is " + regionSizePerChr[i]);
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
//        return randomShuffleReducers[(int) (keysPerChr[chromosome] + region)];
        return (int) (keysPerChr[chromosome] + region);
    }
    
    int getRegion(int position, int chromosome) {
        return position / regionSizePerChr[chromosome];
    }
    
    public int writePairedSAMRecordToContext(SAMRecord sam) throws IOException, InterruptedException {
        int count = 0;
        if (!sam.getReadUnmappedFlag() && sam.getReferenceIndex() == sam.getMateReferenceIndex()) {
            context.getCounter(HalvadeCounters.OUT_BWA_READS).increment(1);
            writableRecord.set(sam);
            // positions are always leftmost positions in sam files!
            int beginpos = Math.min(sam.getAlignmentStart(), sam.getMateAlignmentStart());
            int endpos = Math.max(sam.getAlignmentStart(), sam.getMateAlignmentStart());
            endpos += sam.getReadLength();
            int beginregion = getRegion(beginpos, sam.getReferenceIndex());
            int endregion = getRegion(endpos, sam.getReferenceIndex());
            writableRegion.setChromosomeRegion(sam.getReferenceIndex(), beginregion, 
                    sam.getAlignmentStart(), getKey(beginregion, sam.getReferenceIndex()));         
            context.write(writableRegion, writableRecord);
            count++;
            if(beginregion != endregion) {
                context.getCounter(HalvadeCounters.OUT_OVERLAPPING_READS).increment(1);
                writableRegion.setChromosomeRegion(sam.getReferenceIndex(), endregion, 
                        sam.getAlignmentStart(), getKey(endregion, sam.getReferenceIndex()));
                context.write(writableRegion, writableRecord);
                count++;
            }
        }else {
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
            writableRegion.setChromosomeRegion(sam.getReferenceIndex(), beginregion, 
                    sam.getAlignmentStart(), getKey(beginregion, sam.getReferenceIndex()));
            context.write(writableRegion, writableRecord);
            count++;
            if(beginregion != endregion) {
                context.getCounter(HalvadeCounters.OUT_OVERLAPPING_READS).increment(1);
                writableRegion.setChromosomeRegion(sam.getReferenceIndex(), endregion, 
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
    public abstract void closeBWA() throws InterruptedException;
    protected abstract void startBWA(Mapper.Context context) throws IOException, InterruptedException;
}
