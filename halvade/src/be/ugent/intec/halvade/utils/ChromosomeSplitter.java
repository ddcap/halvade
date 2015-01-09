/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package be.ugent.intec.halvade.utils;

import java.io.IOException;
import net.sf.samtools.SAMSequenceDictionary;

/**
 *
 * @author ddecap
 */
public class ChromosomeSplitter {
    protected static final String[] SPECIAL_CHR = {"M", "_"}; // 
    protected static final int[] SPECIAL_FACTOR = {4000, 2};
    protected int[] regionsPerChr;
    protected int[] regionSizePerChr;
    protected int[] chromosomeStartKey;
    protected int[] chromosomeSizes;
    protected String chr;
    protected int multiplier;
    protected int regionLength;
    protected int regionCount;
    protected SAMSequenceDictionary dict;
    
    public ChromosomeSplitter(SAMSequenceDictionary dict, String chr, int minCount) throws IOException {
        this.dict = dict;
        this.chr = chr;
        getMinRegionLength(minCount);
        calculateRegionsPerChromosome();
    }
    
    public ChromosomeSplitter(SAMSequenceDictionary dict, int regionLength, String chr) throws IOException {
        this.dict = dict;
        this.regionLength = regionLength;
        this.chr = chr;
        calculateRegionsPerChromosome();
    }
    
    public int getKey(int region, int chromosome) {
        return (int) (chromosomeStartKey[chromosome] + region);
    }
    
    public int getRegion(int position, int chromosome) {
        return position / regionSizePerChr[chromosome];
    }
    
    public boolean checkUpperBound(int pos, int ref) {
        return pos < chromosomeSizes[ref];
    }
    
    public int getRegionSize() {
        return regionLength;
    }

    public int getRegionCount() {
        return regionCount;
    }
    
    private String[] getChromosomeNames(SAMSequenceDictionary dict) {
        String[] chrs = new String[dict.size()];
        for(int i = 0; i < dict.size(); i++) 
            chrs[i] = dict.getSequence(i).getSequenceName();
        return chrs;
    }
    
    private int checkSpecialChromsome(String chr_) {
        int factor = 1, s = 0;
        while(factor == 1 && s < SPECIAL_CHR.length){
            if(chr_.contains(SPECIAL_CHR[s]))
                factor = SPECIAL_FACTOR[s];
            s++;
        }
        return factor;
    }
    
    private int getMinRegionLength(int minCount) {
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
        regionLength = maxChrLength;
        for(String chr_ : chrs)
            if(dict.getSequence(chr_).getSequenceLength() < regionLength &&
                    (100.0*dict.getSequence(chr_).getSequenceLength() / maxChrLength) > 25.0)
                regionLength = dict.getSequence(chr_).getSequenceLength();   
        
        double restChr = 0;
        for(String chr_ : chrs) {
            int seqlen = dict.getSequence(chr_).getSequenceLength();
            int lenFact = checkSpecialChromsome(chr_);
            if(seqlen*lenFact > regionLength)
                minRegions += (int)Math.ceil((double)seqlen / regionLength);
            else
                restChr += (double)seqlen / regionLength;
        }
        
        minRegions += (int)Math.ceil(restChr / 1.0);
        multiplier = 1;
        while(multiplier * minRegions < minCount) 
            multiplier++;
        regionLength = regionLength / multiplier;
        Logger.DEBUG("maximum regionLength: " + regionLength);
        return regionLength;
    }
    
    private void calculateRegionsPerChromosome() throws IOException {
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
        
        int i = 0;
        
        // combine small chr
        int currentKeySize = 0;
        String sharedGenomes = "";
        for(String chr_ : chrs) {
            int seqlen = dict.getSequence(chr_).getSequenceLength();
            int lenFact = checkSpecialChromsome(chr_);
            if(seqlen * lenFact < regionLength) {
                sharedGenomes += dict.getSequence(chr_).getSequenceName() + " ";
                regionsPerChr[i] = 1;
                regionSizePerChr[i] = seqlen + 1;
                chromosomeStartKey[i] = currentKey;
                currentKeySize += seqlen;
                if(currentKeySize > regionLength/(3*lenFact)) {
                    Logger.DEBUG("shared region: [" + currentKeySize+ " - " + sharedGenomes + "]");
                    currentKey++;
                    currentKeySize = 0;
                    regionCount++;
                    sharedGenomes = "";
                }
                
            }
            i++;
        }
        if(currentKeySize > 0 ) {
            Logger.DEBUG("shared region: [" + currentKeySize + " - " + sharedGenomes + "]");
            currentKey++;
            regionCount++; 
        }
        // chr bigger than regionlength
        i = 0;
        for(String chr_ : chrs) {
            int seqlen = dict.getSequence(chr_).getSequenceLength();
            int lenFact = checkSpecialChromsome(chr_);
            chromosomeSizes[i] = seqlen;
            if(seqlen*lenFact >= regionLength) {
                regionsPerChr[i] = (int)Math.ceil((double)seqlen*lenFact / regionLength);
                regionCount += regionsPerChr[i];
                Logger.DEBUG(dict.getSequence(chr_).getSequenceName() + ": " + regionsPerChr[i] + 
                    " regions [" + (dict.getSequence(chr_).getSequenceLength() / regionsPerChr[i] + 1) + "].",
                        3);
                
                regionSizePerChr[i] = seqlen / regionsPerChr[i] + 1;
                chromosomeStartKey[i] = currentKey;
                currentKey += regionsPerChr[i];
                Logger.DEBUG(dict.getSequence(i).getSequenceName() + "[" + dict.getSequence(i).getSequenceLength() + 
                        "]: starts with key " + chromosomeStartKey[i] + " with " + regionsPerChr[i] + " regions of size " + regionSizePerChr[i]);
            }
            i++;
        }
        be.ugent.intec.halvade.utils.Logger.DEBUG("Total regions: " + regionCount);
    }
}
