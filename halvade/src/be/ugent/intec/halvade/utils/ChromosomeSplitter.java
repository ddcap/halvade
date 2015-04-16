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

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import net.sf.samtools.SAMSequenceDictionary;

/**
 *
 * @author ddecap
 */
public class ChromosomeSplitter {
    protected static final String[] SPECIAL_CHR = {"@@@M", "@@@_"}; // 
    protected static final int[] SPECIAL_FACTOR = {4000, 200};
    protected static final double MIN_THRESHOLD = 25.0;
    protected static final double LT_FACTOR = 5.0;
    protected int[] regionsPerChr;
    protected int[] regionSizePerChr;
    protected int[] chromosomeStartKey;
    protected int[] chromosomeSizes;
    protected String chr;
//    protected int multiplier;
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
//        int minRegions = 0;
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
                    (100.0*dict.getSequence(chr_).getSequenceLength() / maxChrLength) > MIN_THRESHOLD)
                regionLength = dict.getSequence(chr_).getSequenceLength();   
        
        long genomeLength = 0;
//        double restChr = 0;
        for(String chr_ : chrs) {
            int seqlen = dict.getSequence(chr_).getSequenceLength();
            int lenFact = checkSpecialChromsome(chr_);
            if(seqlen*lenFact > regionLength)
                genomeLength += seqlen*lenFact;
//                minRegions += (int)Math.ceil((double)seqlen / regionLength);
            else
                genomeLength += seqlen*lenFact;
//                restChr += (double)seqlen*lenFact / regionLength;
        }
//        Logger.DEBUG("minRegions: " + minRegions);
//        minRegions += (int)Math.ceil(restChr / 1.0);
//        multiplier = 1;
//        while(multiplier * minRegions < minCount) 
//            multiplier++;
//        Logger.DEBUG("multiplier: " + multiplier);
//        regionLength = regionLength / multiplier;
        regionLength = (int) (genomeLength / minCount);
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
                currentKeySize += seqlen*lenFact;
                if(currentKeySize > regionLength/LT_FACTOR) {
                    Logger.DEBUG("shared region: [" + currentKeySize+ " - " + sharedGenomes + "]", 3);
                    currentKey++;
                    currentKeySize = 0;
                    regionCount++;
                    sharedGenomes = "";
                }
                
            }
            i++;
        }
        if(currentKeySize > 0 ) {
            Logger.DEBUG("shared region: [" + currentKeySize + " - " + sharedGenomes + "]", 3);
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
                    " regions [" + (dict.getSequence(chr_).getSequenceLength() / regionsPerChr[i] + 1) + "].", 3);
                
                regionSizePerChr[i] = seqlen / regionsPerChr[i] + 1;
                chromosomeStartKey[i] = currentKey;
                currentKey += regionsPerChr[i];
                Logger.DEBUG(dict.getSequence(i).getSequenceName() + "[" + dict.getSequence(i).getSequenceLength() + 
                        "]: starts with key " + chromosomeStartKey[i] + " with " + regionsPerChr[i] + " regions of size " + regionSizePerChr[i], 3);
            }
            i++;
        }
        be.ugent.intec.halvade.utils.Logger.DEBUG("Total regions: " + regionCount);
    }
    
    public static void exportSplitter(int[] regionsPerChr, int[] regionSizePerChr, int[] chromosomeStartKey, int[] chromosomeSizes, String filename) {
        BufferedWriter br = null;
        try {
            br = new BufferedWriter(new FileWriter(filename));
            String line = regionsPerChr.length + "\n";
            br.write(line, 0, line.length());
            for(int i = 0; i < regionsPerChr.length; i++) {
                line = regionsPerChr[i] + "\t" + regionSizePerChr[i] + "\t" + chromosomeStartKey[i] + "\t" + chromosomeSizes[i] + "\n";
                br.write(line, 0, line.length());
            }
        } catch (IOException ex) {
            Logger.EXCEPTION(ex);
        } finally {
            if(br != null) {
                try {
                    br.close();
                } catch (IOException ex) {
                    Logger.EXCEPTION(ex);
                }
            }
        }
        
    }
    public void importSplitter(String filename) {
        BufferedReader br = null;
        try {
            br = new BufferedReader(new FileReader(filename));
            String line = br.readLine();
            String split[];
            int len = Integer.parseInt(line);
            regionsPerChr = new int[len];
            regionSizePerChr = new int[len];
            chromosomeStartKey = new int[len];
            chromosomeSizes = new int[len];
            for(int i = 0; i < len; i++) {
                line = br.readLine();
                split = line.split("\t");
                regionsPerChr[i] = Integer.parseInt(split[0]);
                regionSizePerChr[i] = Integer.parseInt(split[1]);
                chromosomeStartKey[i] = Integer.parseInt(split[2]);
                chromosomeSizes[i] = Integer.parseInt(split[3]);
            }
        } catch (IOException ex) {
            Logger.EXCEPTION(ex);
        } finally {
            if(br != null) {
                try {
                    br.close();
                } catch (IOException ex) {
                    Logger.EXCEPTION(ex);
                }
            }
        }
    }
}
