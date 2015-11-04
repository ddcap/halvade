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
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map.Entry;
import htsjdk.samtools.SAMRecord;
import htsjdk.samtools.SAMSequenceDictionary;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 *
 * @author ddecap
 */
public class ChromosomeSplitter {
    protected class BedRegion {
        protected int start;
        protected int end;
        protected int key;

        private BedRegion(String start, String end, String key) {
            this.start = Integer.parseInt(start);
            this.end = Integer.parseInt(end);       
            this.key = Integer.parseInt(key);
        }
        private BedRegion(int start, int end, int key) {
            this.start = start;
            this.end = end;       
            this.key = key;
        }
    }
    protected HashMap<String, ArrayList<BedRegion> > regions;
    protected static final double MIN_THRESHOLD = 1.25;
    protected static final double LT_FACTOR = 5.0;    
    protected final int MIN_GENE_SEPARATION = 100000;
    protected int regionLength;
    protected HashMap<String ,Integer> lengthByContig;
    protected int regionCount;
    protected SAMSequenceDictionary dict;
    
    public ChromosomeSplitter(SAMSequenceDictionary dict, String bedfile, int reduceCount) throws IOException {
        this.dict = dict;
        getMinRegionLength(bedfile, reduceCount);
        getMinRegionLength(reduceCount);
        calculateRegionsPerChromosome(bedfile);
    }
    public ChromosomeSplitter(SAMSequenceDictionary dict, int reduceCount) throws URISyntaxException, IOException {
        this.dict = dict;
        getMinRegionLength(reduceCount);
        calculateRegionsPerChromosome();
    }    
    public ChromosomeSplitter(String filename, Configuration config) throws URISyntaxException, IOException {
        importSplitter(filename, config);
    }
    
    private Integer[] getKey(String refName, int pos, int pos2) {
        Integer tmpList[] = {null, null};
        int p, p2;
        if(pos < pos2)  { p = pos; p2 = pos2; }
        else            { p = pos2; p2 = pos; }
        ArrayList<BedRegion> keyList =  regions.get(refName);
        if(keyList == null) return tmpList;
        int i = 0;
        while(i < keyList.size() && p > keyList.get(i).end)
            i++;
        if(i >= keyList.size()) {
            return tmpList;
        } else {
            if(p >=  keyList.get(i).start)
                tmpList[0] = keyList.get(i).key;
            while(i < keyList.size() && p2 > keyList.get(i).end)
                i++;
            if(i >= keyList.size())
                return tmpList;
            else {
                if(p2 >=  keyList.get(i).start ) 
                    tmpList[1] = keyList.get(i).key;
            }
        }
        return tmpList;
    }
    
    public HashSet<Integer> getRegions(SAMRecord sam, int read1Ref, int read2Ref) {  
        int beginpos1 = sam.getAlignmentStart();
        int endpos1 = sam.getAlignmentEnd();
        int beginpos2 = sam.getMateAlignmentStart();
        int endpos2 =  sam.getMateAlignmentStart() + sam.getReadLength();      // is approximation, but is best we can currently do!
        HashSet<Integer> keys = new HashSet();
        if(read1Ref >= 0)
            Collections.addAll(keys, getKey(sam.getReferenceName(),beginpos1, endpos1));
        if(read2Ref >= 0)
            Collections.addAll(keys, getKey(sam.getMateReferenceName(),beginpos2,endpos2));
        keys.removeAll(Collections.singleton(null));
        return keys;
    }
    
    public HashSet<Integer> getRegions(SAMRecord sam, int read1Ref) { 
        int beginpos1 = sam.getAlignmentStart();
        int endpos1 = sam.getAlignmentEnd();      
        HashSet<Integer> keys = new HashSet();
        if(read1Ref >= 0)
            Collections.addAll(keys, getKey(sam.getReferenceName(),beginpos1,endpos1));
        return keys;
    }
    
    
    public int getRegionLength() {
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
    
    private int getMinRegionLength(String bedFile, int reduceCount) throws FileNotFoundException, IOException {
        long genomeLength = 0;
        int tmpLength = 0;
        lengthByContig = new HashMap();
        BufferedReader br = new BufferedReader(new FileReader(bedFile));
        try {
            String line = br.readLine();
            String[] bedRegion = line.split("\t");
            String currentContig = bedRegion[0];
            tmpLength = Integer.parseInt(bedRegion[2]) - Integer.parseInt(bedRegion[1]);
            line = br.readLine();
            while (line != null) {
                bedRegion = line.split("\t");
                int lineLength = Integer.parseInt(bedRegion[2]) - Integer.parseInt(bedRegion[1]);
                genomeLength += lineLength;
                if(!currentContig.equalsIgnoreCase(bedRegion[0])) {
                    lengthByContig.put(currentContig, tmpLength);
                    currentContig = bedRegion[0];
                    tmpLength = lineLength;
                } else {
                    tmpLength += lineLength;
                }
                line = br.readLine();
            }
            // add last contig length
            lengthByContig.put(currentContig, tmpLength);
        } finally {
            br.close();
        }
        regionLength = (int) (genomeLength / reduceCount);
        Logger.DEBUG("regionLength: " + regionLength);
        return regionLength;
    }
    private int getMinRegionLength(int reduceCount) {
        String[] chrs;
        chrs = getChromosomeNames(dict);
        long genomeLength = 0;
        for(String chr_ : chrs) {
            int seqlen = dict.getSequence(chr_).getSequenceLength();
                genomeLength += seqlen;
        }
        regionLength = (int) (genomeLength / reduceCount);
        Logger.DEBUG("regionLength: " + regionLength);
        return regionLength;
    }
    
    private void calculateRegionsPerChromosome(String bedFile) throws IOException { // , boolean coverAll
        regions = new HashMap();
        String currentContig = "";
        int currentStart = 0;
        int currentEnd = 0;
        int tmpLength = 0;
        int i = 0, start, end;
        regionCount = 0;
        BufferedReader br = new BufferedReader(new FileReader(bedFile));
        try {
            String line = br.readLine();
            String[] bedRegion = line.split("\t");
            end = Integer.parseInt(bedRegion[2]) + 1;
            currentContig = bedRegion[0];
            currentStart = 0;
            currentEnd = end;
            line = br.readLine();
            while (line != null) {
                bedRegion = line.split("\t");
                start = Integer.parseInt(bedRegion[1]) + 1;
                end = Integer.parseInt(bedRegion[2]) + 1;
                
                if(!bedRegion[0].equalsIgnoreCase(currentContig)) {
                    // new region -> end to chr end!
                    if(dict.getSequence(currentContig) != null) {
                        if(!regions.containsKey(currentContig)) regions.put(currentContig, new ArrayList());
                        regions.get(currentContig).add(new BedRegion(currentStart, dict.getSequence(currentContig).getSequenceLength(), i));
                        i++;
                    }
                    currentContig = bedRegion[0];
                    currentStart = 0;
                    currentEnd = end;
                } else if(bedRegion[0].equalsIgnoreCase(currentContig)) {
                    if ((currentEnd - currentStart) < regionLength) {
                        currentEnd = Math.max(end, currentEnd);
                    } else if (start < currentEnd + MIN_GENE_SEPARATION) {
                        currentEnd = Math.max(end, currentEnd);
                    } else {
                        if(dict.getSequence(currentContig) != null) {
                            currentEnd = currentEnd + (start - currentEnd) / 2;
                            if(!regions.containsKey(currentContig)) regions.put(currentContig, new ArrayList());
                            regions.get(currentContig).add(new BedRegion(currentStart, currentEnd, i));
                            i++;
                        }
                        currentContig = bedRegion[0];
                        currentStart = currentEnd + 1;
                        currentEnd = end;
                    }
                }
                line = br.readLine();
            }
            if(dict.getSequence(currentContig) != null) {
                if(!regions.containsKey(currentContig)) regions.put(currentContig, new ArrayList());
                regions.get(currentContig).add(new BedRegion(currentStart, dict.getSequence(currentContig).getSequenceLength(), i));
            }
            for(Entry<String, ArrayList<BedRegion>> region : regions.entrySet()) {
                String contig = region.getKey();
                for (BedRegion breg : region.getValue()) {
                    regionCount++;
                    Logger.DEBUG("region: " + breg.key + ", " + contig + 
                            " (" + breg.start + " _ " + breg.end + " -> " + (breg.end - breg.start) + ")", 3);
                }
            }                    
        } finally {
            br.close();
        }
    }
    private void calculateRegionsPerChromosome() throws IOException {
        regions = new HashMap();
        int currentKey = 0;
        String[] chrs;
        chrs = getChromosomeNames(dict);
        
        int i = 0;
        
        // combine small chr
        int currentKeySize = 0;
        String currentContig;
        for(String chr_ : chrs) {
            int seqlen = dict.getSequence(chr_).getSequenceLength();
            if(seqlen < regionLength) {
                currentContig = dict.getSequence(chr_).getSequenceName();
                if(!regions.containsKey(currentContig)) regions.put(currentContig, new ArrayList());
                regions.get(currentContig).add(new BedRegion(0, seqlen + 1, currentKey));
                currentKeySize += seqlen;
                if(currentKeySize > regionLength/LT_FACTOR) {
                    currentKey++;
                    currentKeySize = 0;
                    regionCount++;
                }
                
            }
            i++;
        }
        if(currentKeySize > 0 ) {
            currentKey++;
            regionCount++; 
        }
        // chr bigger than regionlength
        i = 0;
        int regionsPerChr;
        for(String chr_ : chrs) {
            int seqlen = dict.getSequence(chr_).getSequenceLength();
            if(seqlen >= regionLength) {
                regionsPerChr = (int)Math.ceil((double)seqlen / regionLength);
                regionCount += regionsPerChr;
                int regionSize = (seqlen / regionsPerChr + 1);
                int tmp = 0;
                currentContig = dict.getSequence(i).getSequenceName();
                if(!regions.containsKey(currentContig)) regions.put(currentContig, new ArrayList());
                for(int k = 0; k < regionsPerChr; k++) {
                    regions.get(currentContig).add(new BedRegion(tmp, tmp + regionSize, currentKey + k));  
                    tmp += regionSize + 1;
                }
                currentKey += regionsPerChr;
            }
            i++;
        }
        be.ugent.intec.halvade.utils.Logger.DEBUG("Total regions: " + regionCount);
        for(Entry<String, ArrayList<BedRegion>> region : regions.entrySet()) {
            String contig = region.getKey();
            for (BedRegion breg : region.getValue()) {
                Logger.DEBUG("region: " + breg.key + ", " + contig + 
                        " (" + breg.start + " _ " + breg.end + " -> " + (breg.end - breg.start) + ")", 3);
            }
        }
        
    }
    
    public void exportSplitter(String filename, Configuration conf) throws URISyntaxException, IOException {
        DataOutputStream dos = null;
        FileSystem hdfs = null;
        try {
            hdfs = FileSystem.get( new URI(filename), conf );
            Path file = new Path(filename);
            if ( hdfs.exists( file )) { hdfs.delete( file, true ); } 
            OutputStream os = hdfs.create(file);
            dos = new DataOutputStream(os);
            dos.writeInt(regions.size());
            for(Entry<String, ArrayList<BedRegion>> entry : regions.entrySet()) {
                String key = entry.getKey();
                ArrayList<BedRegion> value = entry.getValue();
                dos.writeUTF(key);
                dos.writeInt(value.size());
                for(BedRegion region : value) {
                    dos.writeInt(region.start);
                    dos.writeInt(region.end);
                    dos.writeInt(region.key);
                }
            }
        } finally {
            if(dos != null)
                dos.close();
        }
    }
    
    private void importSplitter(String filename, Configuration conf) throws URISyntaxException, IOException {
        regions = new HashMap();
        DataInputStream dis = null;
        FileSystem hdfs = null;
        try {
            hdfs = FileSystem.get( new URI(filename), conf );
            Path file = new Path(filename);
            InputStream is = hdfs.open(file);
            dis = new DataInputStream(is);
            int len = dis.readInt();
            for(int i = 0; i < len; i++) {
                String contig = dis.readUTF();
                int count = dis.readInt();
                ArrayList tmp = new ArrayList(count);
                for(int k = 0; k < count; k++) {
                    int start = dis.readInt();
                    int end = dis.readInt();
                    int key = dis.readInt();
                    tmp.add(new BedRegion(start, end, key));
                }
                regions.put(contig, tmp);
            }
        } finally {
            if(dis != null)
                dis.close();
        }
    }
}
