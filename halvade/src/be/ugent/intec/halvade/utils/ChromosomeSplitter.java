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
import htsjdk.samtools.SAMRecord;
import htsjdk.samtools.SAMSequenceDictionary;
import java.io.File;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.Comparator;
import java.util.List;
import java.util.ListIterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 *
 * @author ddecap
 */
public final class ChromosomeSplitter {
    protected class KeyCountPair {
        protected int key;
        protected int reads;

        public KeyCountPair(int key, int reads) {
            this.key = key;
            this.reads = reads;
        }

        @Override
        public String toString() {
            return "KeyCountPair{" + "key=" + key + ", reads=" + reads + '}';
        }
        
    }
    protected class KeyCountPairComparator implements Comparator<KeyCountPair> {

        @Override
        public int compare(KeyCountPair o1, KeyCountPair o2) {
            return o2.reads - o1.reads;
        }
        
    }
    protected class BedRegion {
        protected String contig;
        protected int start;
        protected int end;
        protected int key;
        protected int reads;

        private BedRegion(String contig, String start, String end, String key) {
            this.contig = contig;
            this.start = Integer.parseInt(start);
            this.end = Integer.parseInt(end);       
            this.key = Integer.parseInt(key);
            this.reads = 0;
        }
        private BedRegion(String contig, int start, int end, int key) {
            this.contig = contig;
            this.start = start;
            this.end = end;       
            this.key = key;
            this.reads = 0;
        }
        private BedRegion(String contig, int start, int end, int key, int reads) {
            this.contig = contig;
            this.start = start;
            this.end = end;       
            this.key = key;
            this.reads = reads;
        }

        @Override
        public String toString() {
            return "BedRegion{" + "contig=" + contig + ", start=" + start + ", end=" + end + ", key=" + key + ", reads=" + reads + '}';
        }
        
    }
    protected class BedRegionComparator implements Comparator<BedRegion> {

        @Override
        public int compare(BedRegion o1, BedRegion o2) {
            return o1.key - o2.key;
        }
        
    }
//    protected HashMap<String, ArrayList<BedRegion> > regions;
    protected ArrayList<BedRegion> regions;
    protected HashMap<String, ArrayList<BedRegion> > regionsByChrom;
    protected static final double MIN_THRESHOLD = 1.25;
    protected static final double LT_FACTOR = 5.0;
    protected final int MIN_GENE_SEPARATION = 100000;
    protected int regionLength;
    protected HashMap<String ,Integer> lengthByContig;
    protected int regionCount;
    protected SAMSequenceDictionary dict;
    
    public ChromosomeSplitter(SAMSequenceDictionary dict, String bedfile, int reduceCount) throws IOException {
        this.dict = dict;
//        getMinRegionLength(bedfile, reduceCount);
        getMinRegionLength(reduceCount);
        calculateRegionsPerChromosome(bedfile);
        GetRegionsPerRegion();
    }
    public ChromosomeSplitter(SAMSequenceDictionary dict, int reduceCount) throws URISyntaxException, IOException {
        this.dict = dict;
        getMinRegionLength(reduceCount);
        calculateRegionsPerChromosome();
        GetRegionsPerRegion();
    }
    // fix regions by read counts per 10k pos??? -> either by reordering or by redefining regions by count instead of size
    public ChromosomeSplitter(SAMSequenceDictionary dict, String readsCountFile, int reduceCount, boolean useOnlyToReorder) throws URISyntaxException, IOException {
        this.dict = dict;
        getMinRegionLength(reduceCount);
        if(useOnlyToReorder)
            calculateRegionsPerChromosome();
        else
            calculateRegionsPerChromosomeByReadCount(reduceCount, readsCountFile);
        orderPartitionsBySize(readsCountFile);
        GetRegionsPerRegion();
    }     
    public ChromosomeSplitter(String filename, Configuration config) throws URISyntaxException, IOException {
        importSplitter(filename, config);
        GetRegionsPerRegion();
    }

    private void GetRegionsPerRegion() {
        regionsByChrom = new HashMap();
        for (BedRegion region: regions) {
            if(!regionsByChrom.containsKey(region.contig)) 
                regionsByChrom.put(region.contig, new ArrayList());
            regionsByChrom.get(region.contig).add(region);
        }
    }
    
    private Integer[] getKey(String refName, int pos, int pos2) {
        Integer tmpList[] = {null, null};
        ArrayList<BedRegion> keyList = regionsByChrom.get(refName);
        int i = 0;
        int found = 0;
        while (i < keyList.size() && found <2) {
            BedRegion tmp = keyList.get(i);
            if (pos >= tmp.start && pos < tmp.end) {
                tmpList[0] = tmp.key;
                found++;
            }
            if (pos2 >= tmp.start && pos2 < tmp.end) {
                tmpList[1] = tmp.key;
                found++;
            }
            i++;
        }  
/*
// old      
        for (BedRegion region: regions) {
            if (refName.equalsIgnoreCase(region.contig) && pos >= region.start && pos < region.end)
                tmpList[0] = region.key;
            if (refName.equalsIgnoreCase(region.contig) && pos2 >= region.start && pos2 < region.end)
                tmpList[1] = region.key;
        }

// last correct before
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
*/
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
        keys.removeAll(Collections.singleton(null));
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
        regions = new ArrayList();
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
//                        if(!regions.containsKey(currentContig)) regions.put(currentContig, new ArrayList());
                        regions.add(new BedRegion(currentContig, currentStart, dict.getSequence(currentContig).getSequenceLength(), i));
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
//                            if(!regions.containsKey(currentContig)) regions.put(currentContig, new ArrayList());
                            regions.add(new BedRegion(currentContig, currentStart, currentEnd, i));
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
//                if(!regions.containsKey(currentContig)) regions.put(currentContig, new ArrayList());
                regions.add(new BedRegion(currentContig, currentStart, dict.getSequence(currentContig).getSequenceLength(), i));
            }
//            for(Entry<String, ArrayList<BedRegion>> region : regions.entrySet()) {
//                String contig = region.getKey();
                for (BedRegion breg : regions) {
                    regionCount++;
                    Logger.DEBUG("region: " + breg.key + ", " + breg.contig + 
                            " (" + breg.start + " _ " + breg.end + " -> " + (breg.end - breg.start) + ")", 2);
                }
//            }                    
        } finally {
            br.close();
        }
    }
    
    private void calculateRegionsPerChromosomeByReadCount(int reduceCount, String readCountFile) throws IOException { // , boolean coverAll
        regions = new ArrayList();
        regionCount = 0;
        HashMap<String, List<BedRegion> > readCountsPerContig = makeRegionListFromFile(readCountFile);
        long readsPerGenome = 0;
        for (String contig : readCountsPerContig.keySet())
            for (BedRegion region: readCountsPerContig.get(contig))
                readsPerGenome += region.reads;
        int avgReadsPerRegion = (int) (readsPerGenome / reduceCount);
        Logger.DEBUG("total Reads: " + readsPerGenome);
        Logger.DEBUG("avg Reads Per Region: " + avgReadsPerRegion);
        
        int currentKey = 0;
        String[] chrs;
        chrs = getChromosomeNames(dict);
                
        // combine small chr
        int currentKeySize = 0;
        String currentContig;
        for(String chr_ : chrs) {
            List<BedRegion> cregions = readCountsPerContig.get(chr_);
            currentContig = chr_;
            int currentStart = -1;
            int currentEnd = -1;
            for (BedRegion region: cregions) {
                if (region.reads > avgReadsPerRegion) {
                    if(currentKeySize > 0 && currentStart != -1) {
//                        Logger.DEBUG("finishing last currentKey " + currentKey+ ": " + currentKeySize + " " + currentStart + "-" + currentEnd, 2);
                        if(currentKeySize < avgReadsPerRegion/2 && currentKey > 0)
                            regions.add(new BedRegion(currentContig, currentStart, currentEnd, currentKey - 1));                            
                        else 
                            regions.add(new BedRegion(currentContig, currentStart, currentEnd, currentKey));
                    }
                    currentKey++;
//                    Logger.DEBUG("too big region, make one key: " + currentKey + " > " + region, 2);
                    regions.add(new BedRegion(currentContig, region.start, region.end, currentKey)); 
                    currentKey++;
                    currentKeySize = 0;
                    currentStart = -1; 
                } else if(currentKeySize + region.reads > avgReadsPerRegion) {
                    regions.add(new BedRegion(currentContig, currentStart, currentEnd, currentKey));
//                    Logger.DEBUG("CurrentKey " + currentKey+ ": " + currentKeySize + " " + currentStart + "-" + currentEnd, 2);
                    currentKey++;
                    currentKeySize = region.reads;
                    currentStart = region.start;
                    currentEnd = region.end;
                    regionCount++;
                } else {
//                    Logger.DEBUG("extending region: " + currentStart + " - " + currentEnd, 3);
                    if(currentStart == -1)
                        currentStart = region.start;
                    currentEnd = region.end ;
                    currentKeySize += region.reads;
                }
            }
            if(currentKeySize > 0) {
//                    Logger.DEBUG("CurrentKey " + currentKey+ ": " + currentKeySize + " " + currentStart + "-" + currentEnd, 2);
                    regions.add(new BedRegion(currentContig, currentStart, currentEnd, currentKey));
            }
        }
        Logger.DEBUG("Total regions: " + regionCount);
            for (BedRegion breg : regions) {
                Logger.DEBUG("region: " + breg.key + ", " + breg.contig + 
                        " (" + breg.start + " _ " + breg.end + " -> " + (breg.end - breg.start) + ")", 3);
        }
    }
    
    
    
    private void calculateRegionsPerChromosome() throws IOException {
        regions = new ArrayList();
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
//                if(!regions.containsKey(currentContig)) regions.put(currentContig, new ArrayList());
                regions.add(new BedRegion(currentContig, 0, seqlen + 1, currentKey));
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
//                if(!regions.containsKey(currentContig)) regions.put(currentContig, new ArrayList());
                for(int k = 0; k < regionsPerChr; k++) {
                    regions.add(new BedRegion(currentContig, tmp, tmp + regionSize, currentKey + k));  
                    tmp += regionSize + 1;
                }
                currentKey += regionsPerChr;
            }
            i++;
        }
        be.ugent.intec.halvade.utils.Logger.DEBUG("Total regions: " + regionCount);
//        for(Entry<String, ArrayList<BedRegion>> region : regions.entrySet()) {
//            String contig = region.getKey();
            for (BedRegion breg : regions) {
                Logger.DEBUG("region: " + breg.key + ", " + breg.contig + 
                        " (" + breg.start + " _ " + breg.end + " -> " + (breg.end - breg.start) + ")", 3);
//            }
        }
    }
    
    public HashMap<String, List<BedRegion> > makeRegionListFromFile(String countsFile) throws IOException {
        HashMap<String, List<BedRegion> > readCountsPerContig = new HashMap<>();
        List<String> list = Files.readAllLines(new File(countsFile).toPath(), Charset.defaultCharset() );
        for (String line: list) {   
            String[] split0 = line.split(":");
            String[] split1 = split0[1].split("\t");
            String[] split2 = split1[0].split("-");
            String contig = split0[0];
            int r = Integer.parseInt(split1[1]);
            int s = Integer.parseInt(split2[0]);
            int e = Integer.parseInt(split2[1]);
            if(!readCountsPerContig.containsKey(contig)) readCountsPerContig.put(contig, new ArrayList());
            readCountsPerContig.get(contig).add(new BedRegion(contig, s, e, 0, r));
        }
        return readCountsPerContig;
    }
    
    public int getReadsPerContig(List<BedRegion> list, String contig) {
        int count = 0;
        for (BedRegion region: list)
            if (region.contig.equals(contig)) count += region.reads;
        return count;
    }
    
    public void orderPartitionsBySize(String countsFile) throws IOException {
        HashMap<String, List<BedRegion> > readCountsPerContig = makeRegionListFromFile(countsFile);
        
        ListIterator<BedRegion> it = regions.listIterator();
        while(it.hasNext()) {
            BedRegion region = it.next();
            String contig = region.contig;
            int reads = 0;
            List<BedRegion> readsRegions = readCountsPerContig.get(contig);
            for (BedRegion r: readsRegions) {
                if(r.end > region.start && r.start < region.end)
                        reads += r.reads;
            }
            region.reads = reads;
            it.set(region);
        }
        Collections.sort(regions, new BedRegionComparator());
        
        ArrayList<KeyCountPair> keyCounts = new ArrayList<>();
        it = regions.listIterator();
        int currentKey = 0;
        int currentReads = 0;
        while(it.hasNext()) {
            BedRegion region = it.next();
            if (region.key == currentKey) currentReads += region.reads;
            else {
                keyCounts.add(new KeyCountPair(currentKey, currentReads));
                currentReads = region.reads;
                currentKey = region.key;
            }
        }
        keyCounts.add(new KeyCountPair(currentKey, currentReads)); // ADD LAST ONE!
        Collections.sort(keyCounts, new KeyCountPairComparator());
        
        it = regions.listIterator();
        while(it.hasNext()) {
            BedRegion region = it.next();
            int i = 0;
            while (i < keyCounts.size() && keyCounts.get(i).key != region.key)
                i++;
            if (region.key == keyCounts.get(i).key) region.key = i;
        }
        
        Collections.sort(regions, new BedRegionComparator());
        
        for(BedRegion region : regions) {
           Logger.DEBUG(region.toString());
        }
        Logger.DEBUG("after reordering # reducers: "+ keyCounts.size(),3);
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
            for(BedRegion region : regions) {
                dos.writeUTF(region.contig);
                dos.writeInt(region.start);
                dos.writeInt(region.end);
                dos.writeInt(region.key);
            }
        } finally {
            if(dos != null)
                dos.close();
        }
    }
    
    private void importSplitter(String filename, Configuration conf) throws URISyntaxException, IOException {
        regions = new ArrayList();
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
                int start = dis.readInt();
                int end = dis.readInt();
                int key = dis.readInt();
                regions.add(new BedRegion(contig, start, end, key));
            }
        } finally {
            if(dis != null)
                dis.close();
        }
    }
}
