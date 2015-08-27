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

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;

/**
 *
 * @author ddecap
 */
public class ChromosomeRange {
    protected class Range {
        protected String sequenceName;
        protected int alignmentStart;
        protected int alignmentEnd;

        protected Range(String sequenceName, int alignmentStart, int alignmentEnd) {
            this.sequenceName = sequenceName;
            this.alignmentStart = alignmentStart;
            this.alignmentEnd = alignmentEnd;
        }

        private Range(Range get) {
            this.sequenceName = get.sequenceName;
            this.alignmentStart = get.alignmentStart;
            this.alignmentEnd = get.alignmentEnd;
        }

        protected String getPicardRegion() {
            return sequenceName + ":" + alignmentStart + "-" + alignmentEnd;
        }
        
        protected String getBedRegion() {
            return sequenceName + "\t" + (alignmentStart - 1)  + "\t" + (alignmentEnd - 1); // bed has 0 offset, while gatk,picard has 1 offset
        }
    }
    
    ArrayList<Range> list;

    public ChromosomeRange() {
        list = new ArrayList<>();
    }
    
    public void addRange(String chr, int start, int stop) {
        Logger.DEBUG("adding range: " + chr + " [" + start + " - " + stop + "]", 3);
        if(stop >= start)
            list.add(new Range(chr, start, stop));
    }

    @Override
    public String toString() {
        return list.size() + "regions_" + list.get(0).sequenceName + "-" + list.get(0).alignmentStart;
    }
    
    public void writeToPicardRegionFile(String filename) throws IOException {        
        BufferedWriter bedWriter = new BufferedWriter(new FileWriter(filename));
        for(Range r : list) {
            bedWriter.write(r.getPicardRegion());
            bedWriter.newLine();
        }
        bedWriter.close();
    }
    
    public void writeToBedRegionFile(String filename, int overlap) throws IOException {        
        BufferedWriter bedWriter = new BufferedWriter(new FileWriter(filename));
        Range r = list.get(0);
        Range tmp = new Range(r.sequenceName, Math.max(r.alignmentStart - overlap, 1), r.alignmentEnd);
        for(int i =1; i <list.size(); i++) {
            r = list.get(i);
            if(r.sequenceName.equalsIgnoreCase(tmp.sequenceName) && 
                    r.alignmentStart - overlap < tmp.alignmentEnd)
                tmp.alignmentEnd = Math.max(tmp.alignmentEnd, r.alignmentEnd);
            else {
                bedWriter.write(tmp.getBedRegion());
                bedWriter.newLine();
                tmp = new Range(r.sequenceName, Math.max(r.alignmentStart - overlap, 1), r.alignmentEnd);
            }
        }
        bedWriter.write(tmp.getBedRegion());
        bedWriter.newLine();
        bedWriter.close();
    }
    
    public void writeToBedRegionFile(String filename) throws IOException {        
        BufferedWriter bedWriter = new BufferedWriter(new FileWriter(filename));
        for(Range r : list) {
            bedWriter.write(r.getBedRegion());
            bedWriter.newLine();
        }
        bedWriter.close();
    }

    public int getAlignmentEnd() {
        return list.get(list.size() - 1).alignmentEnd;
    }

    public int getAlignmentStart() {
        return list.get(0).alignmentStart;
    }
}
