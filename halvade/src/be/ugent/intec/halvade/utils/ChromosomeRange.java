/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
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

        protected String getPicardRegion() {
            return sequenceName + ":" + alignmentStart + "-" + alignmentEnd;
        }
        
        protected String getBedRegion() {
            return sequenceName + "\t" + alignmentStart + "\t" + alignmentEnd;
        }
    }
    
    ArrayList<Range> list;
    String chr;

    public ChromosomeRange(String chr) {
        this.chr = chr;
        list = new ArrayList<Range>();
    }
    
    public void addRange(int start, int stop) {
        list.add(new Range(chr, start, stop));
    }
    
    public void addRange(String chr, int start, int stop) {
        list.add(new Range(chr, start, stop));
    }

    @Override
    public String toString() {
        return chr + "-" + list.get(0).alignmentStart;
    }
    
    public void writeToPicardRegionFile(String filename) throws IOException {        
        BufferedWriter bedWriter = new BufferedWriter(new FileWriter(filename));
        for(Range r : list) {
//            Logger.DEBUG("Region: " + r.getPicardRegion());
            bedWriter.write(r.getPicardRegion());
            bedWriter.newLine();
        }
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
