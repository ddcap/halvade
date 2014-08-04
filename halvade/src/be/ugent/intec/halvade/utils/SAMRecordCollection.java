/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package be.ugent.intec.halvade.utils;

import fi.tkk.ics.hadoop.bam.SAMRecordWritable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import net.sf.samtools.SAMRecord;

/**
 *
 * @author ddecap
 */
public class SAMRecordCollection implements Iterator<SAMRecord> {
    ArrayList<SAMRecord> records;
    boolean locked;
    int idx;
            
    public SAMRecordCollection() {
        records = new ArrayList<SAMRecord>();
        locked = false;
        idx = 0;
    }
    
    public SAMRecordCollection(Collection<SAMRecord> coll) {
        records = new ArrayList<SAMRecord>(coll);
        locked = false;
        idx = 0;
    }
    
    public void addSAMRecord(SAMRecord rec) {
        if(!locked)
            records.add(rec);
    }
    
    public void finishAdding() {
        locked = true;
    }

    @Override
    public boolean hasNext() {
        return locked && idx < records.size();
    }

    @Override
    public SAMRecord next() {
        idx++;
        return records.get(idx - 1);
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
    
            
}
