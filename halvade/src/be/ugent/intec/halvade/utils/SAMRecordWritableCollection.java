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
public class SAMRecordWritableCollection implements Iterator<SAMRecord> {
    Iterator<SAMRecordWritable> it;
            
    public SAMRecordWritableCollection(Iterator<SAMRecordWritable> it) {
        this.it = it;
    }

    @Override
    public boolean hasNext() {
        return it.hasNext();
    }

    @Override
    public SAMRecord next() {
        return it.next().get();
    }

    @Override
    public void remove() {
        it.remove();
    }
    
    
}
