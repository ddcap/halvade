/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package be.ugent.intec.halvade.utils;

import be.ugent.intec.halvade.tools.QualityEncoding;
import be.ugent.intec.halvade.tools.QualityException;
import org.seqdoop.hadoop_bam.SAMRecordWritable;
import java.util.Iterator;
import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMRecord;

/**
 *
 * @author ddecap
 */
public class SAMRecordIterator implements Iterator<SAMRecord> {
    protected Iterator<SAMRecordWritable> it;
    protected ChromosomeRange r;
    protected SAMRecord sam = null;
    protected int reads = 0;
    protected int currentStart = -1, currentEnd = -1, currentChr = -1;
    protected String chrString = "";
    protected boolean requireFixQuality = false;
    protected SAMFileHeader header;
    protected static final int INTERVAL_OVERLAP = 51;

    public SAMRecordIterator(Iterator<SAMRecordWritable> it, SAMFileHeader header, boolean requireFixQuality) throws QualityException {
        this.it = it;
        r = new ChromosomeRange();
        this.header = header;
        this.requireFixQuality = requireFixQuality;
        getFirstRecord();
    }
    
    public SAMRecordIterator(Iterator<SAMRecordWritable> it, SAMFileHeader header, ChromosomeRange r, boolean requireFixQuality) throws QualityException {
        this.it = it;
        this.r = r;
        this.header = header;
        this.requireFixQuality = requireFixQuality;
        getFirstRecord();
    }
    
    private void getFirstRecord() throws QualityException {
        sam = null;
        if(it.hasNext()) {
            sam = it.next().get();
            sam.setHeader(header);
            if(!requireFixQuality) // is default so need to check (if true means its set manually)
                requireFixQuality = (QualityEncoding.guessEncoding(sam) == QualityEncoding.QENCODING.ILLUMINA);
            if (requireFixQuality) sam = QualityEncoding.fixMisencodedQuals(sam);
            reads++;
            currentStart = sam.getAlignmentStart();
            currentEnd = sam.getAlignmentEnd();
            currentChr = sam.getReferenceIndex();
            chrString = sam.getReferenceName();
        }
    }
    
    @Override
    public boolean hasNext() {
        return sam != null;
    }

    @Override
    public SAMRecord next() {
        SAMRecord tmp = sam;
        if (it.hasNext()) {
            try {
                sam = it.next().get();
                sam.setHeader(header);
                if (requireFixQuality) sam = QualityEncoding.fixMisencodedQuals(sam);
                reads++;
                if(sam.getReferenceIndex() == currentChr && sam.getAlignmentStart() <= currentEnd + INTERVAL_OVERLAP){
                    if (sam.getAlignmentEnd() > currentEnd) {
                        currentEnd = sam.getAlignmentEnd();
                    }
                } else {
                    // new region to start here, add current!
                    r.addRange(chrString, currentStart, currentEnd);
                    currentStart = sam.getAlignmentStart();
                    currentEnd = sam.getAlignmentEnd();
                    currentChr = sam.getReferenceIndex();
                    chrString = sam.getReferenceName();
                }
            } catch (QualityException ex) {
                Logger.EXCEPTION(ex);
                sam = null;
            }
        } else {
            r.addRange(chrString, currentStart, currentEnd);
            sam = null;
        }
        return tmp;
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    public int getCount() {
        return reads;
    }
    
    public ChromosomeRange getChromosomeRange() {
        return r;
    }
    
}
