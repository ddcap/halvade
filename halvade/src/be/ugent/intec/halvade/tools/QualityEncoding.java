/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package be.ugent.intec.halvade.tools;

import be.ugent.intec.halvade.utils.Logger;
import htsjdk.samtools.SAMRecord;

/**
 *
 * @author ddecap
 */
public class QualityEncoding {
    
    public enum QENCODING { 
        SANGER,
        ILLUMINA
    }
    
    private static final int REASONABLE_SANGER_THRESHOLD = 60;
    
    public static QENCODING guessEncoding(final SAMRecord read) throws QualityException {
        final byte[] quals = read.getBaseQualities();
        byte max = quals[0];
        for ( int i = 0; i < quals.length; i++ ) {
            if(quals[i] > max) max =quals[i];
        }
        Logger.DEBUG("Max quality: " + max, 3);
        if(max <= REASONABLE_SANGER_THRESHOLD) {
            Logger.DEBUG("SANGER Quality encoding");
            return QENCODING.SANGER;
        } else {
            Logger.DEBUG("ILLUMINA Quality encoding");
            return QENCODING.ILLUMINA;
        }
    }
    
    private static final int fixQualityIlluminaToPhred = 31;  
    public static SAMRecord fixMisencodedQuals(final SAMRecord read) throws QualityException {
        final byte[] quals = read.getBaseQualities();
        for ( int i = 0; i < quals.length; i++ ) {
            quals[i] -= fixQualityIlluminaToPhred;
            if ( quals[i] < 0 )
                throw new QualityException(quals[i]);
        }
        read.setBaseQualities(quals);
        return read;
    }
}
