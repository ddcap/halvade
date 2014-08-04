/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package be.ugent.intec.halvade.hadoop.partitioners;

import be.ugent.intec.halvade.hadoop.datatypes.ChromosomeRegion;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 *
 * @author ddecap
 */
public class ChrRgRegionComparator extends WritableComparator {
    protected ChrRgRegionComparator() {
        super(ChromosomeRegion.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        ChromosomeRegion r1 = (ChromosomeRegion) a;
        ChromosomeRegion r2 = (ChromosomeRegion) b;
        
        if(r1.getChromosome() == r2.getChromosome()) {
            return r1.getRegion() - r2.getRegion();
        } else
            return r1.getChromosome() - r2.getChromosome();
    }
}