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
        
        // group per key -> which identifies region (or gathers multiple regions)
        return r1.getReduceNumber() - r2.getReduceNumber();
        
        // group per region
//        if(r1.getChromosome() == r2.getChromosome()) {
//            return r1.getRegion() - r2.getRegion();
//        } else
//            return r1.getChromosome() - r2.getChromosome();
    }
}