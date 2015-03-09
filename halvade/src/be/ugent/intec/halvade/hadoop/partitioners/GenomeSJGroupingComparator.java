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

import be.ugent.intec.halvade.hadoop.datatypes.GenomeSJ;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 *
 * @author ddecap
 */
public class GenomeSJGroupingComparator extends WritableComparator {
    protected GenomeSJGroupingComparator() {
        super(GenomeSJ.class, true);
    }
    
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        GenomeSJ sj1 = (GenomeSJ) a;
        GenomeSJ sj2 = (GenomeSJ) b;
        
        return sj2.getType()- sj1.getType();
    }
}
