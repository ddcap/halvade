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

package be.ugent.intec.halvade.hadoop.datatypes;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.WritableComparable;

/**
 *
 * @author ddecap
 */
public class ChromosomeRegion implements WritableComparable<ChromosomeRegion> {
    protected int chromosome;
    protected int position;
    protected int reduceNumber; // identifies region in chromosome but every number is unique, so chr is also separate!

    public int getChromosome() {
        return chromosome;
    }

    public int getPosition() {
        return position;
    }
    
    public int getReduceNumber() {
        return reduceNumber;
    }

    public void setChromosomeRegion(int chromosome, int position, int reducenumber) {
        this.chromosome = chromosome;
        this.position = position;
        this.reduceNumber = reducenumber;
    }
    
    public ChromosomeRegion() {
        this.reduceNumber = -1;
        this.chromosome = -1;
        this.position = -1;
    }

    @Override
    public void write(DataOutput d) throws IOException {
        d.writeInt(chromosome);
        d.writeInt(position);
        d.writeInt(reduceNumber);
    }

    @Override
    public void readFields(DataInput di) throws IOException {
        chromosome = di.readInt();
        position = di.readInt();
        reduceNumber = di.readInt();
    }

    @Override
    public int compareTo(ChromosomeRegion t) {
        /*
         * returns:
         * x < 0 if t is less than this
         * 0 if equals
         * x > 0 if t is bigger than this
         */
//        if(chromosome == t.chromosome) {
            if(reduceNumber == t.reduceNumber) 
                return position - t.position;
            else
                return reduceNumber - t.reduceNumber;
//        } else
//            return chromosome - t.chromosome;
    }

    @Override
    public String toString() {
        return "region_" + reduceNumber; 
    }
    public String toFullString() {
        return chromosome + "-" + reduceNumber + "-" + position;
    }
}
