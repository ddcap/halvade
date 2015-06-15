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
import htsjdk.samtools.SAMSequenceDictionary;
import org.apache.hadoop.io.WritableComparable;

/**
 *
 * @author ddecap
 */
public class GenomeSJ implements WritableComparable<GenomeSJ> {
    protected int type; // -2 = overhang length, -1 = sj string, 2 =  count per key region
    protected int secondary_key;

    public void setOverhang(int overhang) {
        this.type = -2;
        this.secondary_key = overhang;
    }
    
    public void parseSJString(String sjString, SAMSequenceDictionary dict) {
        String columns[] = sjString.split("\t");
        this.type = -1;
        this.secondary_key = dict.getSequenceIndex(columns[0]);
    }
    
    public void setRegion(int key, int pos) {
        this.type = key;
        this.secondary_key = pos;
    }
    
    public int getType() {
        return type;
    }
    public int getSecKey() {
        return secondary_key;
    }
    
    public GenomeSJ() {
        this.type = 0;
        this.secondary_key = -1;
    }
    
    @Override
    public void write(DataOutput d) throws IOException {
        d.writeInt(type);
        d.writeInt(secondary_key);
    }

    @Override
    public String toString() {
        return "GenomeSJ{" + "1=" + type + ", 2=" + secondary_key + '}';
    }

    
    @Override
    public void readFields(DataInput di) throws IOException {
        type = di.readInt();
        secondary_key = di.readInt();
    }

    @Override
    public int compareTo(GenomeSJ o) {
        if(type == o.type) {
            return secondary_key - o.secondary_key;
        } else 
            return type - o.type;
    }
    
}
