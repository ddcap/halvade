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
import net.sf.samtools.SAMSequenceDictionary;
import org.apache.hadoop.io.WritableComparable;

/**
 *
 * @author ddecap
 */
public class GenomeSJ implements WritableComparable<GenomeSJ> {
    protected int type; // 0 = sj string, 1 = overhang length
    protected int chromosome;
    protected int firstBaseIntron;
    protected int lastBaseIntron;
    protected int overhang;

    public void setOverhang(int overhang) {
        this.type = 1;
        this.overhang = overhang;
    }
    
    public void parseSJString(String sjString, SAMSequenceDictionary dict) {
        String columns[] = sjString.split("\t");
        this.type = 0;
        this.chromosome = dict.getSequenceIndex(columns[0]);
        this.firstBaseIntron = Integer.parseInt(columns[1]);
        this.lastBaseIntron = Integer.parseInt(columns[2]);
    }
    
    public int getType() {
        return type;
    }
    public int getOverhang() {
        return overhang;
    }
    public int getChromosome() {
        return chromosome;
    }
    public int getFirstBaseIntron() {
        return firstBaseIntron;
    }
    public int getLastBaseIntron() {
        return lastBaseIntron;
    }
    
    public GenomeSJ() {
        this.type = 0;
        this.chromosome = Integer.MAX_VALUE;
        this.firstBaseIntron = -1;
        this.lastBaseIntron = -1;
        this.overhang = -1;
    }
    
    @Override
    public void write(DataOutput d) throws IOException {
        d.writeInt(type);
        d.writeInt(chromosome);
        d.writeInt(firstBaseIntron);
        d.writeInt(lastBaseIntron);
        d.writeInt(overhang);
    }

    @Override
    public void readFields(DataInput di) throws IOException {
        type = di.readInt();
        chromosome = di.readInt();
        firstBaseIntron = di.readInt();
        lastBaseIntron = di.readInt();
        overhang = di.readInt();
    }

    @Override
    public int compareTo(GenomeSJ o) {
        if(chromosome == o.chromosome) {
            if(firstBaseIntron == o.firstBaseIntron)
                return lastBaseIntron - o.lastBaseIntron;
            else
                return firstBaseIntron - o.firstBaseIntron;
        } else 
            return chromosome - o.chromosome;
    }
    
}
