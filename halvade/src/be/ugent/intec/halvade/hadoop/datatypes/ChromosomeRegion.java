/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
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
    protected int region;
    protected int position;
    protected int reduceNumber;

    public int getChromosome() {
        return chromosome;
    }

    public int getPosition() {
        return position;
    }

    public int getRegion() {
        return region;
    }
    
    public int getReduceNumber() {
        return reduceNumber;
    }

    public void setChromosomeRegion(int chromosome, int region, int position, int reducenumber) {
        this.chromosome = chromosome;
        this.position = position;
        this.region = region;
        this.reduceNumber = reducenumber;
    }
    
    public ChromosomeRegion() {
        this.region = -1;
        this.chromosome = -1;
        this.position = -1;
    }
    
    public ChromosomeRegion(int chromosome, int region, int position, int reducenumber) {
        this.region = region;
        this.chromosome = chromosome;
        this.position = position;
        this.reduceNumber = reducenumber;
    }

    @Override
    public void write(DataOutput d) throws IOException {
        d.writeInt(chromosome);
        d.writeInt(region);
        d.writeInt(position);
        d.writeInt(reduceNumber);
    }

    @Override
    public void readFields(DataInput di) throws IOException {
        chromosome = di.readInt();
        region = di.readInt();
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
        if(chromosome == t.chromosome) {
            if(region == t.region) 
                return position - t.position;
            else
                return region - t.region;
        } else
            return chromosome - t.chromosome;
    }

    @Override
    public String toString() {
        return chromosome + "-" + region + "-";
    }
    public String toFullString() {
        return chromosome + "-" + region + "-" + position + "-" + reduceNumber;
    }
}
