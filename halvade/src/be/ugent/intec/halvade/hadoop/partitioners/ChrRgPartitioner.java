/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package be.ugent.intec.halvade.hadoop.partitioners;

import be.ugent.intec.halvade.hadoop.datatypes.ChromosomeRegion;
import fi.tkk.ics.hadoop.bam.SAMRecordWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 *
 * @author ddecap
 */
public class ChrRgPartitioner extends Partitioner<ChromosomeRegion, SAMRecordWritable> { 
    
    @Override
    public int getPartition(ChromosomeRegion key, SAMRecordWritable value, int numReduceTasks) {
        return key.getReduceNumber() % numReduceTasks;
    }
    
}
