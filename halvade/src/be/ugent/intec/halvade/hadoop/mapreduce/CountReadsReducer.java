/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package be.ugent.intec.halvade.hadoop.mapreduce;

import be.ugent.intec.halvade.hadoop.datatypes.ChromosomeRegion;
import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.seqdoop.hadoop_bam.SAMRecordWritable;

/**
 *
 * @author ddecap
 */
public class CountReadsReducer extends Reducer<ChromosomeRegion, SAMRecordWritable, Text, LongWritable> {
    private Text outkey = new Text();
    private LongWritable value = new LongWritable();
    
    @Override
    protected void reduce(ChromosomeRegion key, Iterable<SAMRecordWritable> values, Context context) throws IOException, InterruptedException {
        long count = 0;
        SAMRecordWritable samrecord;
        Iterator<SAMRecordWritable> it = values.iterator();
        while (it.hasNext()) {
            samrecord = it.next();
            count++;
        }
        outkey.set(key.toFullString());
        value.set(count);
        context.write(outkey, value);
    }
    
}
