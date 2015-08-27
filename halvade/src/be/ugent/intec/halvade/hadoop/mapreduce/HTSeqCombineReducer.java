/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package be.ugent.intec.halvade.hadoop.mapreduce;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author ddecap
 */
public class HTSeqCombineReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
    LongWritable val = new LongWritable();

    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        Iterator<LongWritable> it = values.iterator();
        long t = 0;
        while(it.hasNext()){
            t = Math.max(t,it.next().get()); 
        }
        val.set(t);
        context.write(key, val);
    }
}
