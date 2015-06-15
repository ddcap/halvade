/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package be.ugent.intec.halvade.hadoop.mapreduce;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author ddecap
 */
public class HTSeqCombineMapper extends Mapper<LongWritable,Text, Text, LongWritable> {
    private Text k = new Text();
    private LongWritable v = new LongWritable();
    
    @Override
    protected void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split("\t");
        k.set(split[0]);
        v.set(Integer.parseInt(split[1]));
        context.write(k, v);
    }
    
}