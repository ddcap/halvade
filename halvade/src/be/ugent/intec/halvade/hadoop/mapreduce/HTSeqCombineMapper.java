/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package be.ugent.intec.halvade.hadoop.mapreduce;

import be.ugent.intec.halvade.utils.Logger;
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
        try {
            k.set(split[0]+"\t"+split[1]+"\t"+split[2]+"\t"+split[3]+"\t"+split[4]); // gene_id contig start end strand
            v.set(Integer.parseInt(split[split.length - 1]));
            context.write(k, v);
        } catch (ArrayIndexOutOfBoundsException | NumberFormatException ex) { // ignore header lines!
            Logger.DEBUG("invalid line ignored; " + value.toString());
        }
    }
    
}