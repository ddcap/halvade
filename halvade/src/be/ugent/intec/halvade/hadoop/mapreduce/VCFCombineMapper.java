/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package be.ugent.intec.halvade.hadoop.mapreduce;

import fi.tkk.ics.hadoop.bam.VariantContextWritable;
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author ddecap
 */
public class VCFCombineMapper extends Mapper<LongWritable,VariantContextWritable, LongWritable, VariantContextWritable> {

    @Override
    protected void map(LongWritable key, VariantContextWritable value, Context context) throws IOException, InterruptedException {
        // Sort on key with chr idx + pos: key.set((long)chromIdx << 32 | (long)(v.getStart() - 1));
        context.write(key, value);
    }
    
}
