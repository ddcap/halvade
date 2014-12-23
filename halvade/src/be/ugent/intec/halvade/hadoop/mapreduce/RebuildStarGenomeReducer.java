/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package be.ugent.intec.halvade.hadoop.mapreduce;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author ddecap
 */
public class RebuildStarGenomeReducer extends Reducer<LongWritable, Text, LongWritable, Text> {
    protected String tmpDir;
    protected BufferedWriter bw;

    @Override
    protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Iterator<Text> it = values.iterator();
        while(it.hasNext()) {
            bw.write(it.next().toString());
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        bw.close();
        // build new genome ref
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        File file = new File(tmpDir + "mergedSJ.out.tab");
        if (!file.exists()) {
                file.createNewFile();
        }
        bw = new BufferedWriter(new FileWriter(file.getAbsoluteFile()));
    }
    
    
}
