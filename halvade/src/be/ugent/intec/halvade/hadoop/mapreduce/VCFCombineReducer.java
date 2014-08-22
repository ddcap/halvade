/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package be.ugent.intec.halvade.hadoop.mapreduce;

import be.ugent.intec.halvade.utils.Logger;
import be.ugent.intec.halvade.utils.MyConf;
import fi.tkk.ics.hadoop.bam.KeyIgnoringVCFOutputFormat;
import fi.tkk.ics.hadoop.bam.VCFFormat;
import fi.tkk.ics.hadoop.bam.VariantContextWritable;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Iterator;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author ddecap
 */
public class VCFCombineReducer extends Reducer<LongWritable, VariantContextWritable, LongWritable, VariantContextWritable> {
    
    KeyIgnoringVCFOutputFormat outpFormat;
    RecordWriter<LongWritable,VariantContextWritable> recordWriter;
    VariantContextWritable tmpVar;
    VariantContextWritable bestVar;
    boolean reportBest = false;

    @Override
    public void run(Context context) throws IOException, InterruptedException {
        super.run(context); //To change body of generated methods, choose Tools | Templates.
        recordWriter.close(context);
    }

    @Override
    protected void reduce(LongWritable key, Iterable<VariantContextWritable> values, Context context) throws IOException, InterruptedException {
        Iterator<VariantContextWritable> it = values.iterator();
        // find vcf with best quality
        if(reportBest) {
            if(it.hasNext())
                bestVar = it.next();
            while(it.hasNext()){
                tmpVar = it.next();
                if(bestVar.get().getPhredScaledQual() < tmpVar.get().getPhredScaledQual())
                    bestVar = tmpVar;            
            }
//            context.write(key, bestVar);
            recordWriter.write(key, bestVar);
        } else {
            while(it.hasNext()){
//                context.write(key, it.next());   
                recordWriter.write(key, it.next());
            }
        }
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        try {
            // read header from input
            outpFormat = new KeyIgnoringVCFOutputFormat(VCFFormat.VCF);
            String input = MyConf.getInputDir(context.getConfiguration());
            String output = MyConf.getOutDir(context.getConfiguration());
            FileSystem fs = FileSystem.get(new URI(input), context.getConfiguration());
            Path firstVcfFile = null;
            if (fs.getFileStatus(new Path(input)).isDirectory()) {
                // get first file
                FileStatus[] files = fs.listStatus(new Path(input));
                if(files.length > 0) {
                    firstVcfFile = files[0].getPath();
                } else {
                    throw new InterruptedException("VCFCombineReducer: No files in input folder.");
                }
            } else {
                throw new InterruptedException("VCFCombineReducer: Input directory is not a directory.");
            }
            outpFormat.readHeaderFrom(firstVcfFile, fs);
            recordWriter = outpFormat.getRecordWriter(context, new Path(output + "HalvadeCombined.vcf"));
        } catch (URISyntaxException ex) {
            Logger.EXCEPTION(ex);
            throw new InterruptedException("URI for input directory is invalid.");
        }
    }
    
}
