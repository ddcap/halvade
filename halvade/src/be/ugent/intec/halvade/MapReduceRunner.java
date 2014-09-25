/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package be.ugent.intec.halvade;

import fi.tkk.ics.hadoop.bam.FastqInputFormat;
import fi.tkk.ics.hadoop.bam.SAMRecordWritable;
import fi.tkk.ics.hadoop.bam.VariantContextWritable;
import be.ugent.intec.halvade.hadoop.datatypes.ChromosomeRegion;
import be.ugent.intec.halvade.hadoop.partitioners.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import be.ugent.intec.halvade.utils.Logger;
import be.ugent.intec.halvade.utils.MyConf;
import be.ugent.intec.halvade.utils.Timer;
import fi.tkk.ics.hadoop.bam.VCFInputFormat;
import java.net.URI;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;

/**
 *
 * @author ddecap
 */
public class MapReduceRunner extends Configured implements Tool  {
    HalvadeOptions halvadeOpts;
            
    @Override
    public int run(String[] strings) throws Exception {
        int ret = 1;
        try {
            Configuration halvadeConf = getConf();
            halvadeOpts = new HalvadeOptions();
            int optR = halvadeOpts.GetOptions(strings, halvadeConf);
            if (optR != 0) return optR;
            // initialise MapReduce
            
            
            // only put files or continue?
            if(halvadeOpts.justPut)
                return 0;
            
            
            Job halvadeJob = new Job(halvadeConf, "Halvade");
            // add to dist cache with job
            halvadeJob.addCacheArchive(new URI(halvadeOpts.halvadeDir + "bin.tar.gz"));   
            
            
            halvadeJob.setJarByClass(be.ugent.intec.halvade.hadoop.mapreduce.BWAMemMapper.class);
            // specify input and output dirs
            // check if input is a file or directory
            
            FileSystem fs = FileSystem.get(new URI(halvadeOpts.in), halvadeConf);
            try {
                if (fs.getFileStatus(new Path(halvadeOpts.in)).isDirectory()) {
                    // add every file in directory
                    FileStatus[] files = fs.listStatus(new Path(halvadeOpts.in));
                    for(FileStatus file : files) {
                        if (!file.isDirectory()) {
                            FileInputFormat.addInputPath(halvadeJob, file.getPath());
                        }
                    }
                } else {
                    FileInputFormat.addInputPath(halvadeJob, new Path(halvadeOpts.in));
                }
                
            } catch (Exception e) {
                Logger.EXCEPTION(e);
            }
            FileOutputFormat.setOutputPath(halvadeJob, new Path(halvadeOpts.out));
            
            // specify a mapper       
            if (halvadeOpts.aln) halvadeJob.setMapperClass(be.ugent.intec.halvade.hadoop.mapreduce.BWAAlnMapper.class);
            else halvadeJob.setMapperClass(be.ugent.intec.halvade.hadoop.mapreduce.BWAMemMapper.class);
            halvadeJob.setMapOutputKeyClass(ChromosomeRegion.class);
            halvadeJob.setMapOutputValueClass(SAMRecordWritable.class);
            halvadeJob.setInputFormatClass(FastqInputFormat.class);
            
            // per chromosome && region
            halvadeJob.setPartitionerClass(ChrRgPartitioner.class);
            halvadeJob.setSortComparatorClass(ChrRgPositionComparator.class);
            halvadeJob.setGroupingComparatorClass(ChrRgRegionComparator.class);
            
            // # reducers
            if(halvadeOpts.justAlign)
                halvadeJob.setNumReduceTasks(0);
            else
                halvadeJob.setNumReduceTasks(halvadeOpts.reducers);
            // specify a reducer
            halvadeJob.setReducerClass(be.ugent.intec.halvade.hadoop.mapreduce.GATKReducer.class);
            halvadeJob.setOutputKeyClass(Text.class);
            halvadeJob.setOutputValueClass(VariantContextWritable.class);
//            job.setOutputFormatClass(VCFOutputFormat.class);
            
            Timer timer = new Timer();
            timer.start();
            ret = halvadeJob.waitForCompletion(true) ? 0 : 1;
            timer.stop();
            Logger.DEBUG("Running time of Halvade Job: " + timer);
            
            
            if(halvadeOpts.combineVcf) {
                /**
                 * combine resulting files:
                 */
                Logger.DEBUG("combining output");            
                Configuration combineConf = getConf();  
                if(!halvadeOpts.out.endsWith("/")) halvadeOpts.out += "/";  
                MyConf.setInputDir(combineConf, halvadeOpts.out);
                MyConf.setOutDir(combineConf, halvadeOpts.out + "combinedVCF/");
                Job combineJob = new Job(combineConf, "HalvadeCombineVCF");            
                combineJob.setJarByClass(be.ugent.intec.halvade.hadoop.mapreduce.VCFCombineMapper.class);

                FileInputFormat.addInputPath(combineJob, new Path(halvadeOpts.out));
                FileOutputFormat.setOutputPath(combineJob, new Path(halvadeOpts.out + "combinedVCF/"));

                combineJob.setMapperClass(be.ugent.intec.halvade.hadoop.mapreduce.VCFCombineMapper.class);
                combineJob.setMapOutputKeyClass(LongWritable.class);
                combineJob.setMapOutputValueClass(VariantContextWritable.class);
                combineJob.setInputFormatClass(VCFInputFormat.class);
                combineJob.setNumReduceTasks(1); 
                combineJob.setReducerClass(be.ugent.intec.halvade.hadoop.mapreduce.VCFCombineReducer.class);
                combineJob.setOutputKeyClass(Text.class);
                combineJob.setOutputValueClass(VariantContextWritable.class);
    //            combineJob.setOutputFormatClass(KeyIgnoringVCFOutputFormat.class);

                timer = new Timer();
                timer.start();
                ret = combineJob.waitForCompletion(true) ? 0 : 1;
                timer.stop();
                Logger.DEBUG("Running time of Combine Job: " + timer);
            }
            
            
        } catch (Exception e) {
            Logger.EXCEPTION(e);
        }
        return ret;
    }
}
