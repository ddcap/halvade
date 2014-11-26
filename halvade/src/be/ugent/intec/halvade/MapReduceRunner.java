/*
 * Copyright (C) 2014 ddecap
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package be.ugent.intec.halvade;

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
import be.ugent.intec.halvade.utils.HalvadeConf;
import be.ugent.intec.halvade.utils.Timer;
import fi.tkk.ics.hadoop.bam.VCFInputFormat;
import java.net.URI;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

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
            // initialise MapReduce - copy ref to each node??
            
            
            Job halvadeJob = Job.getInstance(halvadeConf, "Halvade");
            halvadeJob.addCacheArchive(new URI(halvadeOpts.halvadeBinaries));
            
            
            halvadeJob.setJarByClass(be.ugent.intec.halvade.hadoop.mapreduce.HalvadeMapper.class);
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
            
            if(halvadeOpts.rnaPipeline) 
                halvadeJob.setMapperClass(be.ugent.intec.halvade.hadoop.mapreduce.StarAlignMapper.class);
            else {
                if (halvadeOpts.aln) halvadeJob.setMapperClass(be.ugent.intec.halvade.hadoop.mapreduce.BWAAlnMapper.class);
                else halvadeJob.setMapperClass(be.ugent.intec.halvade.hadoop.mapreduce.BWAMemMapper.class);            }
            halvadeJob.setMapOutputKeyClass(ChromosomeRegion.class);
            halvadeJob.setMapOutputValueClass(SAMRecordWritable.class);
            halvadeJob.setInputFormatClass(TextInputFormat.class);             
            halvadeJob.setPartitionerClass(ChrRgPartitioner.class);
            halvadeJob.setSortComparatorClass(ChrRgPositionComparator.class);
            halvadeJob.setGroupingComparatorClass(ChrRgRegionComparator.class);
            
            if(halvadeOpts.justAlign)
                halvadeJob.setNumReduceTasks(0);
            else
                halvadeJob.setNumReduceTasks(halvadeOpts.reducers);            
            if(halvadeOpts.rnaPipeline)
                halvadeJob.setReducerClass(be.ugent.intec.halvade.hadoop.mapreduce.RnaGATKReducer.class);
            else
                halvadeJob.setReducerClass(be.ugent.intec.halvade.hadoop.mapreduce.DnaGATKReducer.class);            
            halvadeJob.setOutputKeyClass(Text.class);
            halvadeJob.setOutputValueClass(VariantContextWritable.class);
            
            if(halvadeOpts.dryRun) 
                return 0;
            
            Timer timer = new Timer();
            timer.start();
            ret = halvadeJob.waitForCompletion(true) ? 0 : 1;
            timer.stop();
            Logger.DEBUG("Running time of Halvade Job: " + timer);
            
            
            if(halvadeOpts.combineVcf && ret == 0) {
                Logger.DEBUG("combining output");
                Configuration combineConf = getConf();
                if(!halvadeOpts.out.endsWith("/")) halvadeOpts.out += "/";  
                HalvadeConf.setInputDir(combineConf, halvadeOpts.out);
                HalvadeConf.setOutDir(combineConf, halvadeOpts.out + "combinedVCF/");
                HalvadeConf.setReportAllVariant(combineConf, halvadeOpts.reportAll);
                Job combineJob = new Job(combineConf, "HalvadeCombineVCF");            
                combineJob.setJarByClass(be.ugent.intec.halvade.hadoop.mapreduce.VCFCombineMapper.class);

                
                try {
                    if (fs.getFileStatus(new Path(halvadeOpts.out)).isDirectory()) {
                        // add every file in directory
                        FileStatus[] files = fs.listStatus(new Path(halvadeOpts.out));
                        for(FileStatus file : files) {
                            if (!file.isDirectory() && file.getPath().getName().endsWith(".vcf")) {
                                FileInputFormat.addInputPath(combineJob, file.getPath());
                            }
                        }
                    }
                } catch (Exception e) {
                    Logger.EXCEPTION(e);
                }
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
            } else {
            }
            
            
        } catch (Exception e) {
            Logger.EXCEPTION(e);
        }
        return ret;
    }
}
