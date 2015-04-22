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
import be.ugent.intec.halvade.hadoop.datatypes.GenomeSJ;
import be.ugent.intec.halvade.hadoop.mapreduce.HalvadeTextInputFormat;
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
import fi.tkk.ics.hadoop.bam.BAMInputFormat;
import fi.tkk.ics.hadoop.bam.VCFInputFormat;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;

/**
 *
 * @author ddecap
 */
public class MapReduceRunner extends Configured implements Tool  {
    protected final String RNA_PASS2 = " pass 2 RNA job";
    protected final String RNA = " RNA job";
    protected final String DNA = " DNA job";
    protected HalvadeOptions halvadeOpts;
            
    @Override
    public int run(String[] strings) throws Exception {
        int ret = 0;
        try {
            Configuration halvadeConf = getConf();
            halvadeOpts = new HalvadeOptions();
            int optReturn = halvadeOpts.GetOptions(strings, halvadeConf);
            if (optReturn != 0) return optReturn;
            
            String halvadeDir = halvadeOpts.out + "/halvade";
            if(!halvadeOpts.justCombine) {
                if(halvadeOpts.rnaPipeline) {
                    if(halvadeOpts.useSharedMemory && !halvadeOpts.useBamInput) {
                        ret = runPass1RNAJob(halvadeConf, halvadeOpts.out + "/pass1");
                        if(ret != 0) {
                            Logger.DEBUG("Halvade pass 1 job failed.");
                            System.exit(-1);
                        }
                        HalvadeConf.setIsPass2(halvadeConf, true);
                        ret = runHalvadeJob(halvadeConf, halvadeDir, HalvadeResourceManager.RNA_SHMEM_PASS2);
                    } else {
                        ret = runHalvadeJob(halvadeConf, halvadeDir, HalvadeResourceManager.RNA);
                    }
                } else {
                    ret = runHalvadeJob(halvadeConf, halvadeDir, HalvadeResourceManager.DNA);
                }
                if(ret != 0) {
                    Logger.DEBUG("Halvade job failed.");
                    System.exit(-2);
                }
            }
            if(halvadeOpts.combineVcf) 
                runCombineJob(halvadeDir, halvadeOpts.out + "/merge");
        } catch (IOException | ClassNotFoundException | IllegalArgumentException | IllegalStateException | InterruptedException | URISyntaxException e) {
            Logger.EXCEPTION(e);
        }
        return ret;
    }
    
    protected int runPass1RNAJob(Configuration pass1Conf, String tmpOutDir) throws IOException, InterruptedException, ClassNotFoundException, URISyntaxException {
        HalvadeConf.setIsPass2(pass1Conf, false);
        HalvadeResourceManager.setJobResources(halvadeOpts, pass1Conf, HalvadeResourceManager.RNA_SHMEM_PASS1, true);
        Job pass1Job = Job.getInstance(pass1Conf, "Halvade pass 1 RNA pipeline");
        pass1Job.addCacheArchive(new URI(halvadeOpts.halvadeBinaries));
        pass1Job.setJarByClass(be.ugent.intec.halvade.hadoop.mapreduce.HalvadeMapper.class);
        FileSystem fs = FileSystem.get(new URI(halvadeOpts.in), pass1Conf);
        try {
            if (fs.getFileStatus(new Path(halvadeOpts.in)).isDirectory()) {
                // add every file in directory
                FileStatus[] files = fs.listStatus(new Path(halvadeOpts.in));
                for(FileStatus file : files) {
                    if (!file.isDirectory()) {
                        FileInputFormat.addInputPath(pass1Job, file.getPath());
                    }
                }
            } else {
                FileInputFormat.addInputPath(pass1Job, new Path(halvadeOpts.in));
            }
        } catch (IOException | IllegalArgumentException e) {
            Logger.EXCEPTION(e);
        }

        FileSystem outFs = FileSystem.get(new URI(tmpOutDir), pass1Conf);
        boolean skipPass1 = false;
        if (outFs.exists(new Path(tmpOutDir))) {
            // check if genome already exists
            skipPass1 = outFs.exists(new Path(tmpOutDir + "/_SUCCESS"));
            if(skipPass1)
                Logger.DEBUG("pass1 genome already created, skipping pass 1");
            else {
                Logger.INFO("The output directory \'" + tmpOutDir + "\' already exists.");
                Logger.INFO("ERROR: Please remove this directory before trying again.");
                System.exit(-2);
            }
        }
        if(!skipPass1) {
            FileOutputFormat.setOutputPath(pass1Job, new Path(tmpOutDir));
            pass1Job.setMapperClass(be.ugent.intec.halvade.hadoop.mapreduce.StarAlignPassXMapper.class);

            pass1Job.setInputFormatClass(HalvadeTextInputFormat.class);
            pass1Job.setMapOutputKeyClass(GenomeSJ.class);
            pass1Job.setMapOutputValueClass(Text.class);

            pass1Job.setSortComparatorClass(GenomeSJSortComparator.class);
            pass1Job.setGroupingComparatorClass(GenomeSJGroupingComparator.class);
            pass1Job.setNumReduceTasks(1); 
            pass1Job.setReducerClass(be.ugent.intec.halvade.hadoop.mapreduce.RebuildStarGenomeReducer.class);          
            pass1Job.setOutputKeyClass(LongWritable.class);
            pass1Job.setOutputValueClass(Text.class);

            return runTimedJob(pass1Job, "Halvade pass 1 Job");
        } else
            return 0;
    }
    
    protected int runHalvadeJob(Configuration halvadeConf, String tmpOutDir, int jobType) throws IOException, URISyntaxException, InterruptedException, ClassNotFoundException {
        String pipeline = "";
        if(jobType == HalvadeResourceManager.RNA_SHMEM_PASS2) {
            HalvadeConf.setIsPass2(halvadeConf, true);
            HalvadeResourceManager.setJobResources(halvadeOpts, halvadeConf, jobType, false);
            pipeline = RNA_PASS2;
        } else if(jobType == HalvadeResourceManager.RNA) {
            HalvadeResourceManager.setJobResources(halvadeOpts, halvadeConf, jobType, false);
            pipeline = RNA;
        } else if(jobType == HalvadeResourceManager.DNA) {
            HalvadeResourceManager.setJobResources(halvadeOpts, halvadeConf, jobType, false);
            pipeline = DNA; 
        }
        HalvadeConf.setOutDir(halvadeConf, tmpOutDir);
        FileSystem outFs = FileSystem.get(new URI(tmpOutDir), halvadeConf);
        if (outFs.exists(new Path(tmpOutDir))) {
            Logger.INFO("The output directory \'" + tmpOutDir + "\' already exists.");
            Logger.INFO("ERROR: Please remove this directory before trying again.");
            System.exit(-2);
        }
        
        Job halvadeJob = Job.getInstance(halvadeConf, "Halvade" + pipeline);
        halvadeJob.addCacheArchive(new URI(halvadeOpts.halvadeBinaries));
        halvadeJob.setJarByClass(be.ugent.intec.halvade.hadoop.mapreduce.HalvadeMapper.class);
        addInputFiles(halvadeOpts.in, halvadeConf, halvadeJob);
        FileOutputFormat.setOutputPath(halvadeJob, new Path(tmpOutDir));

        if(jobType == HalvadeResourceManager.RNA_SHMEM_PASS2) {
            halvadeJob.setMapperClass(be.ugent.intec.halvade.hadoop.mapreduce.StarAlignPassXMapper.class);
            halvadeJob.setReducerClass(be.ugent.intec.halvade.hadoop.mapreduce.RnaGATKReducer.class);
        } else if(jobType == HalvadeResourceManager.RNA) {
            halvadeJob.setMapperClass(be.ugent.intec.halvade.hadoop.mapreduce.StarAlignMapper.class);
            halvadeJob.setReducerClass(be.ugent.intec.halvade.hadoop.mapreduce.RnaGATKReducer.class);
        } else if(jobType == HalvadeResourceManager.DNA){ 
            if (halvadeOpts.aln) halvadeJob.setMapperClass(be.ugent.intec.halvade.hadoop.mapreduce.BWAAlnMapper.class);
            else halvadeJob.setMapperClass(be.ugent.intec.halvade.hadoop.mapreduce.BWAMemMapper.class);
            halvadeJob.setReducerClass(be.ugent.intec.halvade.hadoop.mapreduce.DnaGATKReducer.class);  
        }
        
        if(halvadeOpts.justAlign)
            halvadeJob.setNumReduceTasks(0);
        else
            halvadeJob.setNumReduceTasks(halvadeOpts.reduces);    
        
        halvadeJob.setMapOutputKeyClass(ChromosomeRegion.class);
        halvadeJob.setMapOutputValueClass(SAMRecordWritable.class);
        halvadeJob.setInputFormatClass(HalvadeTextInputFormat.class);
        halvadeJob.setPartitionerClass(ChrRgPartitioner.class);
        halvadeJob.setSortComparatorClass(ChrRgSortComparator.class);
        halvadeJob.setGroupingComparatorClass(ChrRgGroupingComparator.class);
        halvadeJob.setOutputKeyClass(Text.class);
        halvadeJob.setOutputValueClass(VariantContextWritable.class);

        if(halvadeOpts.useBamInput) {
            halvadeJob.setMapperClass(be.ugent.intec.halvade.hadoop.mapreduce.AlignedBamMapper.class);
            halvadeJob.setInputFormatClass(BAMInputFormat.class);
        }
        
        return runTimedJob(halvadeJob, "Halvade Job");
    }
    
    protected int runCombineJob(String halvadeOutDir, String mergeOutDir) throws IOException, URISyntaxException, InterruptedException, ClassNotFoundException {
        Configuration combineConf = getConf();
        if(!halvadeOpts.out.endsWith("/")) halvadeOpts.out += "/";  
        HalvadeConf.setInputDir(combineConf, halvadeOutDir);
        HalvadeConf.setOutDir(combineConf, mergeOutDir);
        FileSystem outFs = FileSystem.get(new URI(mergeOutDir), combineConf);
        if (outFs.exists(new Path(mergeOutDir))) {
            Logger.INFO("The output directory \'" + mergeOutDir + "\' already exists.");
            Logger.INFO("ERROR: Please remove this directory before trying again.");
            System.exit(-2);
        }
        HalvadeConf.setReportAllVariant(combineConf, halvadeOpts.reportAll);
        HalvadeResourceManager.setJobResources(halvadeOpts, combineConf, HalvadeResourceManager.COMBINE, false);
        Job combineJob = Job.getInstance(combineConf, "HalvadeCombineVCF");            
        combineJob.setJarByClass(be.ugent.intec.halvade.hadoop.mapreduce.VCFCombineMapper.class);

        addInputFiles(halvadeOutDir, combineConf, combineJob, ".vcf");
        FileOutputFormat.setOutputPath(combineJob, new Path(mergeOutDir));

        combineJob.setMapperClass(be.ugent.intec.halvade.hadoop.mapreduce.VCFCombineMapper.class);
        combineJob.setMapOutputKeyClass(LongWritable.class);
        combineJob.setMapOutputValueClass(VariantContextWritable.class);
        combineJob.setInputFormatClass(VCFInputFormat.class);
        combineJob.setNumReduceTasks(1); 
        combineJob.setReducerClass(be.ugent.intec.halvade.hadoop.mapreduce.VCFCombineReducer.class);
        combineJob.setOutputKeyClass(Text.class);
        combineJob.setOutputValueClass(VariantContextWritable.class);

        return runTimedJob(combineJob, "Combine Job");
    }
    
    protected int runTimedJob(Job job, String jobname) throws IOException, InterruptedException, ClassNotFoundException {
        if(halvadeOpts.dryRun) 
            return 0;
        Logger.DEBUG("Started " + jobname);
        Timer timer = new Timer();
        timer.start();
        int ret = job.waitForCompletion(true) ? 0 : 1;
        timer.stop();
        Logger.DEBUG("Finished " + jobname + " [runtime: " + timer.getFormattedElapsedTime() + "]");
        return ret;
    }
    
    protected void addInputFiles(String input, Configuration conf, Job job) throws URISyntaxException, IOException {
        FileSystem fs = FileSystem.get(new URI(input), conf);
        if (fs.getFileStatus(new Path(input)).isDirectory()) {
            // add every file in directory
            FileStatus[] files = fs.listStatus(new Path(input));
            for(FileStatus file : files) {
                if (!file.isDirectory()) {
                    FileInputFormat.addInputPath(job, file.getPath());
                }
            }
        } else {
            FileInputFormat.addInputPath(job, new Path(input));
        }
    }
    
    protected void addInputFiles(String input, Configuration conf, Job job, String filter) throws URISyntaxException, IOException {
        FileSystem fs = FileSystem.get(new URI(input), conf);
        if (fs.getFileStatus(new Path(input)).isDirectory()) {
            // add every file in directory
            FileStatus[] files = fs.listStatus(new Path(input));
            for(FileStatus file : files) {
                if (!file.isDirectory() && file.getPath().getName().endsWith(filter)) {
                    FileInputFormat.addInputPath(job, file.getPath());
                }
            }
        } else {
            FileInputFormat.addInputPath(job, new Path(input));
        }
    }
    
}
