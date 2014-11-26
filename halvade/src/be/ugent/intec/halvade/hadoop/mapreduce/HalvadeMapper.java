/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package be.ugent.intec.halvade.hadoop.mapreduce;

import be.ugent.intec.halvade.hadoop.datatypes.ChromosomeRegion;
import be.ugent.intec.halvade.tools.AlignerInstance;
import be.ugent.intec.halvade.utils.Logger;
import be.ugent.intec.halvade.utils.HalvadeConf;
import fi.tkk.ics.hadoop.bam.SAMRecordWritable;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class HalvadeMapper extends Mapper<LongWritable, Text, ChromosomeRegion, SAMRecordWritable> {
    protected int count, readcount;
    protected boolean reuseJVM;
    protected AlignerInstance instance;

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
        Logger.DEBUG(readcount + " fastq reads processed");
        Logger.DEBUG("starting cleanup");
        try {
            // check if its the last on this node, if so close it:
            // assumes the jvm is reused fully (mapred.job.reuse.jvm.num.tasks = -1)
            if (!reuseJVM || HalvadeConf.allTasksCompleted(context.getConfiguration())) {
                Logger.DEBUG("closing aligner");
                // also runs the sampe/samse in this function!
                // needs context to write output!
                instance.closeAligner();
            }
        } catch (URISyntaxException ex) {
            Logger.EXCEPTION(ex);
            throw new InterruptedException();
        }
        Logger.DEBUG("finished cleanup");
    }
    

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        if(count % 4 == 0) {
            context.getCounter(HalvadeCounters.IN_BWA_READS).increment(1);
            readcount++;
        }
        count++;
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        try {
            reuseJVM = HalvadeConf.getReuseJVM(context.getConfiguration());  
            count = 0;
            readcount = 0;
            // add a file to distributed cache representing this task
            String taskId = context.getTaskAttemptID().toString();
            Logger.DEBUG("taskId = " + taskId);
            HalvadeConf.addTaskRunning(context.getConfiguration(), taskId);
        } catch (URISyntaxException ex) {
            Logger.EXCEPTION(ex);
            throw new InterruptedException();
        }
    }
    
    protected String checkBinaries(Context context) throws IOException {
        Logger.DEBUG("Checking for binaries...");
        String binDir = null;
        URI[] localPaths = context.getCacheArchives();
        for(int i = 0; i < localPaths.length; i++ ) {
            Path path = new Path(localPaths[i].getPath());
            if(path.getName().endsWith(".tar.gz")) {
                binDir = "./" + path.getName() + "/bin/";
            }
        }
        printDirectoryTree(new File(binDir), 0);
        return binDir;
    }
    protected void printDirectoryTree(File dir, int level) {
        String whitespace = "";
        for(int i = 0; i < level; i++)
            whitespace += "\t";
        File[] list = dir.listFiles();
        if(list != null) {
            for(int i = 0; i < list.length; i++ ) {
                java.nio.file.Path path = FileSystems.getDefault().getPath(list[i].getAbsolutePath());
                String attr = "";
                if(list[i].isDirectory()) 
                    attr += "D ";
                else 
                    attr += "F ";
                if(list[i].canExecute()) 
                    attr += "E ";
                else 
                    attr += "NE ";
                if(list[i].canRead()) 
                    attr += "R ";
                else 
                    attr += "NR ";
                if(list[i].canWrite()) 
                    attr += "W ";
                else 
                    attr += "NW ";
                if(Files.isSymbolicLink(path)) 
                    attr += "S ";
                else 
                    attr += "NS ";
                    
                Logger.DEBUG(whitespace + attr + "\t" + list[i].getName());
                if(list[i].isDirectory())
                    printDirectoryTree(list[i], level + 1);
            }
        } else {
                    Logger.DEBUG(whitespace + "N");
        }
    }
}
