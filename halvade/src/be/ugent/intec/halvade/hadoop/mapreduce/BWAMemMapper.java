/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package be.ugent.intec.halvade.hadoop.mapreduce;

import fi.tkk.ics.hadoop.bam.SAMRecordWritable;
import fi.tkk.ics.hadoop.bam.SequencedFragment;
import be.ugent.intec.halvade.hadoop.datatypes.ChromosomeRegion;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import be.ugent.intec.halvade.tools.BWAMemInstance;
import be.ugent.intec.halvade.utils.Logger;
import be.ugent.intec.halvade.utils.MyConf;
import java.io.File;
import java.net.URISyntaxException;
import org.apache.hadoop.fs.Path;

/**
 *
 * @author ddecap
 */
// TODO use compressed SAMrecod as output
public class BWAMemMapper extends Mapper<Text, SequencedFragment, ChromosomeRegion, SAMRecordWritable> {
    private BWAMemInstance instance;
    private int count;
    private boolean reuseJVM;

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
        Logger.DEBUG(count + "fastq reads processed");
        Logger.DEBUG("starting cleanup");
        // check if its the last on this node, if so close it:
        // assumes the jvm is reused fully (mapred.job.reuse.jvm.num.tasks = -1)
        try {
            if (!reuseJVM || MyConf.allTasksCompleted(context.getConfiguration())) {
                Logger.DEBUG("closing BWA");
                instance.closeBWA();
            }
        } catch (URISyntaxException ex) {
            Logger.EXCEPTION(ex);
            throw new InterruptedException();
        }
        Logger.DEBUG("finished cleanup");
    }
    

    @Override
    protected void map(Text key, SequencedFragment value, Context context) throws IOException, InterruptedException {
        instance.feedLine("@" + key.toString());
        instance.feedLine(value.getSequence().toString());
        instance.feedLine("+"); // just write +, illumina is always +?
        instance.feedLine(value.getQuality().toString());
        count++;
        context.getCounter(HalvadeCounters.IN_BWA_READS).increment(1);
        // tell the framework we are still working
        context.progress();
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        try {
            reuseJVM = MyConf.getReuseJVM(context.getConfiguration());
            String binDir = checkBinaries(context); 
            instance = BWAMemInstance.getBWAInstance(context, binDir);
            Logger.DEBUG("bwa startup state: " + instance.getState());
            count = 0;
            // add a file to distributed cache representing this task
            String taskId = context.getTaskAttemptID().toString();
            Logger.DEBUG("taskId = " + taskId);
            MyConf.addTaskRunning(context.getConfiguration(), taskId);
        } catch (URISyntaxException ex) {
            Logger.EXCEPTION(ex);
            throw new InterruptedException();
        }
    }
    
    protected String checkBinaries(Context context) throws IOException {
        Logger.DEBUG("Checking for binaries...");
        String binDir = MyConf.getBinDir(context.getConfiguration());
        if(binDir != null) {
            return binDir;
        }
        Path[] localPath = context.getLocalCacheArchives();
        for(int i = 0; i < localPath.length; i++ ) {
            if(localPath[i].getName().equals("bin.tar.gz")) {
                binDir = localPath[i] + "/bin/";
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
                Logger.DEBUG(whitespace + attr + "\t" + list[i].getName());
                if(list[i].isDirectory())
                    printDirectoryTree(list[i], level + 1);
            }
        } else {
                    Logger.DEBUG(whitespace + "N");
        }
    }
}
