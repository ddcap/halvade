/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package be.ugent.intec.halvade.hadoop.mapreduce;

import be.ugent.intec.halvade.tools.STARInstance;
import be.ugent.intec.halvade.utils.HDFSFileIO;
import be.ugent.intec.halvade.utils.HalvadeConf;
import be.ugent.intec.halvade.utils.Logger;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.util.Iterator;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author ddecap
 */
public class RebuildStarGenomeReducer extends Reducer<LongWritable, Text, LongWritable, Text> {
    protected String tmpDir;
    protected String mergeJS;
    protected BufferedWriter bw;
    protected int count;
    protected String bin, ref;
    protected String taskId;
    protected int overhang = 101, threads;

    @Override
    protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Iterator<Text> it = values.iterator();
        if(key.get() == 0) {
            while(it.hasNext()) {
                bw.write(it.next().toString() + "\n");
                count++;
            }
        } else if (key.get() == 1) {
            if(it.hasNext()) {
                String str = it.next().toString();
                Logger.DEBUG("test: " + str);
                overhang = Integer.parseInt(str);
                Logger.DEBUG("set overhang to " + overhang);
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        bw.close();
        Logger.DEBUG("written " + count + " lines to " + mergeJS);
        // build new genome ref
        String newGenomeDir = tmpDir + taskId + "-newSTARgenome";
        File starOut = new File(newGenomeDir);
        starOut.mkdirs();
        long time = STARInstance.rebuildStarGenome(bin, newGenomeDir, ref, mergeJS, 
                                                    overhang, threads);
        context.getCounter(HalvadeCounters.TIME_STAR_BUILD).increment(time);
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        tmpDir = HalvadeConf.getScratchTempDir(context.getConfiguration());
        taskId = context.getTaskAttemptID().toString();
        taskId = taskId.substring(taskId.indexOf("r_"));
        mergeJS = tmpDir + taskId + "-SJ.out.tab";
        File file = new File(mergeJS);
        if (!file.exists()) {
                file.createNewFile();
        }
        bw = new BufferedWriter(new FileWriter(file.getAbsoluteFile()));
        Logger.DEBUG("opened file write for " + mergeJS);
        
        threads = HalvadeConf.getReducerThreads(context.getConfiguration());
        bin = checkBinaries(context);
        try {
            ref = HDFSFileIO.downloadGATKIndex(context, taskId);
        } catch (URISyntaxException ex) {
            Logger.EXCEPTION(ex);
            throw new InterruptedException();
        }
    }
    
    protected String checkBinaries(Reducer.Context context) throws IOException {
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
