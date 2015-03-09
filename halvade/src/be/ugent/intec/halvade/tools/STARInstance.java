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

package be.ugent.intec.halvade.tools;

import be.ugent.intec.halvade.hadoop.datatypes.GenomeSJ;
import be.ugent.intec.halvade.hadoop.mapreduce.HalvadeCounters;
import be.ugent.intec.halvade.utils.CommandGenerator;
import be.ugent.intec.halvade.utils.HalvadeFileUtils;
import be.ugent.intec.halvade.utils.Logger;
import be.ugent.intec.halvade.utils.HalvadeConf;
import be.ugent.intec.halvade.utils.ProcessBuilderWrapper;
import be.ugent.intec.halvade.utils.SAMStreamHandler;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.util.logging.Level;
import net.sf.samtools.SAMSequenceDictionary;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;

/**
 *
 * @author ddecap
 */
public class STARInstance extends AlignerInstance {
    public static int PASS1 = 1;
    public static int PASS2 = 2;
    public static int PASS1AND2 = 3;
    private static STARInstance instance;
    private ProcessBuilderWrapper star;
    private SAMStreamHandler ssh;
    private BufferedWriter fastqFile1;
    private BufferedWriter fastqFile2;
    private String taskId;
    private String starOutDir;
    private int overhang, nReads;
    private int starType;
    
    private STARInstance(Mapper.Context context, String bin, int starType) throws IOException, URISyntaxException {
        super(context, bin);
        this.starType = starType;
        taskId = context.getTaskAttemptID().toString();
        taskId = taskId.substring(taskId.indexOf("m_"));
        ref = HalvadeFileUtils.downloadSTARIndex(context, taskId, starType == PASS2);
        starOutDir = tmpdir + taskId + "-STARout/";
        nReads = 0;
        overhang = 0;
    }
    
    public int feedLine(String line, int count, int read) throws IOException, InterruptedException  {
        if(read == 1 || !isPaired) {
            fastqFile1.write(line + "\n");
//            return feedLine(line, star);
        } else if (read == 0) {
            fastqFile2.write(line + "\n");
//            return feedLine(line, star);
        }
        if(count % 2 == 0) {
            int possibleOverhang = line.length() - 1;
            if(possibleOverhang > overhang)
                overhang = possibleOverhang; // overhang == max read length - 1;
        }
        nReads++;
        return 0;
    }
    
    public String getGenomeDir() {
        return ref;
    }
    
    public int getOverhang() {
        return overhang;
    }
    
    protected String getFileName(String dir, String id, int read) {
        String outFile = dir;
        if(read == 1) {
            outFile += read1File + id + ".fastq";
        } else if (read == 2) {
            outFile += read2File + id + ".fastq";
        } else return null;
        return outFile;
    }

    public static AlignerInstance getSTARInstance(Mapper.Context context, String bin, int starType) throws URISyntaxException, IOException, InterruptedException {
        if(instance == null) {
            Logger.DEBUG("STAR instance type: " + starType);
            instance = new STARInstance(context, bin, starType);
            instance.startAligner(context);
        }
        BWAAlnInstance.context = context;
        Logger.DEBUG("Started STAR");
        return instance;
    }

    @Override
    public void flushStream() {
        try {
            // close the input stream
            if(star != null)
                star.getSTDINWriter().flush();
        } catch (IOException ex) {
            Logger.EXCEPTION(ex);
        }
    }

    @Override
    public int getState() {
        return star.getState();
    }

    @Override
    public InputStream getSTDOUTStream() {
        return star.getSTDOUTStream(); 
    }

    @Override
    public void closeAligner() throws InterruptedException {
        try {
            // close both fastq files          
            fastqFile1.close();
            if(isPaired) {
                fastqFile2.close();
            }
        } catch (IOException ex) {
            Logger.EXCEPTION(ex);
            throw new ProcessException("STAR fastq files", -1);
        }
        
        // make command
        String customArgs = HalvadeConf.getCustomArgs(context.getConfiguration(), "star", "");
        String[] command = CommandGenerator.starAlign(bin, starType, ref, starOutDir,  
                getFileName(tmpdir, taskId, 1), getFileName(tmpdir, taskId, 2), 
                threads, overhang, nReads / 4, customArgs);
        star = new ProcessBuilderWrapper(command, bin);
        // run command
        // needs to be streamed to output otherwise the process blocks ...
        star.startProcess(null, System.err);
        // check if alive
        if(!star.isAlive())
            throw new ProcessException("STAR aligner", star.getExitState());
        if(starType == PASS2 || starType == PASS1AND2) {
            // make a SAMstream handler
            ssh = new SAMStreamHandler(instance, context);
            ssh.start();
        }
        
        // send a heartbeat every min its still running -> avoid timeout
        HalvadeHeartBeat hhb = new HalvadeHeartBeat(context);
        hhb.start();
        
        if(starType == PASS2 || starType == PASS1AND2)
            ssh.join();
        int error = star.waitForCompletion();
        hhb.jobFinished();
        hhb.join();
        if(error != 0)
            throw new ProcessException("STAR aligner", error);
        context.getCounter(HalvadeCounters.TIME_STAR).increment(star.getExecutionTime());
        if (starType == PASS1) {
            emitJSFile(starOutDir, context);
        }
        //remove all temporary fastq/sai files
        HalvadeFileUtils.removeLocalFile(keep, getFileName(tmpdir, taskId, 1), context, HalvadeCounters.FOUT_STAR_TMP);
        HalvadeFileUtils.removeLocalFile(keep, getFileName(tmpdir, taskId, 2), context, HalvadeCounters.FOUT_STAR_TMP);
        // delete star tmp/out dirs
        HalvadeFileUtils.removeLocalDir(keep, starOutDir, context, HalvadeCounters.FOUT_STAR_TMP);
        instance = null;
    }

    protected Text val;
    protected GenomeSJ sj;
    private void emitJSFile(String starOutDir, Mapper.Context context) throws InterruptedException {
        SAMSequenceDictionary dict = null;
        try {
            dict = HalvadeConf.getSequenceDictionary(context.getConfiguration());
        } catch (IOException ex) {
            Logger.EXCEPTION(ex);
            throw new InterruptedException("Error getting the SAMSequenceDictionary for SJ processing");
        }
        BufferedReader br = null;
        val = new Text();
        sj = new GenomeSJ();
        try {
            br = new BufferedReader(new FileReader(starOutDir + "/SJ.out.tab"));
            String line = br.readLine();
            sj.parseSJString(line, dict);
            while (line != null) {
                val.set(line);
                context.write(sj, val);
                line = br.readLine();
            }
        } catch (IOException | InterruptedException ex) {
            Logger.EXCEPTION(ex);
        } finally {
            if(br != null) {
                try {
                    br.close();
                } catch (IOException ex) {
                    Logger.EXCEPTION(ex);
                }
            }
        }
    }

    @Override
    protected void startAligner(Mapper.Context context) throws IOException, InterruptedException {
        File file1 = new File(getFileName(tmpdir, taskId, 1));
        if (!file1.exists()) {
            file1.createNewFile();
        }
        fastqFile1 = new BufferedWriter(new FileWriter(file1.getAbsoluteFile()));
        if(isPaired) {
            File file2 = new File(getFileName(tmpdir, taskId, 2));
            if (!file2.exists()) {
                    file2.createNewFile();
            }
            fastqFile2 = new BufferedWriter(new FileWriter(file2.getAbsoluteFile()));
        }
        // make output dir!
        File starOut = new File(starOutDir);
        starOut.mkdirs();
    }
    
    public void loadSharedMemoryReference(String ref, boolean unload) throws InterruptedException {
        if(ref == null) ref = this.ref;
        if(unload)  Logger.DEBUG("Remove ref [" + ref + "] from shared memory.");
        else Logger.DEBUG("Load ref [" + ref + "] to shared memory");
        String[] command = CommandGenerator.starGenomeLoad(bin, ref, unload);
        star = new ProcessBuilderWrapper(command, bin);
        star.startProcess(System.out, System.err);
        if(!star.isAlive())
            throw new ProcessException("STAR aligner load", star.getExitState());
        HalvadeHeartBeat hhb = new HalvadeHeartBeat(context);
        hhb.start();
        int error = star.waitForCompletion();
        hhb.jobFinished();
        hhb.join();
        if(!(error == 0 || error == 105)) // 105 = no ref in memory
            throw new ProcessException("STAR aligner load", error);
        context.getCounter(HalvadeCounters.TIME_STAR_REF).increment(star.getExecutionTime());
    }
    
    protected static boolean sparseGenome = true;
    public static long rebuildStarGenome(TaskInputOutputContext context, String bin, String newGenomeDir, 
            String ref, String SJouttab, int sjoverhang, int threads, long mem) throws InterruptedException {
        Logger.DEBUG("Creating new genome in " + newGenomeDir);
        String[] command = 
                CommandGenerator.starRebuildGenome(bin, newGenomeDir, ref, SJouttab, 
                        sjoverhang, threads, mem, sparseGenome);
        
        ProcessBuilderWrapper starbuild = new ProcessBuilderWrapper(command, bin);
        starbuild.startProcess(System.out, System.err);
        if(!starbuild.isAlive())
            throw new ProcessException("STAR rebuild genome", starbuild.getExitState());
        HalvadeHeartBeat hhb = new HalvadeHeartBeat(context);
        hhb.start();
        int error = starbuild.waitForCompletion();
        hhb.jobFinished();
        hhb.join();
        if(error != 0)
            throw new ProcessException("STAR aligner load", error);
        return starbuild.getExecutionTime();        
    }
}
