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

import be.ugent.intec.halvade.hadoop.mapreduce.HalvadeCounters;
import static be.ugent.intec.halvade.tools.AlignerInstance.context;
import static be.ugent.intec.halvade.tools.AlignerInstance.ref;
import be.ugent.intec.halvade.utils.CommandGenerator;
import be.ugent.intec.halvade.utils.HDFSFileIO;
import be.ugent.intec.halvade.utils.Logger;
import be.ugent.intec.halvade.utils.HalvadeConf;
import be.ugent.intec.halvade.utils.ProcessBuilderWrapper;
import be.ugent.intec.halvade.utils.SAMStreamHandler;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author ddecap
 */
public class STARInstance extends AlignerInstance {
    private static STARInstance instance;
    private ProcessBuilderWrapper star;
    private SAMStreamHandler ssh;
    private BufferedWriter fastqFile1;
    private BufferedWriter fastqFile2;
    private String taskId;
    private String starTmpDir;
    private String starOutDir;
    private int overhang, nReads;
    
    private STARInstance(Mapper.Context context, String bin) throws IOException, URISyntaxException {
        super(context, bin);  
        taskId = context.getTaskAttemptID().toString();
        taskId = taskId.substring(taskId.indexOf("m_"));
        ref = HDFSFileIO.downloadSTARIndex(context, taskId);
        starTmpDir = tmpdir + taskId + "-STARtmp/";
        starOutDir = tmpdir + taskId + "-STARout/";
        nReads = 0;
        overhang = 0;
    }
    
    public int feedLine(String line, int count, int read) throws IOException, InterruptedException  {
        if(read == 1 || !isPaired) {
            fastqFile1.write(line + "\n");
//            return feedLine(line, star);
        } else if (read == 2) {
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
    
    protected String getFileName(String dir, String id, int read) {
        String outFile = dir;
        if(read == 1) {
            outFile += read1File + id + ".fastq";
        } else if (read == 2) {
            outFile += read2File + id + ".fastq";
        } else return null;
        return outFile;
    }

    public static AlignerInstance getSTARInstance(Mapper.Context context, String bin) throws URISyntaxException, IOException, InterruptedException {
        if(instance == null) {
            instance = new STARInstance(context, bin);
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
        String customArgs = HalvadeConf.getBwaMemArgs(context.getConfiguration());
        // overhang = reandlength -1 
        // count nreads! -> 
        String[] command = CommandGenerator.starAlign(bin, ref, starOutDir, starTmpDir, 
                getFileName(tmpdir, taskId, 1), getFileName(tmpdir, taskId, 2), threads, overhang, nReads, customArgs);
        star = new ProcessBuilderWrapper(command, bin);
        // run command
        // needs to be streamed to output otherwise the process blocks ...
        star.startProcess(null, System.err);
        // check if alive.
        if(!star.isAlive())
            throw new ProcessException("STAR aligner", star.getExitState());
        star.getSTDINWriter();
        // make a SAMstream handler
        ssh = new SAMStreamHandler(instance, context);
        ssh.start();
        
        // send a heartbeat every min its still running -> avoid timeout
        HalvadeHeartBeat hhb = new HalvadeHeartBeat(context);
        hhb.start();
        
        ssh.join();
        int error = star.waitForCompletion();
        hhb.jobFinished();
        hhb.join();
        if(error != 0)
            throw new ProcessException("STAR aligner", error);
        context.getCounter(HalvadeCounters.TIME_STAR).increment(star.getExecutionTime());
        
        //remove all temporary fastq/sai files
        removeLocalFile(getFileName(tmpdir, taskId, 1), context, HalvadeCounters.FOUT_STAR_TMP);
        removeLocalFile(getFileName(tmpdir, taskId, 2), context, HalvadeCounters.FOUT_STAR_TMP);
        // delete star tmp/out dirs
        removeLocalDir(starTmpDir, context, HalvadeCounters.FOUT_STAR_TMP);
        removeLocalDir(starOutDir, context, HalvadeCounters.FOUT_STAR_TMP);
        instance = null;
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
        starOut.mkdir();
    }
    
}
