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
import be.ugent.intec.halvade.utils.CommandGenerator;
import be.ugent.intec.halvade.utils.HalvadeFileUtils;
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
public class BWAAlnInstance extends AlignerInstance {
    
    private static BWAAlnInstance instance;
    private ProcessBuilderWrapper reads1;
    private ProcessBuilderWrapper reads2;
    private ProcessBuilderWrapper samxe;
    private SAMStreamHandler ssh;
    private BufferedWriter fastqFile1;
    private BufferedWriter fastqFile2;
    private String taskId;
    
    private BWAAlnInstance(Mapper.Context context, String bin) throws IOException, URISyntaxException {
        super(context, bin);  
        taskId = context.getTaskAttemptID().toString();
        taskId = taskId.substring(taskId.indexOf("m_"));
        ref = HalvadeFileUtils.downloadBWAIndex(context, taskId);
    }
    
    public int feedLine(String line, int read) throws IOException, InterruptedException  {
        if(read == 1 || !isPaired) {
            fastqFile1.write(line + "\n");
            return feedLine(line, reads1);
        } else if (read == 2) {
            fastqFile2.write(line + "\n");
            return feedLine(line, reads2);
        }
        return -1;
    }
    
    protected String getFileName(String dir, String id, boolean isSai, int read) {
        String outFile = dir;
        if(isSai) {
            if(read == 1) {
                outFile += read1File + id + ".sai";
            } else if (read == 2) {
                outFile += read2File + id + ".sai";
            } else return null;
        } else {
            if(read == 1) {
                outFile += read1File + id + ".fastq";
            } else if (read == 2) {
                outFile += read2File + id + ".fastq";
            } else return null;
        }
        return outFile;
    }
        
    @Override
    protected void startAligner(Mapper.Context context) throws IOException, InterruptedException {
        // make command
        // use half the threads if paired reads ( 2 instances of aln will run)
        int threadsToUse = threads;
        if (isPaired && threadsToUse > 1 ) threadsToUse /= 2;
        String customArgs = HalvadeConf.getCustomArgs(context.getConfiguration(), "bwa", "aln");
        String[] command1 = CommandGenerator.bwaAln(bin, ref, "/dev/stdin", getFileName(tmpdir, taskId, true, 1), threadsToUse, customArgs);
        reads1 = new ProcessBuilderWrapper(command1, bin);
        reads1.setThreads(threadsToUse);
        reads1.startProcess(null, System.err);
        // check if alive.
        if(!reads1.isAlive())
            throw new ProcessException("BWA aln", reads1.getExitState());
            
        File file1 = new File(getFileName(tmpdir, taskId, false, 1));
        if (!file1.exists()) {
            file1.createNewFile();
        }
        fastqFile1 = new BufferedWriter(new FileWriter(file1.getAbsoluteFile()));
        if(isPaired) {
            String[] command2 = CommandGenerator.bwaAln(bin, ref, "/dev/stdin", getFileName(tmpdir, taskId, true, 2), threadsToUse, customArgs);
            reads2 = new ProcessBuilderWrapper(command2, bin);
            reads2.setThreads(threadsToUse);
            reads2.startProcess(null, System.err);
            if(!reads2.isAlive())
                throw new ProcessException("BWA aln", reads2.getExitState());
            File file2 = new File(getFileName(tmpdir, taskId,false, 2));
            if (!file2.exists()) {
                    file2.createNewFile();
            }
            fastqFile2 = new BufferedWriter(new FileWriter(file2.getAbsoluteFile()));
        }
        
//        hhb = new HalvadeHeartBeat(context);
//        hhb.start();
    }
        
    /**
     * 
     * @return 1 is running, 0 is completed, -1 is error 
     */
    @Override
    public int getState() {
        if (isPaired) 
            return reads1.getState() & reads2.getState();
        else
            return reads1.getState();
    }
    
    private void closeBWAAln() throws InterruptedException {
        try {
            // close the input stream
            reads1.getSTDINWriter().flush();
            reads1.getSTDINWriter().close();
            fastqFile1.close();
            if(isPaired) {
                reads2.getSTDINWriter().flush();
                reads2.getSTDINWriter().close();
                fastqFile2.close();
            }
        } catch (IOException ex) {
//            hhb.jobFinished();
//            hhb.join();
            Logger.EXCEPTION(ex);
            throw new ProcessException("BWA aln", -1);
        }
                
        int error = reads1.waitForCompletion();
//        hhb.jobFinished();
//        hhb.join();
        if(error != 0)
            throw new ProcessException("BWA aln", error);
        context.getCounter(HalvadeCounters.TIME_BWA_ALN).increment(reads1.getExecutionTime());
        if(isPaired) {
            error = reads2.waitForCompletion();
            if(error != 0)
                throw new ProcessException("BWA aln", error);
            context.getCounter(HalvadeCounters.TIME_BWA_ALN).increment(reads2.getExecutionTime());
        }
    }
    
    private void startBWASamXe() throws InterruptedException { 
        String customArgs = HalvadeConf.getCustomArgs(context.getConfiguration(), "bwa", "sampe");  
        String[] command = CommandGenerator.bwaSamXe(bin, ref,
                getFileName(tmpdir, taskId, true, 1), 
                getFileName(tmpdir, taskId, false, 1), 
                getFileName(tmpdir, taskId, true, 2), 
                getFileName(tmpdir, taskId, false, 2), 
                isPaired, threads, customArgs);
        samxe = new ProcessBuilderWrapper(command, bin);
        samxe.startProcess(null, System.err);     
        if(!samxe.isAlive())
            throw new ProcessException("BWA samXe", samxe.getExitState());   
        
        // make a SAMstream handler
        ssh = new SAMStreamHandler(instance, context, false);
        ssh.start();
    }

    @Override
    public void closeAligner() throws InterruptedException {
        // close last BWA aln
        closeBWAAln();

        // but now start sampe/samse
        startBWASamXe();
        ssh.join();
        int error = samxe.waitForCompletion();
        if(error != 0)
            throw new ProcessException("BWA samXe", error);
        context.getCounter(HalvadeCounters.TIME_BWA_SAMPE).increment(samxe.getExecutionTime());
        
        //remove all temporary fastq/sai files
        HalvadeFileUtils.removeLocalFile(keep, getFileName(tmpdir, taskId, true, 1), context, HalvadeCounters.FOUT_BWA_TMP);
        HalvadeFileUtils.removeLocalFile(keep, getFileName(tmpdir, taskId, false, 1), context, HalvadeCounters.FOUT_BWA_TMP);
        HalvadeFileUtils.removeLocalFile(keep, getFileName(tmpdir, taskId, true, 2), context, HalvadeCounters.FOUT_BWA_TMP);
        HalvadeFileUtils.removeLocalFile(keep, getFileName(tmpdir, taskId, false, 2), context, HalvadeCounters.FOUT_BWA_TMP);
        instance = null;
    }
    
    static public BWAAlnInstance getBWAInstance(Mapper.Context context, String bin) throws IOException, InterruptedException, URISyntaxException {
        if(instance == null) {
            instance = new BWAAlnInstance(context, bin);
            instance.startAligner(context);
        }
        BWAAlnInstance.context = context;
        Logger.DEBUG("Started BWA");
        return instance;
    }
    
    @Override
    public InputStream getSTDOUTStream() {
        return samxe.getSTDOUTStream();
    }

    @Override
    public void flushStream() {
        try {
            // close the input stream
            reads1.getSTDINWriter().flush();
            if(isPaired) reads2.getSTDINWriter().flush();
        } catch (IOException ex) {
            Logger.EXCEPTION(ex);
        }
    }
}
