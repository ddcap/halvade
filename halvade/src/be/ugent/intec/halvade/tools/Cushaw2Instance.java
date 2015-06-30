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
public class Cushaw2Instance extends AlignerInstance {
    
    private static Cushaw2Instance instance;
    private ProcessBuilderWrapper cushaw2;
    private SAMStreamHandler ssh;
    private BufferedWriter fastqFile1;
    private BufferedWriter fastqFile2;
    private String taskId;
    private String cushaw2CustomArgs;
    
    private Cushaw2Instance(Mapper.Context context, String bin) throws IOException, URISyntaxException {
        super(context, bin);  
        taskId = context.getTaskAttemptID().toString();
        taskId = taskId.substring(taskId.indexOf("m_"));
        ref = HalvadeFileUtils.downloadCushaw2Index(context, taskId);
        cushaw2CustomArgs = HalvadeConf.getCustomArgs(context.getConfiguration(), "cushaw2", "");
    }
    
    public int feedLine(String line, int read) throws IOException, InterruptedException  {
        if(read == 1 || !isPaired) {
            fastqFile1.write(line + "\n");
        } else if (read == 2) {
            fastqFile2.write(line + "\n");
        }
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
        
    @Override
    protected void startAligner(Mapper.Context context) throws IOException, InterruptedException {
        if(redistribute) {
            getIdleCores(context);
            Logger.DEBUG("Redistributing cores: using " + threads);
        }
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
    }
        
    /**
     * 
     * @return 1 is running, 0 is completed, -1 is error 
     */
    @Override
    public int getState() {
        if (cushaw2 != null)
            return cushaw2.getState();
        else 
            return 1;
    }

    @Override
    public void closeAligner() throws InterruptedException {
        try {
            fastqFile1.close();
            if(isPaired) {
                fastqFile2.close();
            }
        } catch (IOException ex) {
            Logger.EXCEPTION(ex);
            throw new ProcessException("Cushaw2", -1);
        }

        String customArgs = HalvadeConf.getCustomArgs(context.getConfiguration(), "cushaw2", "");  
        String[] command = CommandGenerator.cushaw2(bin, ref,
                getFileName(tmpdir, taskId, 1), 
                getFileName(tmpdir, taskId, 2), 
                threads, customArgs);
        cushaw2 = new ProcessBuilderWrapper(command, bin);
        cushaw2.startProcess(null, System.err);     
        if(!cushaw2.isAlive())
            throw new ProcessException("Cushaw2", cushaw2.getExitState());   
        
        // make a SAMstream handler
        ssh = new SAMStreamHandler(instance, context, false);
        ssh.start();


        ssh.join();
        int error = cushaw2.waitForCompletion();
        if(error != 0)
            throw new ProcessException("Cushaw2", error);
        context.getCounter(HalvadeCounters.TIME_CUSHAW2).increment(cushaw2.getExecutionTime());
        
        //remove all temporary fastq files
        HalvadeFileUtils.removeLocalFile(keep, getFileName(tmpdir, taskId, 1), context, HalvadeCounters.FOUT_BWA_TMP);
        HalvadeFileUtils.removeLocalFile(keep, getFileName(tmpdir, taskId, 2), context, HalvadeCounters.FOUT_BWA_TMP);
        instance = null;
    }
    
    static public Cushaw2Instance getCushaw2Instance(Mapper.Context context, String bin) throws IOException, InterruptedException, URISyntaxException {
        if(instance == null) {
            instance = new Cushaw2Instance(context, bin);
            instance.startAligner(context);
        }
        Cushaw2Instance.context = context;
        Logger.DEBUG("Started Cushaw2");
        return instance;
    }
    
    @Override
    public InputStream getSTDOUTStream() {
        return cushaw2.getSTDOUTStream();
    }

    @Override
    public void flushStream() {
        try {
            if (cushaw2 != null) cushaw2.getSTDINWriter().flush();
        } catch (IOException ex) {
            Logger.EXCEPTION(ex);
        }
    }
}
