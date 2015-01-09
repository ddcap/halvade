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
import java.io.IOException;
import java.io.InputStream;
import org.apache.hadoop.mapreduce.Mapper;
import be.ugent.intec.halvade.utils.*;
import java.net.URISyntaxException;
import org.apache.hadoop.mapreduce.Mapper.Context;

/**
 *
 * @author ddecap
 */
public class BWAMemInstance extends AlignerInstance {
    
    private static BWAMemInstance instance;
    private ProcessBuilderWrapper pbw;
    private SAMStreamHandler ssh;
    /**
     * 
     * This BWA instance runs BWA from stdin (custom provided BWA is needed)
     */
    private BWAMemInstance(Context context, String bin) throws IOException, URISyntaxException {
        super(context, bin);
        String taskid = context.getTaskAttemptID().toString();
        taskid = taskid.substring(taskid.indexOf("m_"));
        ref = HalvadeFileUtils.downloadBWAIndex(context, taskid);
    }
    
    public int feedLine(String line) throws IOException  {
        return feedLine(line, pbw);
    }
    
    @Override
    protected void startAligner(Mapper.Context context) throws IOException, InterruptedException {
        // make command
        String customArgs = HalvadeConf.getCustomArgs(context.getConfiguration(), "bwa", "mem");
        String[] command = CommandGenerator.bwaMem(bin, ref, null, null, isPaired, true, threads, customArgs);
        pbw = new ProcessBuilderWrapper(command, bin);
        // run command
        // needs to be streamed to output otherwise the process blocks ...
        pbw.startProcess(null, System.err);
        // check if alive.
        if(!pbw.isAlive())
            throw new ProcessException("BWA mem", pbw.getExitState());
        pbw.getSTDINWriter();
        // make a SAMstream handler
        ssh = new SAMStreamHandler(instance, context);
        ssh.start();
    }
        
    /**
     * 
     * @return 1 is running, 0 is completed, -1 is error 
     */
    @Override
    public int getState() {
        return pbw.getState();
    }

    @Override
    public void closeAligner() throws InterruptedException {
        try {
            // close the input stream
            pbw.getSTDINWriter().flush();
            pbw.getSTDINWriter().close();
        } catch (IOException ex) {
            Logger.EXCEPTION(ex);
        }
                
        int error = pbw.waitForCompletion();
        context.getCounter(HalvadeCounters.TIME_BWA_MEM).increment(pbw.getExecutionTime());
        if(error != 0)
            throw new ProcessException("BWA mem", error);
        ssh.join();
        instance = null;
    }
        
    static public BWAMemInstance getBWAInstance(Mapper.Context context, String bin) throws IOException, InterruptedException, URISyntaxException {
        if(instance == null) {
            instance = new BWAMemInstance(context, bin);
            instance.startAligner(context);
        }
        BWAMemInstance.context = context;
        return instance;
    }
    
    
    @Override
    public InputStream getSTDOUTStream() {
        return pbw.getSTDOUTStream();
    }

    @Override
    public void flushStream() {
        try {
            // close the input stream
            pbw.getSTDINWriter().flush();
        } catch (IOException ex) {
            Logger.EXCEPTION(ex);
        }
    }
}
