/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package be.ugent.intec.halvade.tools;

import be.ugent.intec.halvade.utils.HalvadeFileUtils;
import be.ugent.intec.halvade.utils.Logger;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author ddecap
 */
public class DummyAlignerInstance extends AlignerInstance {
    
    private static DummyAlignerInstance instance;
    private String taskId;
    
    private DummyAlignerInstance(Mapper.Context context, String bin) throws IOException, URISyntaxException {
        super(context, bin);  
        taskId = context.getTaskAttemptID().toString();
        taskId = taskId.substring(taskId.indexOf("m_"));
//        ref = HalvadeFileUtils.downloadBWAIndex(context, taskId);
    }
    
    @Override
    protected void startAligner(Mapper.Context context) throws IOException, InterruptedException {
    }
        
    /**
     * 
     * @return 1 is running, 0 is completed, -1 is error 
     */
    @Override
    public int getState() {
        return 1;
    }
    

    @Override
    public void closeAligner() throws InterruptedException {
    }
    
    static public DummyAlignerInstance getDummyInstance(Mapper.Context context, String bin) throws IOException, InterruptedException, URISyntaxException {
        if(instance == null) {
            instance = new DummyAlignerInstance(context, bin);
            instance.startAligner(context);
        }
        BWAAlnInstance.context = context;
        Logger.DEBUG("Started Dummy aligner");
        return instance;
    }
    
    @Override
    public InputStream getSTDOUTStream() {
        return null;
    }

    @Override
    public void flushStream() {
    }
}

