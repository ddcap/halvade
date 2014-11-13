/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package be.ugent.intec.halvade.tools;

import static be.ugent.intec.halvade.tools.AlignerInstance.ref;
import be.ugent.intec.halvade.utils.HDFSFileIO;
import be.ugent.intec.halvade.utils.Logger;
import be.ugent.intec.halvade.utils.MyConf;
import be.ugent.intec.halvade.utils.ProcessBuilderWrapper;
import be.ugent.intec.halvade.utils.SAMStreamHandler;
import java.io.BufferedWriter;
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
//    private BufferedWriter fastqFile1;
//    private BufferedWriter fastqFile2;
    private String taskId;
    private boolean keep = false;
    
    private STARInstance(Mapper.Context context, String bin) throws IOException, URISyntaxException {
        super(context, bin);  
        taskId = context.getTaskAttemptID().toString();
        taskId = taskId.substring(taskId.indexOf("m_"));
        ref = HDFSFileIO.downloadBWAIndex(context, taskId); 
        keep = MyConf.getKeepFiles(context.getConfiguration());
    }
    
    public int feedLine(String line, int read) throws IOException, InterruptedException  {
        return feedLine(line, star);
    }

    public static AlignerInstance getSTARInstance(Mapper.Context context, String binDir) throws URISyntaxException, IOException, InterruptedException {
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
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public int getState() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public InputStream getSTDOUTStream() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void closeAligner() throws InterruptedException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    protected void startAligner(Mapper.Context context) throws IOException, InterruptedException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
    
}
