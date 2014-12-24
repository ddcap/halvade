/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package be.ugent.intec.halvade.hadoop.mapreduce;

import be.ugent.intec.halvade.tools.STARInstance;
import be.ugent.intec.halvade.utils.HalvadeConf;
import be.ugent.intec.halvade.utils.Logger;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

/**
 *
 * @author ddecap
 */
public class StarAlignPassXMapper  extends HalvadeMapper<LongWritable, Text> {
    protected String tmpDir;
    protected final String SH_MEM_LOCK = "load_sh_mem.lock";
    protected boolean runPass2;

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
        if(allTasksHaveStarted) {
            getLock(tmpDir, SH_MEM_LOCK);    
            ((STARInstance)instance).loadSharedMemoryReference(true);
            lockfile.deleteOnExit();
            releaseLock();
        }
        if(!runPass2) {
            context.write(new LongWritable(1), 
                    new Text("" + ((STARInstance)instance).getOverhang()));
        }
    }
    
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        super.map(key, value, context);
        ((STARInstance)instance).feedLine(value.toString(), count, (readcount % 2 + 1));
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        tmpDir = HalvadeConf.getScratchTempDir(context.getConfiguration());
        try {
            String binDir = checkBinaries(context);
            runPass2 = HalvadeConf.getIsPass2(context.getConfiguration());
            instance = STARInstance.getSTARInstance(context, binDir, runPass2 ? STARInstance.PASS2 : STARInstance.PASS1);  
            loadReference();    
         } catch (URISyntaxException ex) {
            Logger.EXCEPTION(ex);
            throw new InterruptedException();
        }
    }
    
    protected void loadReference() throws IOException, InterruptedException {
        try {
            getLock(tmpDir, SH_MEM_LOCK);            
            ByteBuffer bytes = ByteBuffer.allocate(4);
            // read first int
            if (f.read(bytes) > 0) {
                bytes.flip();
                long val = bytes.getInt();
                Logger.DEBUG("Ref has been loaded into shared memory: " + val);
            } else {
                ((STARInstance)instance).loadSharedMemoryReference(false);
                bytes.putInt(1).flip();
                f.write(bytes);
                f.force(false);
            }
        } catch (IOException ex) {
            Logger.EXCEPTION(ex);
        } finally {
            releaseLock();
        }
    }
}