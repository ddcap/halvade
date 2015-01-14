/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package be.ugent.intec.halvade.hadoop.mapreduce;

import be.ugent.intec.halvade.tools.STARInstance;
import be.ugent.intec.halvade.utils.HalvadeConf;
import be.ugent.intec.halvade.utils.HalvadeFileLock;
import be.ugent.intec.halvade.utils.HalvadeFileUtils;
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
    protected boolean runPass2;
    protected final String SH_MEM_LOCK = "load_sh_mem.lock";
    protected static final int PASS1_LOCK_VAL = 1;
    protected static final int PASS2_LOCK_VAL = 2;
    protected HalvadeFileLock star_shmem_lock;

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
        if(allTasksHaveStarted) {
            try {
                star_shmem_lock.getLock();    
                ((STARInstance)instance).loadSharedMemoryReference(null, true);
            } finally {
                star_shmem_lock.removeAndReleaseLock();
            }
        }
        if(!runPass2) {
            context.write(new LongWritable(1), 
                    new Text("" + ((STARInstance)instance).getOverhang()));
        }
    }
    
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        super.map(key, value, context);
        ((STARInstance)instance).feedLine(value.toString(), count, readcount % 2);
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        tmpDir = HalvadeConf.getScratchTempDir(context.getConfiguration());
        star_shmem_lock = new HalvadeFileLock(tmpDir, SH_MEM_LOCK);
        try {
            String binDir = checkBinaries(context);
            runPass2 = HalvadeConf.getIsPass2(context.getConfiguration());
            instance = STARInstance.getSTARInstance(context, binDir, runPass2 ? STARInstance.PASS2 : STARInstance.PASS1);  
            loadReference(context);    
         } catch (URISyntaxException ex) {
            Logger.EXCEPTION(ex);
            throw new InterruptedException();
        }
    }
    
    protected void loadReference(Context context) throws IOException, InterruptedException, URISyntaxException {
        try {
            star_shmem_lock.getLock();
            ByteBuffer bytes = ByteBuffer.allocate(4);
            if (star_shmem_lock.read(bytes) > 0) {
                bytes.flip();
                long val = bytes.getInt();
                if(val == (runPass2 ? PASS2_LOCK_VAL : PASS1_LOCK_VAL))
                    Logger.DEBUG("Ref has been loaded into shared memory: " + val);
                else {
                    Logger.DEBUG("Pass 1 Ref is still loaded into shared memory, freeing first");
                    
                    String taskId = context.getTaskAttemptID().toString();
                    taskId = taskId.substring(taskId.indexOf("m_"));
                    String starGen = HalvadeFileUtils.downloadSTARIndex(context, taskId, false);
                    ((STARInstance)instance).loadSharedMemoryReference(starGen, true); // unload other first
                    ((STARInstance)instance).loadSharedMemoryReference(null, false);
                    bytes.clear();
                    bytes.putInt(runPass2 ? PASS2_LOCK_VAL : PASS1_LOCK_VAL).flip();
                    star_shmem_lock.forceWrite(bytes);
                }
            } else {
                ((STARInstance)instance).loadSharedMemoryReference(null, false);
                bytes.putInt(runPass2 ? PASS2_LOCK_VAL : PASS1_LOCK_VAL).flip();
                star_shmem_lock.forceWrite(bytes);
            }
        } catch (IOException ex) {
            Logger.EXCEPTION(ex);
        } finally {
            star_shmem_lock.releaseLock();
        }
    }
}