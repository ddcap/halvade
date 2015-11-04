/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package be.ugent.intec.halvade.hadoop.mapreduce;

import be.ugent.intec.halvade.hadoop.datatypes.ChromosomeRegion;
import be.ugent.intec.halvade.tools.AlignerInstance;
import be.ugent.intec.halvade.tools.DummyAlignerInstance;
import be.ugent.intec.halvade.utils.HalvadeConf;
import be.ugent.intec.halvade.utils.Logger;
import org.seqdoop.hadoop_bam.SAMRecordWritable;
import java.io.IOException;
import java.net.URISyntaxException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author ddecap
 */
public class AlignedBamMapper extends Mapper<LongWritable,SAMRecordWritable, 
        ChromosomeRegion, SAMRecordWritable> {
    protected AlignerInstance instance;
    boolean isPaired = true;
    
    @Override
    protected void map(LongWritable key, SAMRecordWritable value, Context context) throws IOException, InterruptedException {
        try {
            value.get().getSAMString();
            instance.writePairedSAMRecordToContext(value.get(), false);
        } catch(StringIndexOutOfBoundsException e) {
            Logger.DEBUG("incorrect samstring, skipping...");
        } catch(IllegalArgumentException ex) {
            Logger.DEBUG("error in : " + value.get().getReadName());
            Logger.EXCEPTION(ex);
        }
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        isPaired = HalvadeConf.getIsPaired(context.getConfiguration());
        try {
            String taskId = context.getTaskAttemptID().toString();
            Logger.DEBUG("taskId = " + taskId);
            HalvadeConf.addTaskRunning(context.getConfiguration(), taskId);
            instance = DummyAlignerInstance.getDummyInstance(context, null);
        } catch (URISyntaxException ex) {
            Logger.EXCEPTION(ex);
            throw new InterruptedException();
        }
    }
}
