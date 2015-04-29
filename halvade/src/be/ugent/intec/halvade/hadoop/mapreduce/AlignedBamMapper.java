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
import fi.tkk.ics.hadoop.bam.SAMRecordWritable;
import java.io.IOException;
import java.net.URISyntaxException;
import net.sf.samtools.SAMRecord;
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
        SAMRecord sam = value.get();
        try{
            String s = sam.getSAMString();
        } catch(StringIndexOutOfBoundsException e) {
            Logger.DEBUG("error with samstring...");
            Logger.DEBUG(sam.getReadName() + " " + sam.getReadString());
            throw e;
        }
        instance.writePairedSAMRecordToContext(value.get(), false);
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
