/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package be.ugent.intec.halvade.hadoop.mapreduce;

import be.ugent.intec.halvade.hadoop.datatypes.ChromosomeRegion;
import be.ugent.intec.halvade.utils.HalvadeConf;
import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMReadGroupRecord;
import htsjdk.samtools.SAMRecord;
import htsjdk.samtools.SAMSequenceDictionary;
import htsjdk.samtools.SAMTag;
import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.Reducer;
import org.seqdoop.hadoop_bam.KeyIgnoringBAMOutputFormat;
import org.seqdoop.hadoop_bam.SAMRecordWritable;
import org.seqdoop.hadoop_bam.util.SAMHeaderReader;

/**
 *
 * @author ddecap
 */
public class BamMergeReducer extends Reducer<ChromosomeRegion, SAMRecordWritable, LongWritable, SAMRecordWritable> {
    
    protected SAMFileHeader header;
    protected SAMSequenceDictionary dict;
    protected KeyIgnoringBAMOutputFormat outpFormat;
    protected String RGID = "GROUP1";    
    protected String RGLB = "LIB1";
    protected String RGPL = "ILLUMINA";
    protected String RGPU = "UNIT1";
    protected String RGSM = "SAMPLE1";  
    protected SAMReadGroupRecord bamrg;
    protected boolean inputIsBam = false;
    protected RecordWriter<LongWritable,SAMRecordWritable> recordWriter;
    protected SAMRecordWritable samWritable = new SAMRecordWritable();
    protected LongWritable outKey;
    boolean reportBest = false;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        outpFormat = new KeyIgnoringBAMOutputFormat();
        String output = HalvadeConf.getOutDir(context.getConfiguration());
        inputIsBam = HalvadeConf.inputIsBam(context.getConfiguration());
        dict = HalvadeConf.getSequenceDictionary(context.getConfiguration());
        if(inputIsBam) {
            header = SAMHeaderReader.readSAMHeaderFrom(new Path(HalvadeConf.getHeaderFile(context.getConfiguration())), context.getConfiguration());
        } else {
            getReadGroupData(context.getConfiguration());
            header = new SAMFileHeader();
            header.setSequenceDictionary(dict);
            bamrg = new SAMReadGroupRecord(RGID);
            bamrg.setLibrary(RGLB);
            bamrg.setPlatform(RGPL);
            bamrg.setPlatformUnit(RGPU);
            bamrg.setSample(RGSM);
            header.addReadGroup(bamrg);
        }
        
        outpFormat.setSAMHeader(header);
        recordWriter = outpFormat.getRecordWriter(context, new Path(output + "mergedBam.bam"));
        outKey = new LongWritable();
        outKey.set(0);
    }

    @Override
    protected void reduce(ChromosomeRegion key, Iterable<SAMRecordWritable> values, Context context) throws IOException, InterruptedException {
        Iterator<SAMRecordWritable> it = values.iterator();
        SAMRecord sam = null;
        while(it.hasNext()) {
            sam = it.next().get();
            sam.setAttribute(SAMTag.RG.name(), RGID);
            samWritable.set(sam);
            recordWriter.write(outKey, samWritable);
                    
        }
    }
    
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context); //To change body of generated methods, choose Tools | Templates.
        recordWriter.close(context);
    }
    
    protected void getReadGroupData(Configuration conf) {
        String readGroup = HalvadeConf.getReadGroup(conf);
        String[] elements = readGroup.split(" ");
        for(String ele : elements) {
            String[] val = ele.split(":");
            if(val[0].equalsIgnoreCase("id"))
                RGID = val[1];
            else if(val[0].equalsIgnoreCase("lb"))
                RGLB = val[1];
            else if(val[0].equalsIgnoreCase("pl"))
                RGPL = val[1];
            else if(val[0].equalsIgnoreCase("pu"))
                RGPU = val[1];
            else if(val[0].equalsIgnoreCase("sm"))
                RGSM = val[1];
        }
    }
}
