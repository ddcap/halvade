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

package be.ugent.intec.halvade.utils;

import fi.tkk.ics.hadoop.bam.SAMRecordWritable;
import be.ugent.intec.halvade.hadoop.datatypes.ChromosomeRegion;
import be.ugent.intec.halvade.tools.AlignerInstance;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import net.sf.samtools.*;
import net.sf.samtools.util.BufferedLineReader;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;

/**
 *
 * @author ddecap
 */
public class SAMStreamHandler extends Thread {

    /*
     * reads input from an outputstream (stdout from a process)
     * writes every input from the stream to the output of hadoop
     * which will be sorted and processed further
     */
    InputStream is;
    SAMRecordFactory samRecordFactory;
    BufferedLineReader mReader;
    SAMFileHeader mFileHeader;
    String mCurrentLine;
    File mFile;
    SAMFileReader.ValidationStringency validationStringency;
    SAMFileReader mParentReader;
    SAMLineParser parser;
    TaskInputOutputContext<LongWritable, Text, ChromosomeRegion, SAMRecordWritable> context;
    SAMRecordWritable writableRecord;
    ChromosomeRegion writableRegion;
    AlignerInstance instance;
    boolean isPaired = true;
    protected boolean useCompact;

    public SAMStreamHandler(AlignerInstance instance, Context context, boolean useCompact) {
        this.is = instance.getSTDOUTStream();
        this.mFileHeader = instance.getFileHeader();
        this.instance = instance;
        this.useCompact = useCompact;
        mCurrentLine = null;
        mFile = null;
        this.validationStringency = SAMFileReader.ValidationStringency.LENIENT;
        mReader = new BufferedLineReader(this.is);
        samRecordFactory = new DefaultSAMRecordFactory();
        this.context = context;
        isPaired = HalvadeConf.getIsPaired(context.getConfiguration());
    }
    
    @Override
    public void run() {
        // get header first 
        SAMTextHeaderCodec headerCodec = new SAMTextHeaderCodec();
        headerCodec.setValidationStringency(validationStringency);
        if(mFileHeader == null) {
            mFileHeader = headerCodec.decode(mReader, mFile == null ? null : mFile.toString());
            instance.setFileHeader(mFileHeader);
        } else {
            mFileHeader = instance.getFileHeader();
        }
        parser = new SAMLineParser(samRecordFactory, validationStringency, mFileHeader, mParentReader, mFile);
        // now process each read...
        int count = 0;
        mCurrentLine = mReader.readLine();
        try {
            while (mCurrentLine != null) {
                SAMRecord samrecord = parser.parseLine(mCurrentLine, mReader.getLineNumber());
                // only write mapped records as output
                // paired or unpaired ?? need to know to check for boundaries
                
                if(isPaired) count += instance.writePairedSAMRecordToContext(samrecord, useCompact);
                else count += instance.writeSAMRecordToContext(samrecord, useCompact);
                //advance line even if bad line
                advanceLine();
            }
        } catch (IOException | InterruptedException ex) {
            Logger.EXCEPTION(ex);
        }
        Logger.DEBUG("SAMstream counts " + count + " records");
    }
    
    private String advanceLine()
    {
        mCurrentLine = mReader.readLine();
        return mCurrentLine;
    }
}
