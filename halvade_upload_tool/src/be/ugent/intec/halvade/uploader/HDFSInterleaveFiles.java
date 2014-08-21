/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package be.ugent.intec.halvade.uploader;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.zip.GZIPOutputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 *
 * @author ddecap
 */
public class HDFSInterleaveFiles extends Thread {
    private FastQFileReader pReader;
    private FastQFileReader sReader;
    private static long MAXFILESIZE = 60000000L; // ~60MB
    String pairedBase;
    String singleBase;
    FileSystem fs; // HDFS
    long read, written;
    
    public HDFSInterleaveFiles(String paired, String single, long maxFileSize, FileSystem fs) {
        this.fs = fs;
        this.pairedBase = paired;
        this.singleBase = single;
        written = 0;
        read = 0;
        pReader = FastQFileReader.getPairedInstance();
        sReader = FastQFileReader.getSingleInstance();
        MAXFILESIZE = maxFileSize;
    }

    private double round(double value) {
        return (int)(value * 100 + 0.5) / 100.0;
    }

    @Override
    public void run() {
        try {
            Logger.DEBUG("Starting thread to write reads to hdfs");
            int part = 0;    
            int count = 0;
            FSDataOutputStream dataStream = fs.create(new Path(pairedBase + part + ".fq.gz"),true);
            OutputStream gzipOutputStream = 
                    new GZIPOutputStream(new BufferedOutputStream(dataStream));
            
            ReadBlock block = new ReadBlock();
            int fileWritten = 0;
            while(pReader.getNextBlock(block)) {
                fileWritten += block.write(gzipOutputStream);
                // check filesize
                if(dataStream.size() > MAXFILESIZE) {
                    gzipOutputStream.close();
                    count += block.size / 4;
                    dataStream.close();
                    written += dataStream.size();
                    read += fileWritten;
                    fileWritten = 0;
                    part++;
                    dataStream = fs.create(new Path(pairedBase + part + ".fq.gz"),true);
                    gzipOutputStream = 
                        new GZIPOutputStream(new BufferedOutputStream(dataStream));                  
                }
            }
            // finish the files          
            gzipOutputStream.close();
            if(dataStream.size() == 0 || fileWritten == 0) {
                // delete this file
                fs.delete(new Path(singleBase + part + ".fq.gz"), true);
            } else {
                count += block.size / 4;
                written += dataStream.size();
                read += fileWritten;
                dataStream.close();
            }
            
            // do single reads
            part = 0;
            dataStream = fs.create(new Path(singleBase + part + ".fq.gz"),true);
            gzipOutputStream = 
                    new GZIPOutputStream(new BufferedOutputStream(dataStream));
            fileWritten = 0;
            while(sReader.getNextBlock(block)) {
                fileWritten += block.write(gzipOutputStream);
                // check filesize
                if(dataStream.size() > MAXFILESIZE) {
                    gzipOutputStream.close();
                    count += block.size / 4;
                    dataStream.close();
                    written += dataStream.size();
                    read += fileWritten;
                    fileWritten = 0;
                    part++;
                    dataStream = fs.create(new Path(singleBase + part + ".fq.gz"),true);
                    gzipOutputStream = 
                        new GZIPOutputStream(new BufferedOutputStream(dataStream));
                }
            }
            // finish the files
            gzipOutputStream.close();
            if(dataStream.size() == 0 || fileWritten == 0) {
                // delete this file
                fs.delete(new Path(singleBase + part + ".fq.gz"), true);
            } else {
                count += block.size / 4;
                written += dataStream.size();
                read += fileWritten;
                dataStream.close();
            }
            Logger.DEBUG("number of reads: "+ count);
            Logger.DEBUG("read " + round(read / (1024*1024)) + "MB");
            Logger.DEBUG("written " + round(written / (1024*1024)) + "MB");
        } catch (IOException ex) {
            Logger.EXCEPTION(ex);
        }
    }
}
