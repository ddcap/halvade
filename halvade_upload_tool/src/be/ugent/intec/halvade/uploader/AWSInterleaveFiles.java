/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package be.ugent.intec.halvade.uploader;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.zip.GZIPOutputStream;

/**
 *
 * @author ddecap
 */
public class AWSInterleaveFiles extends Thread {
    private FastQFileReader pReader;
    private FastQFileReader sReader;
    private static long MAXFILESIZE = 60000000L; // ~60MB
    String pairedBase;
    String singleBase;
    AWSUploader upl; // S3
    long read, written;
    
    public AWSInterleaveFiles(String paired, String single, long maxFileSize, AWSUploader upl) {
        this.upl = upl;
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
            ByteArrayOutputStream dataStream = new ByteArrayOutputStream();
            dataStream.reset();
            OutputStream gzipOutputStream = new GZIPOutputStream(dataStream);
            
            ReadBlock block = new ReadBlock();
            int fileWritten = 0;
            while(pReader.getNextBlock(block)) {
                fileWritten += block.write(gzipOutputStream);
                // check filesize
                if(dataStream.size() > MAXFILESIZE) {
                    gzipOutputStream.close();
                    count += block.size / 4;
                    uploadToAWS(part, dataStream);
                    written += dataStream.size();
                    read += fileWritten;
                    fileWritten = 0;
                    part++;
                    
                    dataStream.reset(); 
                    gzipOutputStream = new GZIPOutputStream(dataStream);                    
                }
            }
            // finish the files          
            gzipOutputStream.close();
            if(fileWritten != 0) {
                count += block.size / 4;
                uploadToAWS(part, dataStream);
                written += dataStream.size();
                read += fileWritten;
                dataStream.reset();
            }
            gzipOutputStream = new GZIPOutputStream(dataStream);
            
            // do single reads
            part = 0;
            dataStream.reset();
            gzipOutputStream = new GZIPOutputStream(dataStream);
            fileWritten = 0;
            while(sReader.getNextBlock(block)) {
                fileWritten += block.write(gzipOutputStream);
                // check filesize
                if(dataStream.size() > MAXFILESIZE) {
                    gzipOutputStream.close();
                    count += block.size / 4;
                    uploadToAWS(part, dataStream);
                    dataStream.reset();
                    written += dataStream.size();
                    read += fileWritten;
                    fileWritten = 0;
                    part++;
                    gzipOutputStream = new GZIPOutputStream(dataStream);
                }
            }
            // finish the files
            gzipOutputStream.close();
            if(fileWritten != 0) {
                count += block.size / 4;
                uploadToAWS(part, dataStream);
                written += dataStream.size();
                read += fileWritten;
                dataStream.reset();
            }
            Logger.DEBUG("number of reads: "+ count);
            Logger.DEBUG("read " + round(read / (1024*1024)) + "MB");
            Logger.DEBUG("written " + round(written / (1024*1024)) + "MB");
            dataStream.close();
        } catch (IOException ex) {
            Logger.EXCEPTION(ex);
        }
    }
    
    public void uploadToAWS(int part, ByteArrayOutputStream stream) {
        try {
            Logger.DEBUG("uploading part " + part + ": " + stream.size());
            upl.Upload(pairedBase + part + ".fq.gz", new ByteArrayInputStream(stream.toByteArray()), stream.size());
        } catch (InterruptedException ex) {
            Logger.DEBUG("failed to upload part to AWS...");
            Logger.EXCEPTION(ex);
        }
    }
}
