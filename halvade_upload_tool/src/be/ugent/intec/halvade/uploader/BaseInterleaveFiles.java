/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package be.ugent.intec.halvade.uploader;

import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.zip.GZIPOutputStream;
import org.apache.hadoop.fs.Path;

/**
 *
 * @author ddecap
 */
abstract class BaseInterleaveFiles extends Thread {
    protected static final int BUFFERSIZE = 128*1024;
    protected FastQFileReader pReader;
    protected FastQFileReader sReader;
    protected static long MAXFILESIZE = 60000000L; // ~60MB
    protected String pairedBase;
    protected String singleBase;
    protected long read, written;
    protected String fsName;
    
    public BaseInterleaveFiles(String paired, String single, long maxFileSize) {
        this.pairedBase = paired;
        this.singleBase = single;
        written = 0;
        read = 0;
        pReader = FastQFileReader.getPairedInstance();
        sReader = FastQFileReader.getSingleInstance();
        MAXFILESIZE = maxFileSize;
    }

    protected double round(double value) {
        return (int)(value * 100 + 0.5) / 100.0;
    }

    protected abstract OutputStream getNewDataStream(int part, String prefix) throws IOException;
    protected abstract BufferedOutputStream getNewGZIPStream(OutputStream dataStream) throws IOException;
    protected abstract int getSize(OutputStream dataStream);
    protected abstract void writeData(int part, OutputStream dataStream, BufferedOutputStream gzipStream) throws IOException;
    protected abstract OutputStream resetDataStream(int part, String prefix, OutputStream dataStream) throws IOException;
    protected abstract void deleteFile(String prefix, int part) throws IOException;
    protected abstract void closeStreams(OutputStream dataStream, OutputStream gzipStream) throws IOException;

    @Override
    public void run() {
        try {
            Logger.DEBUG("Starting thread to write reads to " + fsName);
            int part = 0;    
            int count = 0;
            OutputStream dataStream = getNewDataStream(part, pairedBase);
            BufferedOutputStream gzipStream = getNewGZIPStream(dataStream); 
            
            ReadBlock block = new ReadBlock();
            int fileWritten = 0;
            int tSize;
            while(pReader.getNextBlock(block)) {
                fileWritten += block.write(gzipStream);
                // check filesize
                tSize = getSize(dataStream);
                if(tSize > MAXFILESIZE) {
                    gzipStream.close();
                    count += block.size / 4;
                    writeData(part, dataStream, gzipStream);
                    written += tSize;
                    read += fileWritten;
                    fileWritten = 0;
                    part++;
                    dataStream = resetDataStream(part, pairedBase, dataStream);
                    gzipStream = getNewGZIPStream(dataStream);                 
                }
            }
            // finish the files          
            gzipStream.close();
            tSize = getSize(dataStream);
            if(tSize == 0 || fileWritten == 0) {
                deleteFile(pairedBase, part);
            } else if(fileWritten != 0) {
                count += block.size / 4;
                writeData(part, dataStream, gzipStream);
                written += tSize;
                read += fileWritten;
            }
            
            // do single reads
            part = 0;
            dataStream = resetDataStream(part, singleBase, dataStream);
            gzipStream = getNewGZIPStream(dataStream);  
            fileWritten = 0;
            while(sReader.getNextBlock(block)) {
                fileWritten += block.write(gzipStream);
                // check filesize
                tSize = getSize(dataStream);
                if(tSize > MAXFILESIZE) {
                    gzipStream.close();
                    count += block.size / 4;
                    writeData(part, dataStream, gzipStream);
                    written += tSize;
                    read += fileWritten;
                    fileWritten = 0;
                    part++;
                    dataStream = resetDataStream(part, singleBase, dataStream);
                    gzipStream = getNewGZIPStream(dataStream);    
                }
            }
            // finish the files
            gzipStream.close();
            tSize = getSize(dataStream);
            if(tSize == 0 || fileWritten == 0) {
                deleteFile(pairedBase, part);
            } else if(fileWritten != 0) {
                count += block.size / 4;
                writeData(part, dataStream, gzipStream);
                written += tSize;
                read += fileWritten;
            }
            Logger.DEBUG("number of reads: "+ count);
            Logger.DEBUG("read " + round(read / (1024*1024)) + "MB");
            Logger.DEBUG("written " + round(written / (1024*1024)) + "MB");
            closeStreams(dataStream, gzipStream);
        } catch (IOException ex) {
            Logger.EXCEPTION(ex);
        }
    }
}
