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
public class HDFSInterleaveFiles extends BaseInterleaveFiles {
    FileSystem fs; // HDFS
    
    public HDFSInterleaveFiles(String paired, String single, long maxFileSize, FileSystem fs) {
        super(paired, single, maxFileSize);
        this.fs = fs;
        this.fsName = "HDFS";
    }

    @Override
    protected OutputStream getNewDataStream(int part, String prefix) throws IOException {
       return fs.create(new Path(prefix + part + ".fq.gz"),true);
    }

    @Override
    protected BufferedOutputStream getNewGZIPStream(OutputStream dataStream) throws IOException {
        return new BufferedOutputStream(new GZIPOutputStream(dataStream), BUFFERSIZE);
    }

    @Override
    protected int getSize(OutputStream dataStream) {
        return ((FSDataOutputStream)dataStream).size();
    }
    
    @Override
    protected void writeData(int part, OutputStream dataStream, BufferedOutputStream gzipStream) throws IOException {
        gzipStream.close();
        dataStream.close();
    }
    
    @Override
    protected OutputStream resetDataStream(int part, String prefix, OutputStream dataStream) throws IOException {
        return fs.create(new Path(prefix + part + ".fq.gz"),true);
    }
    
    @Override
    protected void deleteFile(String prefix, int part) throws IOException {
        fs.delete(new Path(prefix + part + ".fq.gz"), true);
    }
    
    @Override
    protected void closeStreams(OutputStream dataStream, OutputStream gzipStream) throws IOException {
        dataStream.close();
        gzipStream.close();
    }
}
