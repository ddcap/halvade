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

/**
 *
 * @author ddecap
 */
public class AWSInterleaveFiles extends BaseInterleaveFiles {
    AWSUploader upl; // S3
    
    public AWSInterleaveFiles(String paired, String single, long maxFileSize, AWSUploader upl) {
        super(paired, single, maxFileSize);
        this.upl = upl;
        this.fsName = "S3";
    }

    @Override
    protected OutputStream getNewDataStream(int part, String prefix) throws IOException {
       return new ByteArrayOutputStream();
    }

    @Override
    protected BufferedOutputStream getNewGZIPStream(OutputStream dataStream) throws IOException {
        ((ByteArrayOutputStream)dataStream).reset();
        return new BufferedOutputStream(new GZIPOutputStream(dataStream), BUFFERSIZE);
    }

    @Override
    protected int getSize(OutputStream dataStream) {
        return ((ByteArrayOutputStream)dataStream).size();
    }
    
    @Override
    protected void writeData(int part, OutputStream dataStream, BufferedOutputStream gzipStream) throws IOException {
        ByteArrayOutputStream byteStream = (ByteArrayOutputStream) dataStream;
        try {
            Logger.DEBUG("uploading part " + part + ": " + byteStream.size());
            upl.Upload(pairedBase + part + ".fq.gz", new ByteArrayInputStream(byteStream.toByteArray()), byteStream.size());
            gzipStream.close();
        } catch (InterruptedException ex) {
            Logger.DEBUG("failed to upload part to AWS...");
            Logger.EXCEPTION(ex);
        }
    }
    
    @Override
    protected OutputStream resetDataStream(int part, String prefix, OutputStream dataStream) throws IOException {
        return dataStream;
    }
    
    @Override
    protected void deleteFile(String prefix, int part) throws IOException {
        // no need for S3
    }
    
    @Override
    protected void closeStreams(OutputStream dataStream, OutputStream gzipStream) throws IOException {
        dataStream.close();
        gzipStream.close();
    }
}
