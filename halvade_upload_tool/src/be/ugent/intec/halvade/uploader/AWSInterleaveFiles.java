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

package be.ugent.intec.halvade.uploader;

import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.zip.GZIPOutputStream;
import org.apache.hadoop.io.compress.CompressionCodec;

/**
 *
 * @author ddecap
 */
public class AWSInterleaveFiles extends BaseInterleaveFiles {
    protected AWSUploader upl; // S3
    protected CompressionCodec codec;
    
    public AWSInterleaveFiles(String base, long maxFileSize, AWSUploader upl, int thread, CompressionCodec codec) {
        super(base, maxFileSize, thread);
        this.upl = upl;
        this.fsName = "S3";
        this.codec = codec;
        if(codec != null)
            useHadoopCompression = true;
    }

    @Override
    protected OutputStream getNewDataStream(int part, String prefix) throws IOException {
       return new ByteArrayOutputStream();
    }

    @Override
    protected BufferedOutputStream getNewCompressedStream(OutputStream dataStream) throws IOException {
        ((ByteArrayOutputStream)dataStream).reset();
        return new BufferedOutputStream(
                useHadoopCompression ? 
                        codec.createOutputStream(dataStream):
                        new GZIPOutputStream(dataStream), 
                BUFFERSIZE);
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
            upl.Upload(fileBase + part + (useHadoopCompression ? ".fq" + codec.getDefaultExtension() : ".fq.gz"), new ByteArrayInputStream(byteStream.toByteArray()), byteStream.size());
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
