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
import java.io.IOException;
import java.io.OutputStream;
import java.util.zip.GZIPOutputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;

/**
 *
 * @author ddecap
 */
public class HDFSInterleaveFiles extends BaseInterleaveFiles {
    protected FileSystem fs; // HDFS or others (lustre,gpfs)
    protected CompressionCodec codec;
    
    public HDFSInterleaveFiles(String base, long maxFileSize, FileSystem fs, int thread, CompressionCodec codec) {
        super(base, maxFileSize, thread);
        this.fs = fs;
        this.fsName = "HDFS";
        this.codec = codec;
        if(codec != null)
            useHadoopCompression = true;
    }

    @Override
    protected OutputStream getNewDataStream(int part, String prefix) throws IOException {
       return fs.create(new Path(prefix + part +
                (useHadoopCompression ? ".fq" + codec.getDefaultExtension() : ".fq.gz")),true);
    }

    @Override
    protected BufferedOutputStream getNewCompressedStream(OutputStream dataStream) throws IOException {
        return new BufferedOutputStream(
                useHadoopCompression ? 
                        codec.createOutputStream(dataStream):
                        new GZIPOutputStream(dataStream), 
                BUFFERSIZE);
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
        return fs.create(new Path(prefix + part + 
                (useHadoopCompression ? ".fq" + codec.getDefaultExtension() : ".fq.gz")),true);
    }
    
    @Override
    protected void deleteFile(String prefix, int part) throws IOException {
        fs.delete(new Path(prefix + part + 
                (useHadoopCompression ? ".fq" + codec.getDefaultExtension() : ".fq.gz")), true);
    }
    
    @Override
    protected void closeStreams(OutputStream dataStream, OutputStream gzipStream) throws IOException {
        dataStream.close();
        gzipStream.close();
    }
}
