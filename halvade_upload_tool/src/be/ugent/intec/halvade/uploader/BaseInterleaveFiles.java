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

import be.ugent.intec.halvade.uploader.input.ReadBlock;
import be.ugent.intec.halvade.uploader.input.FileReaderFactory;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 *
 * @author ddecap
 */
abstract class BaseInterleaveFiles extends Thread {
    protected static final int BUFFERSIZE = 8*1024;
    protected static long maxFileSize; // ~60MB
    protected FileReaderFactory factory;
    protected String fileBase;
    protected long read, written, count;
    protected String fsName;
    protected int thread;
    protected boolean useHadoopCompression = false;
    
    public BaseInterleaveFiles(String base, long maxFileSize, int thread) {
        this.fileBase = base;
        this.thread = thread;
        written = 0;
        read = 0;
        count = 0;
        factory = FileReaderFactory.getInstance(thread);
        BaseInterleaveFiles.maxFileSize = maxFileSize;
    }

    protected double round(double value) {
        return (int)(value * 100 + 0.5) / 100.0;
    }

    protected abstract OutputStream getNewDataStream(int part, String prefix) throws IOException;
    protected abstract BufferedOutputStream getNewCompressedStream(OutputStream dataStream) throws IOException;
    protected abstract int getSize(OutputStream dataStream);
    protected abstract void writeData(int part, OutputStream dataStream, BufferedOutputStream gzipStream) throws IOException;
    protected abstract OutputStream resetDataStream(int part, String prefix, OutputStream dataStream) throws IOException;
    protected abstract void deleteFile(String prefix, int part) throws IOException;
    protected abstract void closeStreams(OutputStream dataStream, OutputStream gzipStream) throws IOException;

    @Override
    public void run() {
        try {
            Logger.DEBUG("Starting thread " + thread + " to write reads to " + fsName);
            
            int part = 0, tSize;  
            long fileWritten = 0;  
            OutputStream dataStream = getNewDataStream(part, fileBase);
            BufferedOutputStream gzipStream = getNewCompressedStream(dataStream);
            
            
            fileWritten = 0;
            ReadBlock block = factory.retrieveBlock();
            while(block != null) {
                fileWritten += block.write(gzipStream);
                count += block.getSize();
                tSize = getSize(dataStream);
                if(tSize > maxFileSize) {
                    gzipStream.close();
                    writeData(part, dataStream, gzipStream);
                    Logger.DEBUG("Thread " + thread + " wrote " + count + " lines to dfs");
                    written += tSize;
                    read += fileWritten;
                    fileWritten = 0;
                    part++;
                    dataStream = resetDataStream(part, fileBase, dataStream);
                    gzipStream = getNewCompressedStream(dataStream);                 
                }
                block = factory.retrieveBlock();
            }
            // finish the files          
            gzipStream.close();
            tSize = getSize(dataStream);
            if(tSize == 0 || fileWritten == 0) {
                deleteFile(fileBase, part);
            } else if(fileWritten != 0) {
                writeData(part, dataStream, gzipStream);
//                Logger.DEBUG("Thread " + thread + " written " + count + " lines to dfs");
                written += tSize;
                read += fileWritten;
            }
            Logger.DEBUG("Thread " + thread + " read " + count + " lines");
            Logger.DEBUG("Thread " + thread + " read " + round(read / (1024*1024)) + "MB");
            Logger.DEBUG("Thread " + thread + " wrote " + round(written / (1024*1024)) + "MB");
            closeStreams(dataStream, gzipStream);
        } catch (IOException ex) {
            Logger.EXCEPTION(ex);
        }
    }
}
