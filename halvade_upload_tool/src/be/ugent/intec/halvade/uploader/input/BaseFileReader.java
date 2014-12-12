/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package be.ugent.intec.halvade.uploader.input;

import be.ugent.intec.halvade.uploader.Logger;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.zip.GZIPInputStream;
import org.apache.tools.bzip2.CBZip2InputStream;

/**
 *
 * @author ddecap
 */
public abstract class BaseFileReader {
    protected static final int BUFFERSIZE = 128*1024;
    protected static final int LINES_PER_READ = 4;
    protected long count;
    protected boolean isPaired = true;
    protected boolean isInterleaved = false;
    protected String toStr;
    
    protected BaseFileReader(boolean paired) {
        count = 0;
        isPaired = paired;
    }

    @Override
    public String toString() {
        return toStr;
    }
    
    protected static BufferedReader getReader(String file) throws FileNotFoundException, IOException {
        if(file.endsWith(".gz")) {
            GZIPInputStream gzip = new GZIPInputStream(new FileInputStream(file), BUFFERSIZE); 
            return new BufferedReader(new InputStreamReader(gzip));
        } else if(file.endsWith(".bz2")) {
            CBZip2InputStream bzip2 = new CBZip2InputStream(new FileInputStream(file));
            return new BufferedReader(new InputStreamReader(bzip2));
        } else if(file.equals("-")) {
            return new BufferedReader(new InputStreamReader(System.in));
        }else 
            return new BufferedReader(new FileReader(file));            
    }  

    @Override
    public void finalize() throws Throwable {
        super.finalize(); 
        if(count > 0) Logger.DEBUG("Total # reads: " + count);
    }
    
    public synchronized boolean getNextBlock(ReadBlock block) {
        block.reset();
        try {
            while(addNextRead(block) == 0) {
            }
            count += block.size / LINES_PER_READ;
        } catch (IOException ex) {
            Logger.EXCEPTION(ex);
        }
        if(block.size == 0) {
            return false;
        } else 
            return true;
    }
    
    protected abstract int addNextRead(ReadBlock block) throws IOException;
}
