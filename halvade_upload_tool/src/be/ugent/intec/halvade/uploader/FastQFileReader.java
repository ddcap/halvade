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

import java.io.*;
import java.util.LinkedList;
import java.util.Queue;
import java.util.zip.GZIPInputStream;
import org.apache.tools.bzip2.CBZip2InputStream;

/**
 *
 * @author ddecap
 */
public class FastQFileReader {
    private static final int BUFFERSIZE = 128*1024;
    
    private static BufferedReader getReader(String file) throws FileNotFoundException, IOException {
        if(file.endsWith(".gz")) {
            GZIPInputStream gzip = new GZIPInputStream(new FileInputStream(file), BUFFERSIZE); 
            return new BufferedReader(new InputStreamReader(gzip));
        } else if(file.endsWith(".bz2")) {
            CBZip2InputStream bzip2 = new CBZip2InputStream(new FileInputStream(file));
            return new BufferedReader(new InputStreamReader(bzip2));
        } else
            return new BufferedReader(new FileReader(file));            
    }    
    protected interface ReadFile {
        public BufferedReader getCurrentReader();
    }
    protected class StringPair {
        public StringPair(String file1, String file2) {
            this.file1 = file1;
            this.file2 = file2;
        }
        public String file1;
        public String file2;
    }
    protected class PairedReadFile implements ReadFile {
        protected BufferedReader reader1;
        protected BufferedReader reader2;
        int toggle;
        public PairedReadFile(StringPair pair) throws IOException {
            Logger.DEBUG("Paired: " + pair.file1 + " & " + pair.file2);
            reader1 = FastQFileReader.getReader(pair.file1);
            reader2 = FastQFileReader.getReader(pair.file2);
            toggle = 1;
        }

        @Override
        public BufferedReader getCurrentReader() {
            toggle = (toggle + 1) % 2;
            if (toggle == 0) return reader1;
            else return reader2;
        }
    }
    protected class SingleReadFile implements ReadFile {
        protected BufferedReader reader1;
        public SingleReadFile(String file1) throws IOException {
            Logger.DEBUG("Single: " + file1 );
            reader1 = FastQFileReader.getReader(file1);
        }

        @Override
        public BufferedReader getCurrentReader() {
            return reader1;
        }
    }
    
    protected static FastQFileReader pReader = null;
    protected static FastQFileReader sReader = null;
    protected Queue <StringPair > pairedFiles;
    protected Queue <String> singleFiles;
    protected ReadFile currentFile = null;
    
    protected FastQFileReader() {
        pairedFiles = new LinkedList<>();
        singleFiles = new LinkedList<>();
    }
    
    public static FastQFileReader getPairedInstance() {
        if(pReader == null) {
            pReader = new FastQFileReader();
        }
        return pReader;
    }
    
    public static FastQFileReader getSingleInstance() {
        if(sReader == null) {
            sReader = new FastQFileReader();
        }
        return sReader;
    }
    
    public void addFilePair(String file1, String file2) {
        pairedFiles.add(new StringPair(file1, file2));
    }
    
    public void addSingleFile(String file1) {
        singleFiles.add(file1);
    }
        
    public synchronized boolean getNextBlock(ReadBlock block) {
        block.reset();
        try {
            while(getNextRead(block)) {
            }
        } catch (IOException ex) {
            Logger.EXCEPTION(ex);
        }
        if(block.size == 0) 
            return false;
        else 
            return true;
    }
    
    private boolean getNextRead(ReadBlock block) throws IOException {
        if(currentFile == null && !getNextFile())
            return false;
        int val = block.addRead(currentFile.getCurrentReader());
        if(val == 0)
            return true;
        // file is empty
        if(val == -1) {
            if(!getNextFile())
                return false;
            else
                return block.addRead(currentFile.getCurrentReader()) == 0;
        } else return false;
            
    }
    
    private boolean getNextFile() throws IOException {
        if (!pairedFiles.isEmpty()) {
            currentFile = new PairedReadFile(pairedFiles.remove());
            return true;
        } else if(!singleFiles.isEmpty()) {
            currentFile = new SingleReadFile(singleFiles.remove());
            return true;
        } else {
            currentFile = null;
            return false;
        }
    }
}
