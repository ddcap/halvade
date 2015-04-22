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

package be.ugent.intec.halvade.uploader.input;

import java.io.IOException;
import java.io.OutputStream;

/**
 *
 * @author ddecap
 */
public class ReadBlock {
    private static final int DEFAULTCAP = 50000; // 50000 ~ only 3MB unzipped -> more accurate file sizes
    int capacity;
    int size;
    int lastSize = -1;
    String[] reads;
    
    public ReadBlock(int capacity) {
        this.capacity = capacity;
        reads = new String[this.capacity];
    }
    
    public ReadBlock() {
        capacity = DEFAULTCAP;
        reads = new String[capacity];
    }
    
    public void reset() {
//        reads = new String[this.capacity];
        size = 0;
    }

    public int getSize() {
        return size;
    }
    
    public long write(OutputStream outStream) throws IOException {
        long bytes = 0;
        int len;
        for(int i = 0; i < size; i++) {
            len = reads[i].length();
            bytes += len + 1;
            outStream.write((reads[i] + "\n").getBytes(), 0, len + 1);
        }
        return bytes;
    }
    
    public boolean secureAddLine(String line) throws IOException {
        if(size < capacity && line != null) {
            reads[size] = line;
            size++;
            return true;
        } else
            return false;   
    }
    
    public boolean fastAddLine(String line) throws IOException {
        if(line == null) return false;
        reads[size] = line;
        size++;
        return true;
    }
    
    public void setCheckPoint() {
        lastSize = size;
    }
    
    public boolean revertToCheckPoint() {
        if(lastSize != -1) {
            size = lastSize; 
            return true;
        } else 
            return false;
    }

    public boolean checkCapacity(int lineBlockCount) {
        return size + lineBlockCount <= capacity;
    }
}
