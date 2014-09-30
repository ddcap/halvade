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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.OutputStream;

/**
 *
 * @author ddecap
 */
public class ReadBlock {
    private static final int DEFAULTCAP = 50000; 
    int capacity;
    int size;
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
        size = 0;
    }
    
    public long write(OutputStream outStream) throws IOException {
        long bytes = 0;
        int len;
        for(int i = 0; i < size; i++) {
            len = reads[i].length();
            bytes += len;
            outStream.write((reads[i] + "\n").getBytes(), 0, len + 1);
        }
        return bytes;
    }

    public int addRead(BufferedReader data) throws IOException {
        if(size + 4 <= capacity) {
            int sizeBefore = size;
            for(int i = 0; i < 4; i++) {
                reads[size] = data.readLine();
                if(reads[size] == null) {
                    size = sizeBefore;
                    return -1;
                }
                size ++;
            }
            return 0;
        } else 
            return 1;
    }
}
