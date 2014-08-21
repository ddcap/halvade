/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
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
