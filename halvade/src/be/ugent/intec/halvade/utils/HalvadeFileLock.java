/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package be.ugent.intec.halvade.utils;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;

/**
 *
 * @author ddecap
 */
public class HalvadeFileLock {    
    protected File lockfile;
    protected FileChannel f;
    protected RandomAccessFile file;
    protected FileLock lock;
    protected String dir, filename;
    
    public HalvadeFileLock(String dir, String filename) {
        this.dir = dir;
        this.filename = filename;
    }
    
    public int read(ByteBuffer bytes) throws IOException {
        return f.read(bytes);
    } 
    public void forceWrite(ByteBuffer bytes) throws IOException {
        f.write(bytes, 0);
        f.force(false);
    }
    
    public void getLock() throws IOException, InterruptedException {
        lockfile = new File(dir, filename);
        file = new RandomAccessFile(lockfile, "rw");
        f = file.getChannel();
        lock = f.tryLock();  
        int loop = 60;
        int i =0;
        while(lock == null) {
            if (i % loop == 0) Logger.DEBUG("waiting for lock...");
            Thread.sleep(1000);
            i++;
            lock = f.tryLock();
        }
    }
    
    public void removeAndReleaseLock() throws IOException {
        lockfile.deleteOnExit();
        if (lock != null && lock.isValid())
          lock.release();
        if (file != null)
          file.close();
    }
    
    public void releaseLock() throws IOException {
        if (lock != null && lock.isValid())
          lock.release();
        if (file != null)
          file.close();
    }
}
