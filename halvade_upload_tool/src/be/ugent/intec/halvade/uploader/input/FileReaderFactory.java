/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package be.ugent.intec.halvade.uploader.input;

import be.ugent.intec.halvade.uploader.Logger;
import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 *
 * @author ddecap
 */
public class FileReaderFactory extends BaseFileReader implements Runnable {
    protected ArrayList<BaseFileReader> readers;
    protected BaseFileReader currentReader = null;
    protected static FileReaderFactory allReaders = null;
    protected int threads;
    protected boolean fromHDFS = false;

    public FileReaderFactory(int threads, boolean fromHDFS) {
        super(false);
        readers = new ArrayList<>();
        this.threads = threads;
        this.fromHDFS = fromHDFS;
    }
    
    public static FileReaderFactory getInstance(int threads, boolean fromHDFS) {
        if(allReaders == null) {
            allReaders = new FileReaderFactory(threads, fromHDFS);
        }
        return allReaders;
    }
    
    public static BaseFileReader createFileReader(boolean fromHDFS, String fileA, String fileB, boolean interleaved) throws IOException {
        if (fileB == null) {
            return new SingleFastQReader(fromHDFS, fileA, interleaved);
        } else {
            return new PairedFastQReader(fromHDFS, fileA, fileB);
        }     
    }
    
    public void addReader(String fileA, String fileB, boolean interleaved) throws IOException {
        readers.add(createFileReader(fromHDFS, fileA, fileB, interleaved));
    }
    
    public void addReader(BaseFileReader reader) {
        readers.add(reader);
    }

    
    public ReadBlock retrieveBlock() {
        try {
            ReadBlock block = null;
            while((check || blocks.size() > 0) && block == null) {
                block = blocks.poll(1000, TimeUnit.MILLISECONDS);
            }
            return block;
        } catch (InterruptedException ex) {
            Logger.EXCEPTION(ex);
            return null;
        }
    }
    
    
    @Override
    protected int addNextRead(ReadBlock block) throws IOException {
        return currentReader.addNextRead(block);
    }
    
    protected synchronized boolean getNextReader() {
        if(currentReader == null) {
            if(readers.size() > 0) {
                currentReader = readers.remove(readers.size() - 1);
                Logger.DEBUG("Reader: " + currentReader);
                return true;
            } else {
                Logger.DEBUG("Processed all readers");
                return false;
            }
        } else return true;
    }


    protected boolean check = true;
    protected ArrayBlockingQueue<ReadBlock> blocks;
    protected int READ_BLOCK_CAPACITY_PER_THREAD = 10;
    
    @Override
    public void run() {
        blocks = new ArrayBlockingQueue<>(READ_BLOCK_CAPACITY_PER_THREAD*threads);
        if(currentReader == null) {
            if(!getNextReader()) check = false;
        }
        try {
            while(check) {
                ReadBlock block = new ReadBlock();
                boolean hasReads = super.getNextBlock(block);
                if (!hasReads) {
                    currentReader = null;
                    if(!getNextReader())
                        check = false;
                } else {
                    blocks.put(block);
                }
            }
        } catch (InterruptedException ex) {
            Logger.EXCEPTION(ex);
        }
    }
}
