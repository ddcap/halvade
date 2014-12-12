/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package be.ugent.intec.halvade.uploader.input;

import be.ugent.intec.halvade.uploader.Logger;
import static be.ugent.intec.halvade.uploader.input.BaseFileReader.getReader;
import java.io.BufferedReader;
import java.io.IOException;

/**
 *
 * @author ddecap
 */
public class SingleFastQReader extends BaseFileReader {
    protected BufferedReader readerA;
    protected ReadBlock block;
    protected int readsFactor;
    

    public SingleFastQReader(String fileA, boolean interleaved) throws IOException {
        super(true);
        readerA = getReader(fileA);
        this.isInterleaved = interleaved;
        if(isInterleaved) 
            readsFactor = 2;
        else 
            readsFactor = 1;
        toStr = fileA;
        Logger.DEBUG("Single: " + toStr + (isInterleaved ? " interleaved" : ""));
    }

    @Override
    protected int addNextRead(ReadBlock block) throws IOException {
        if(block.checkCapacity(LINES_PER_READ*readsFactor)) {
            block.setCheckPoint();
            boolean check = true;
            int i = 0;
            while(i <LINES_PER_READ*readsFactor && check) {
                check = block.fastAddLine(readerA.readLine());
                i++;
            }
            if(!check)  {
                block.revertToCheckPoint();
                return -1;
            }
            return 0;
        } else 
            return 1;
    }
    
}