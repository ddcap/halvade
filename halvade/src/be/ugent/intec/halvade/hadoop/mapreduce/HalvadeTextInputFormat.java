/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package be.ugent.intec.halvade.hadoop.mapreduce;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

/**
 *
 * @author ddecap
 */
public class HalvadeTextInputFormat extends TextInputFormat {

    @Override
    protected boolean isSplitable(JobContext context, Path file) {
        return false; 
    }
    
}
