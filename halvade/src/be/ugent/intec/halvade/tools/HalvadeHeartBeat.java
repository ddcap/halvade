/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package be.ugent.intec.halvade.tools;

import be.ugent.intec.halvade.hadoop.mapreduce.HalvadeCounters;
import be.ugent.intec.halvade.utils.Logger;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;


/**
 *
 * @author ddecap
 */
public class HalvadeHeartBeat extends Thread {
    protected int loop = 60;
    protected int interval = 1000;
    protected TaskInputOutputContext context;
    protected boolean stopBeating = false;

    public HalvadeHeartBeat(TaskInputOutputContext context) {
        this.context = context;
    }   
    
    public HalvadeHeartBeat(TaskInputOutputContext context, int interval) {
        this.interval = interval;
        this.context = context;
    }
    
    @Override
    public void run() {
        try {
            int i, count = 0;
            while(!stopBeating) {
                i = 0;
                while(!stopBeating && i < loop) {
                    Thread.sleep(interval);
                    i++;
                }
                context.setStatus("Heartbeat: " + count);
                context.getCounter(HalvadeCounters.STILL_RUNNING_HEARTBEAT).increment(1);
//                context.progress();
//                Logger.DEBUG("Heartbeat");
                count++;
            }
        } catch (InterruptedException ex) {
            Logger.EXCEPTION(ex);
        }
        
    }

    void jobFinished() {
        stopBeating = true;
    }
}
