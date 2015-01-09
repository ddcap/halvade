/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package be.ugent.intec.halvade;

import be.ugent.intec.halvade.utils.HalvadeConf;
import be.ugent.intec.halvade.utils.Logger;
import org.apache.hadoop.conf.Configuration;

/**
 *
 * @author ddecap
 */
public class HalvadeResourceManager {
    public static int RNA_SHMEM_PASS1 = 0;
    public static int RNA_SHMEM_PASS2 = 1;
    public static int RNA = 2;
    public static int DNA = 3;
    public static int COMBINE = 4;
    
    protected static final int ALL = Integer.MAX_VALUE;
    protected static final int MEM_AM = (int) (1.5*1024);
    protected static final int VCORES_AM = 1;
    protected static final int MEM_STAR = (int) (12*1024);
    protected static final int[][] RESOURCE_REQ = { 
        //mapmem, mapcpu, redmem, redcpu
        {MEM_STAR,  ALL},     // RNA with shared memory pass1
        {MEM_STAR,  14*1024}, // RNA with shared memory pass2
        {MEM_STAR,  14*1024}, // RNA without shared memory
        {14*1024,   14*1024}, // DNA
        {4*1024,    4*1024}   // combine
    };
    
    public static void setJobResources(HalvadeOptions opt, Configuration conf, int type, boolean subtractAM) {
        int tmpmem = (int) (opt.mem * 1024);
        int tmpvcores = opt.vcores;
        if(subtractAM) {
            tmpmem -= MEM_AM;
            tmpvcores -= VCORES_AM;
        }
        
        if (opt.setMapContainers) 
            opt.mapsPerContainer = Math.min(tmpvcores, Math.max(tmpmem / RESOURCE_REQ[type][0],1));
        if (opt.setReduceContainers) 
            opt.reducersPerContainer = Math.min(tmpvcores, Math.max(tmpmem / RESOURCE_REQ[type][1], 1));
        
        opt.maps = Math.max(1,opt.nodes*opt.mapsPerContainer);
        opt.mthreads = Math.max(1, tmpvcores/opt.mapsPerContainer);
        opt.rthreads = Math.max(1, tmpvcores/opt.reducersPerContainer);
        
        int mmem = RESOURCE_REQ[type][0];
        int rmem = RESOURCE_REQ[type][1] == ALL ? tmpmem : RESOURCE_REQ[type][1];
        
        Logger.DEBUG("resources set to " + opt.mapsPerContainer + " maps [" 
                + opt.mthreads + " cpu , " + mmem + " mb] per node and " 
                + opt.reducersPerContainer + " reducers ["
                + opt.rthreads + " cpu, " + rmem + " mb] per node");
        
        conf.set("mapreduce.map.cpu.vcores", "" + opt.mthreads);
        conf.set("mapreduce.reduce.cpu.vcores", "" + opt.rthreads );
        conf.set("mapreduce.reduce.memory.mb", "" + rmem);
        conf.set("mapreduce.map.memory.mb", "" + mmem); 
        conf.set("mapreduce.reduce.java.opts", "-Xmx" + (int)(0.8*rmem) + "m");
        conf.set("mapreduce.map.java.opts", "-Xmx" + (int)(0.8*mmem) + "m"); 
        conf.set("mapreduce.job.reduce.slowstart.completedmaps", "" + 1.0);
        
        HalvadeConf.setMapThreads(conf, opt.mthreads);
        HalvadeConf.setReducerThreads(conf, opt.rthreads);     
    }
}
