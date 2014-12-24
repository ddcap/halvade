/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package be.ugent.intec.halvade;

import be.ugent.intec.halvade.utils.HalvadeConf;
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
    private static final int ALL = Integer.MAX_VALUE;
    private static final int[][] RESOURCE_REQ = { 
        //mapmem, mapcpu, redmem, redcpu
        {4*1024,     4,  ALL,       ALL}, // RNA with shared memory pass1
        {4*1024,     4,  14*1024,     4}, // RNA with shared memory pass2
        {34*1024,    4,  14*1024,     4}, // RNA without shared memory
        {14*1024,    8,  14*1024,     4}  // DNA
    };
    private static final int STAR_GENOME_SIZE = (int) (2.5*1024);
    private static final int MEM_RESERVED_FOR_AM = (int) (1.5*1024);
    private static final int VCORES_RESERVED_FOR_AM = 1;
    
    public static void setJobResources(HalvadeOptions opt, Configuration conf, int type) {
        int tmpmem = opt.mem * 1024 - MEM_RESERVED_FOR_AM;
        int tmpvcores = opt.vcores - VCORES_RESERVED_FOR_AM;
        
        if (opt.mapsPerContainer == -1) 
            opt.mapsPerContainer = Math.min(Math.max(tmpvcores / RESOURCE_REQ[type][1],1), 
                                        Math.max(((opt.rnaPipeline && opt.useSharedMemory) ? tmpmem - STAR_GENOME_SIZE : tmpmem) / 
                                                    RESOURCE_REQ[type][0],1));
        if (opt.reducersPerContainer == -1) 
            opt.reducersPerContainer = Math.min(Math.max(tmpvcores / RESOURCE_REQ[type][3], 1), 
                                            Math.max(tmpmem / RESOURCE_REQ[type][2], 1));
        opt.mappers = Math.max(1,opt.nodes*opt.mapsPerContainer);
        opt.mthreads = Math.max(1,opt.vcores/opt.mapsPerContainer);
        opt.rthreads = Math.max(1,opt.vcores/opt.reducersPerContainer);
        
        int mmem = Math.min(tmpmem,tmpmem/opt.mapsPerContainer);
        int rmem = Math.min(tmpmem,tmpmem/opt.reducersPerContainer);
        
        be.ugent.intec.halvade.utils.Logger.DEBUG("using " + opt.mapsPerContainer + " maps [" 
                + opt.mthreads + " cpu , " + mmem + " mb] per node and " 
                + opt.reducersPerContainer + " reducers ["
                + opt.rthreads + " cpu, " + rmem + " mb] per node");
        
        conf.set("mapreduce.map.cpu.vcores", "" + opt.mthreads);
        conf.set("mapreduce.map.memory.mb", "" + mmem); 
        conf.set("mapreduce.reduce.cpu.vcores", "" + opt.rthreads );
        conf.set("mapreduce.reduce.memory.mb", "" + rmem);
        conf.set("mapreduce.job.reduce.slowstart.completedmaps", "" + 1.0);
        
        HalvadeConf.setMapThreads(conf, opt.mthreads);
        HalvadeConf.setReducerThreads(conf, opt.rthreads);     
    }
}
