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
    public static int DNA = 2;
    public static int COMBINE = 3;
    
    protected static final int ALL = Integer.MAX_VALUE;
    protected static final int MEM_AM = (int) (1.5*1024);
    protected static final int VCORES_AM = 1;
    protected static final int MEM_ALN = (int) (10*1024);
    protected static final int MEM_STAR = (int) (16*1024); // 16 for hg -> reduced reference
    protected static final int MEM_REF = (int) (6*1024); // 4g for hg fasta 
    protected static final int MEM_ELPREP = (int) (16*1024); // 16g for hg for 50x coverage
    // gatk 2-4gb per thread!
    protected static final int[][] RESOURCE_REQ = { 
        //mapmem, redmem
        {MEM_STAR,  ALL},     // RNA with shared memory pass1
        {MEM_STAR,  MEM_ELPREP}, // RNA with shared memory pass2
        {MEM_ALN,   MEM_ELPREP}, // DNA
        {4*1024,    4*1024}   // combine
    };
    
    public static void setJobResources(HalvadeOptions opt, Configuration conf, int type, boolean subtractAM, boolean BAMinput) throws InterruptedException {
        int tmpmem = (int) (opt.mem * 1024);
        int tmpvcores = opt.vcores;
        
        BAMinput = BAMinput && type < 3;
        int mmem = RESOURCE_REQ[BAMinput? 3 : type][0];
        int rmem = RESOURCE_REQ[type][1] == ALL ? tmpmem - MEM_AM : RESOURCE_REQ[type][1];
        if (rmem == MEM_ELPREP && !opt.useElPrep) 
            rmem = MEM_REF;
        if((opt.overrideMapMem > 0 || opt.overrideRedMem > 0)&& type != COMBINE) {
            if(!BAMinput && opt.overrideMapMem > 0)
                mmem = opt.overrideMapMem;
            if(opt.overrideRedMem > 0)
                rmem = opt.overrideRedMem;
        }
        if(mmem > opt.mem*1024 || rmem > opt.mem*1024)
            throw new InterruptedException("Not enough memory available on system; memory requirements: " + opt.mem*1024 + "/" + Math.max(rmem, mmem));
        if (opt.setMapContainers)
            opt.mapContainersPerNode = Math.min(tmpvcores, Math.max(tmpmem / mmem,1));
        if (opt.setReduceContainers) 
            opt.reducerContainersPerNode = Math.min(tmpvcores, Math.max(tmpmem / rmem, 1));
        
        HalvadeConf.setMapContainerCount(conf, opt.maps);
        HalvadeConf.setVcores(conf, opt.vcores);
        opt.mthreads = Math.max(1, tmpvcores/opt.mapContainersPerNode);
        if(opt.mthreads > 1 && opt.mthreads % 2 == 1 ) {
            opt.mthreads++;
            opt.mapContainersPerNode = Math.min(Math.max(tmpvcores/opt.mthreads,1), Math.max(tmpmem / mmem,1));
        }
        opt.rthreads = Math.max(1, tmpvcores/opt.reducerContainersPerNode);
        if(opt.smtEnabled) {
            opt.mthreads *=2;
            opt.rthreads *=2;
        }
        opt.maps = Math.max(1,opt.nodes*opt.mapContainersPerNode);
        Logger.DEBUG("set # map containers: " + opt.maps);        
       	HalvadeConf.setMapContainerCount(conf, opt.maps); 
        
        Logger.DEBUG("resources set to " + opt.mapContainersPerNode + " maps [" 
                + opt.mthreads + " cpu , " + mmem + " mb] per node and " 
                + opt.reducerContainersPerNode + " reducers ["
                + opt.rthreads + " cpu, " + rmem + " mb] per node");
        
        conf.set("mapreduce.map.cpu.vcores", "" + opt.mthreads);
        conf.set("mapreduce.map.memory.mb", "" + mmem); 
        if(subtractAM) 
            conf.set("mapreduce.reduce.cpu.vcores", "" + (opt.rthreads - VCORES_AM) );
        else 
            conf.set("mapreduce.reduce.cpu.vcores", "" + opt.rthreads );
        conf.set("mapreduce.reduce.memory.mb", "" + rmem);        
        conf.set("mapreduce.reduce.java.opts", "-Xmx" + (int)(0.18*rmem) + "m");
        conf.set("mapreduce.map.java.opts", "-Xmx" + (int)(0.18*mmem) + "m");
        conf.set("mapreduce.job.reduce.slowstart.completedmaps", "0.99");
        
        HalvadeConf.setMapThreads(conf, opt.mthreads);
        HalvadeConf.setReducerThreads(conf, opt.rthreads);  
    }
}
