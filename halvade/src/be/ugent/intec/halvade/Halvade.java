/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package be.ugent.intec.halvade;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import be.ugent.intec.halvade.utils.Logger;
/**
 *
 * @author ddecap
 * HALVADE: Hadoop ALigner and VAriant DEtection
 */
public class Halvade {


    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        // the mapreduce job
        runMapReduce(args);
    }
    
    public static void runMapReduce(String[] args) {
        int res = 0;
        try {
            Configuration c = new Configuration();
            MapReduceRunner runner = new MapReduceRunner();
            res = ToolRunner.run(c, runner, args);
        } catch (Exception ex) {
            Logger.EXCEPTION(ex);
        }
        System.exit(res);
    }
    
}
