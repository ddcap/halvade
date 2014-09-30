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
