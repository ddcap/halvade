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

package be.ugent.intec.halvade.hadoop.mapreduce;

import be.ugent.intec.halvade.hadoop.datatypes.ChromosomeRegion;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import be.ugent.intec.halvade.tools.BWAMemInstance;
import be.ugent.intec.halvade.utils.Logger;
import fi.tkk.ics.hadoop.bam.SAMRecordWritable;
import java.net.URISyntaxException;
import org.apache.hadoop.io.LongWritable;

/**
 *
 * @author ddecap
 */
public class BWAMemMapper extends HalvadeMapper<ChromosomeRegion, SAMRecordWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        super.map(key, value, context);
        ((BWAMemInstance)instance).feedLine(value.toString());
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        try {
            String binDir = checkBinaries(context); 
            instance = BWAMemInstance.getBWAInstance(context, binDir);
        } catch (URISyntaxException ex) {
            Logger.EXCEPTION(ex);
            throw new InterruptedException();
        }
    }
}