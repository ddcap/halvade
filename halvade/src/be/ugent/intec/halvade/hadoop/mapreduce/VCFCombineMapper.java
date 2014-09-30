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

import fi.tkk.ics.hadoop.bam.VariantContextWritable;
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author ddecap
 */
public class VCFCombineMapper extends Mapper<LongWritable,VariantContextWritable, LongWritable, VariantContextWritable> {

    @Override
    protected void map(LongWritable key, VariantContextWritable value, Context context) throws IOException, InterruptedException {
        // Sort on key with chr idx + pos: key.set((long)chromIdx << 32 | (long)(v.getStart() - 1));
        context.write(key, value);
    }
    
}
