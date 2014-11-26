 /* Copyright (C) 2014 ddecap
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

/**
 *
 * @author ddecap
 */
public enum HalvadeCounters {
    TIME_BWA_ALN,
    TIME_BWA_MEM,
    TIME_BWA_SAMPE,
    TIME_STAR,
    TIME_ELPREP,
    TIME_BEDTOOLS,
    TIME_SAMTOBAM,
    TIME_HADOOP_SAMTOBAM,
    TIME_PICARD_CLEANSAM,
    TIME_PICARD_MARKDUP,
    TIME_PICARD_ADDGRP,
    TIME_PICARD_BAI,    
    TIME_GATK_RECAL,  
    TIME_GATK_TARGET_CREATOR, 
    TIME_GATK_INDEL_REALN, 
    TIME_GATK_PRINT_READS, 
    TIME_GATK_COMBINE_VCF,
    TIME_GATK_VARIANT_CALLER,
    
    IN_BWA_READS,
    IN_PREP_READS,
    
    OUT_BWA_READS,
    OUT_VCF_FILES,
    OUT_UNMAPPED_READS,
    OUT_DIFF_CHR_READS,
    OUT_OVERLAPPING_READS,
    
    FOUT_BWA_TMP,
    FOUT_STAR_TMP,
    FOUT_GATK_TMP,
    FOUT_GATK_VCF,
    FOUT_TO_HDFS,
    
    FIN_FROM_HDFS,
    
    TOOLS_GATK,
    
    STILL_RUNNING_HEARTBEAT
}
