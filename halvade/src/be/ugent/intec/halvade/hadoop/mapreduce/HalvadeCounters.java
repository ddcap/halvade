/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
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
    TIME_IPREP,
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
    
    OUT_BWA_READS,
    OUT_PREP_READS,
    OUT_VCF_FILES,
    OUT_UNMAPPED_READS,
    OUT_DIFF_CHR_READS,
    OUT_OVERLAPPING_READS,
    
    FOUT_BWA_TMP,
    FOUT_GATK_TMP,
    FOUT_GATK_VCF,
    FOUT_TO_HDFS,
    
    FIN_FROM_HDFS,
    
    TOOLS_GATK
    
}
