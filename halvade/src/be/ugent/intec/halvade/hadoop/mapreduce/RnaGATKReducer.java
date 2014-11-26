/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package be.ugent.intec.halvade.hadoop.mapreduce;

import be.ugent.intec.halvade.tools.GATKTools;
import be.ugent.intec.halvade.tools.PreprocessingTools;
import be.ugent.intec.halvade.tools.QualityException;
import be.ugent.intec.halvade.utils.ChromosomeRange;
import be.ugent.intec.halvade.utils.Logger;
import be.ugent.intec.halvade.utils.HalvadeConf;
import be.ugent.intec.halvade.utils.SAMRecordIterator;
import fi.tkk.ics.hadoop.bam.SAMRecordWritable;
import java.io.IOException;
import java.net.URISyntaxException;

/**
 *
 * @author ddecap
 */
public class RnaGATKReducer extends GATKReducer {
    
    @Override
    protected void processAlignments(Iterable<SAMRecordWritable> values, Context context, PreprocessingTools tools, GATKTools gatk) throws IOException, InterruptedException, URISyntaxException, QualityException {
        long startTime = System.currentTimeMillis();
        // temporary files
        String region = tmpFileBase + "-region.intervals";
        String preprocess = tmpFileBase + ".bam";
        String tmpFile1 = tmpFileBase + "-2.bam";
        String tmpFile2 = tmpFileBase + "-3.bam";
        String tmpFile3 = tmpFileBase + "-4.bam";
        String snps = tmpFileBase + ".vcf";    
//        String filteredSnps = tmpFileBase + "-filtered.vcf";    
//        String annotatedSnps = tmpFileBase + "-annotated.vcf";   
        
        boolean useElPrep = HalvadeConf.getUseIPrep(context.getConfiguration());
        ChromosomeRange r = new ChromosomeRange();
        SAMRecordIterator SAMit = new SAMRecordIterator(values.iterator(), header, r);
        
        if(useElPrep)
            elPrepPreprocess(context, tools, SAMit, preprocess);
        else 
            PicardPreprocess(context, tools, SAMit, preprocess);
        region = makeRegionFile(context, r, tools, region);
        if(region == null) return;
        
        splitNTrim(context, region, gatk, preprocess, tmpFile1);
        indelRealignment(context, region, gatk, tmpFile1, tmpFile2);        
        baseQualityScoreRecalibration(context, region, r, tools, gatk, tmpFile2, tmpFile3);        
        RnaVariantCalling(context, region, gatk, tmpFile3, snps);  
        variantFiles.add(snps);
        
        // filter/annotate??        
//        windows = 35;
//        cluster = 3;
//        minFS = 30.0;
//        maxQD = 2.0;
//        annotateVariants(context, region, gatk, snps, annotatedSnps);
//        filterVariants(context, gatk, annotatedSnps, filteredSnps);     
         
        removeLocalFile(region);
        long estimatedTime = System.currentTimeMillis() - startTime;
        Logger.DEBUG("total estimated time: " + estimatedTime / 1000);
    }
    
}