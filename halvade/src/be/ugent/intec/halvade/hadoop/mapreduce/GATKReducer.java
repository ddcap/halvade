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

import be.ugent.intec.halvade.utils.SAMRecordIterator;
import org.seqdoop.hadoop_bam.SAMRecordWritable;
import be.ugent.intec.halvade.hadoop.datatypes.ChromosomeRegion;
import java.io.File;
import java.io.IOException;
import htsjdk.samtools.*;
import org.apache.hadoop.fs.FileSystem;
import be.ugent.intec.halvade.tools.GATKTools;
import be.ugent.intec.halvade.tools.PreprocessingTools;
import be.ugent.intec.halvade.tools.ProcessException;
import be.ugent.intec.halvade.tools.QualityException;
import be.ugent.intec.halvade.utils.ChromosomeRange;
import be.ugent.intec.halvade.utils.HalvadeFileUtils;
import be.ugent.intec.halvade.utils.HalvadeConf;
import be.ugent.intec.halvade.utils.Logger;
import java.net.URI;
import java.net.URISyntaxException;

/**
 *
 * @author ddecap
 */
public abstract class GATKReducer extends HalvadeReducer {
    protected boolean isFirstAttempt;
    protected boolean useBedTools;
    protected boolean useUnifiedGenotyper;
    protected double sec, scc;
    protected String exomeBedFile;   
    protected int newMaxQualScore = 60;
    protected int windows, cluster;
    protected double minFS, maxQD;
    protected boolean isRNA;
    protected boolean redistribute;
    protected int containers;
    protected int tasksLeft;
    protected String gff;
    
    @Override
    protected void reduce(ChromosomeRegion key, Iterable<SAMRecordWritable> values, Context context) throws IOException, InterruptedException {
        super.reduce(key, values, context);
        try {
            Logger.DEBUG("Processing key: " + key);        
            // wrappers to call external programs
            PreprocessingTools tools = new PreprocessingTools(bin);
            GATKTools gatk = new GATKTools(ref, bin);
            gatk.setContext(context);
            tools.setContext(context);
            gatk.setThreads(threads);
            if(java !=null) {
                gatk.setJava(java);
                tools.setJava(java);
            }
            processAlignments(values, context, tools, gatk);
        } catch (URISyntaxException | QualityException | ProcessException ex) {
            Logger.EXCEPTION(ex);
            throw new InterruptedException(ex.getMessage());
        }
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        isFirstAttempt = taskId.endsWith("_0");
        isRNA = HalvadeConf.getIsRNA(context.getConfiguration());
        scc = HalvadeConf.getSCC(context.getConfiguration(), isRNA);
        sec = HalvadeConf.getSEC(context.getConfiguration(), isRNA);
        try {
            gff = HalvadeFileUtils.downloadGFF(context, taskId);
        } catch (URISyntaxException ex) {
            Logger.EXCEPTION(ex);
            throw new InterruptedException("Error when downloading GFF file");
        }
        exomeBedFile = HalvadeConf.getBed(context.getConfiguration());
        useBedTools = HalvadeConf.getUseBedTools(context.getConfiguration());
        useUnifiedGenotyper = HalvadeConf.getUseUnifiedGenotyper(context.getConfiguration());
        redistribute = HalvadeConf.getRedistribute(context.getConfiguration());
        containers = HalvadeConf.getMapContainerCount(context.getConfiguration());
        tasksLeft = Integer.parseInt(context.getConfiguration().get("mapred.map.tasks")) - taskNr;
        // get task number: 
        if(redistribute && tasksLeft < containers) {
            threads = 6;
        }
    }
    

    protected abstract void processAlignments(Iterable<SAMRecordWritable> values, Context context, PreprocessingTools tools, GATKTools gatk) 
            throws IOException, InterruptedException, URISyntaxException, QualityException;

    
    protected void elPrepPreprocess(Context context, PreprocessingTools tools, SAMRecordIterator input, String output) throws InterruptedException, IOException, QualityException, URISyntaxException {
        String dictF = ref.substring(0, ref.lastIndexOf('.')) + ".dict";
        String rg = createReadGroupRecordString(RGID, RGLB, RGPL, RGPU, RGSM);
        String preSamOut = tmpFileBase + "-p1.sam";
        String samOut = tmpFileBase + "-p2.sam";
        String fCounts = tmpFileBase + "-features.count";
        
        outHeader = header.clone();
        outHeader.setSortOrder(SAMFileHeader.SortOrder.coordinate);
        
        Logger.DEBUG("call elPrep");
        context.setStatus("call elPrep");
        int reads;
        if(keep) 
            reads = tools.callElPrep(preSamOut, samOut, inputIsBam ? null : rg, threads, input, outHeader, dictF);
        else
            reads = tools.streamElPrep(context, samOut, inputIsBam ? null : rg, threads, input, outHeader, dictF);
        
        Logger.DEBUG(reads + " reads processed in elPrep");
        context.getCounter(HalvadeCounters.IN_PREP_READS).increment(reads);
        
        if(gff != null) {      
            Logger.DEBUG("featureCounts");
            context.setStatus("featureCounts");
            tools.runFeatureCounts(gff, samOut, fCounts, threads);
            HalvadeFileUtils.uploadFileToHDFS(context, FileSystem.get(new URI(outputdir), context.getConfiguration()),
                        fCounts, outputdir + context.getTaskAttemptID().toString() + ".count");
        }
        context.setStatus("convert SAM to BAM");
        Logger.DEBUG("convert SAM to BAM");
        tools.callSAMToBAM(samOut, output, threads);
        context.setStatus("build bam index");
        Logger.DEBUG("build bam index");
        tools.runBuildBamIndex(output);
        // remove temporary files
        HalvadeFileUtils.removeLocalFile(keep, preSamOut, context, HalvadeCounters.FOUT_GATK_TMP);
        HalvadeFileUtils.removeLocalFile(keep, samOut, context, HalvadeCounters.FOUT_GATK_TMP);
        HalvadeFileUtils.removeLocalFile(keep, fCounts);
    }
    
    protected void PicardPreprocess(Context context, PreprocessingTools tools, SAMRecordIterator input, String output) throws InterruptedException, QualityException, IOException, URISyntaxException {
        outHeader = header.clone();
        outHeader.setSortOrder(SAMFileHeader.SortOrder.coordinate);
        // tmp files
        String tmpOut1 = tmpFileBase + "-p1.bam";
        String tmpOut2 = tmpFileBase + "-p2.bam";
        String tmpOut3 = tmpFileBase + "-p3.sam"; // test with 1 sam output for htseq count and then back to bam!
        String fCounts = tmpFileBase + "-features.count";
        String tmpMetrics = tmpFileBase + "-p3-metrics.txt";
        SAMFileWriterFactory factory = new SAMFileWriterFactory();
        if(!inputIsBam)
            outHeader.addReadGroup(bamrg);
        SAMFileWriter writer = factory.makeBAMWriter(outHeader, true, new File(tmpOut1));
        
        long startTime = System.currentTimeMillis();
        
        int count = 0;
        SAMRecord sam;
        while(input.hasNext()) {
            sam = input.next();
            writer.addAlignment(sam);
            count++;
        }
        int reads = input.getCount();
        writer.close();
        
        context.getCounter(HalvadeCounters.IN_PREP_READS).increment(reads);
        long estimatedTime = System.currentTimeMillis() - startTime;
        context.getCounter(HalvadeCounters.TIME_HADOOP_SAMTOBAM).increment(estimatedTime);
        Logger.DEBUG("time writing " + count + " records to disk: " + estimatedTime / 1000);
        
        Logger.DEBUG("clean sam");
        context.setStatus("clean sam");
        tools.runCleanSam(tmpOut1, tmpOut2);
        Logger.DEBUG("mark duplicates");
        context.setStatus("mark duplicates");
        tools.runMarkDuplicates(tmpOut2, tmpOut3, tmpMetrics);
        if(gff != null) {
            // tmpOut3 is sam for htseq count!        
            Logger.DEBUG("featureCounts");
            context.setStatus("featureCounts");
            tools.runFeatureCounts(gff, tmpOut3, fCounts, threads);
            HalvadeFileUtils.uploadFileToHDFS(context, FileSystem.get(new URI(outputdir), context.getConfiguration()),
                        fCounts, outputdir + context.getTaskAttemptID().toString() + ".count");
            HalvadeFileUtils.uploadFileToHDFS(context, null, ref, tmp);
        }  
        if(!inputIsBam) {
            Logger.DEBUG("add read-group");
            context.setStatus("add read-group");
            tools.runAddOrReplaceReadGroups(tmpOut3, output, RGID, RGLB, RGPL, RGPU, RGSM);
        }
        Logger.DEBUG("build bam index");
        context.setStatus("build bam index");
        tools.runBuildBamIndex(output);
        
        estimatedTime = System.currentTimeMillis() - startTime;
        Logger.DEBUG("estimated time: " + estimatedTime / 1000);
        
        // remove all temporary files now!
        HalvadeFileUtils.removeLocalFile(keep, tmpMetrics, context, HalvadeCounters.FOUT_GATK_TMP);
        HalvadeFileUtils.removeLocalFile(keep, tmpOut1, context, HalvadeCounters.FOUT_GATK_TMP);
        HalvadeFileUtils.removeLocalFile(keep, tmpOut2, context, HalvadeCounters.FOUT_GATK_TMP);
        HalvadeFileUtils.removeLocalFile(keep, tmpOut3, context, HalvadeCounters.FOUT_GATK_TMP);  
        HalvadeFileUtils.removeLocalFile(keep, fCounts);     
    }

    protected String makeRegionFile(Context context, ChromosomeRange r, PreprocessingTools tools, String region) throws URISyntaxException, IOException, InterruptedException {        
        // download exomebed 
        if(exomeBedFile != null) {
            String exomebed = tmpFileBase  + "exome.bed";
            if(exomeBedFile.endsWith(".gz"))
                exomebed += ".gz";
            HalvadeFileUtils.downloadFileFromHDFS(context, FileSystem.get(new URI(exomeBedFile), context.getConfiguration()),
                exomeBedFile, exomebed);
            if(exomebed.endsWith(".gz"))
                exomebed = HalvadeFileUtils.Unzip(exomebed);
            region = tools.filterExomeBed(exomebed, r);
            if(region == null) {
                Logger.DEBUG("empty region file, no vcf results!!");
                return null;
            }
            HalvadeFileUtils.removeLocalFile(keep, exomebed);
        } else 
            r.writeToPicardRegionFile(region);
        return region;
    }

    protected void indelRealignment(Context context, String region, GATKTools gatk, String input, String output) throws InterruptedException {
        String targets = tmpFileBase + ".intervals";
        
        Logger.DEBUG("run RealignerTargetCreator");
        context.setStatus("run RealignerTargetCreator");
        context.getCounter(HalvadeCounters.TOOLS_GATK).increment(1);
        gatk.runRealignerTargetCreator(input, targets, ref, region);
        
        Logger.DEBUG("run IndelRealigner");
        context.setStatus("run IndelRealigner");
        context.getCounter(HalvadeCounters.TOOLS_GATK).increment(1);
        gatk.runIndelRealigner(input, targets, output, ref, region);
        
        HalvadeFileUtils.removeLocalFile(keep, input, context, HalvadeCounters.FOUT_GATK_TMP);
        HalvadeFileUtils.removeLocalFile(keep, input.replaceAll(".bam", ".bai"));
        HalvadeFileUtils.removeLocalFile(keep, targets, context, HalvadeCounters.FOUT_GATK_TMP);
    }
    
    protected void baseQualityScoreRecalibration(Context context, String region, ChromosomeRange r, PreprocessingTools tools, GATKTools gatk, 
            String input, String output) throws InterruptedException, IOException, URISyntaxException {
        String table = tmpFileBase + ".table";
        
        // get snp database(s)
        String[] snpslocal = HalvadeFileUtils.downloadSites(context, taskId);
        String[] newKnownSites = new String[snpslocal.length];
        for(int i = 0 ; i < snpslocal.length; i++) {
            if(useBedTools) newKnownSites[i] = tools.filterDBSnps(ref.replaceAll("fasta", "dict"), snpslocal[i], r, tmpFileBase, threads); 
            else newKnownSites[i] = snpslocal[i]; 
            if(newKnownSites[i].endsWith(".gz"))
                newKnownSites[i] = HalvadeFileUtils.Unzip(newKnownSites[i]);
        }
        
        // should be created automatically by GATK v3.0 or higher
//        Logger.DEBUG("build bam index");
//        context.setStatus("build bam index");
//        tools.runBuildBamIndex(tmpFile1); 
        Logger.DEBUG("run baseRecalibrator");
        context.setStatus("run baseRecalibrator");
        context.getCounter(HalvadeCounters.TOOLS_GATK).increment(1);
        gatk.runBaseRecalibrator(input, table, ref, newKnownSites, region);
        
        Logger.DEBUG("run printReads");
        context.setStatus("run printReads");
        context.getCounter(HalvadeCounters.TOOLS_GATK).increment(1);
        gatk.runPrintReads(input, output, ref, table, region);
        
        HalvadeFileUtils.removeLocalFile(keep, input, context, HalvadeCounters.FOUT_GATK_TMP);
        HalvadeFileUtils.removeLocalFile(keep, input.replaceAll(".bam", ".bai"));
        HalvadeFileUtils.removeLocalFile(keep, table, context, HalvadeCounters.FOUT_GATK_TMP);
        for(int i = 0 ; i < newKnownSites.length; i++) {
            if(useBedTools) HalvadeFileUtils.removeLocalFile(keep, newKnownSites[i], context, HalvadeCounters.FOUT_GATK_TMP);
        }
    }

    protected void DnaVariantCalling(Context context, String region, GATKTools gatk, String input, String output) throws InterruptedException {
        // choose between unifiendgenotyper vs haplotypegenotyper
        Logger.DEBUG("run variantCaller");
        context.setStatus("run variantCaller");
        context.getCounter(HalvadeCounters.TOOLS_GATK).increment(1);
        if(useUnifiedGenotyper) 
            gatk.runUnifiedGenotyper(input, output, scc, sec, ref, null, region);
        else
            gatk.runHaplotypeCaller(input, output, false, scc, sec, ref, null, region);
                
        context.setStatus("cleanup");
        context.getCounter(HalvadeCounters.OUT_VCF_FILES).increment(1);
        
        HalvadeFileUtils.removeLocalFile(keep, input, context, HalvadeCounters.FOUT_GATK_TMP);
        HalvadeFileUtils.removeLocalFile(keep, input.replaceAll(".bam", ".bai"));
    }
    
    protected void RnaVariantCalling(Context context, String region, GATKTools gatk, String input, String output) throws InterruptedException {
        // choose between unifiendgenotyper vs haplotypegenotyper
        Logger.DEBUG("run variantCaller");
        context.setStatus("run variantCaller");
        context.getCounter(HalvadeCounters.TOOLS_GATK).increment(1);
        gatk.runHaplotypeCaller(input, output, true, scc, sec, ref, null, region);
                
        context.setStatus("cleanup");
        context.getCounter(HalvadeCounters.OUT_VCF_FILES).increment(1);
        
        HalvadeFileUtils.removeLocalFile(keep, input, context, HalvadeCounters.FOUT_GATK_TMP);
        HalvadeFileUtils.removeLocalFile(keep, input.replaceAll(".bam", ".bai"));
    }

    protected void splitNTrim(Context context, String region, GATKTools gatk, String input, String output) throws InterruptedException {        
        Logger.DEBUG("run SplitNCigarReads");
        context.setStatus("run SplitNCigarReads");
        context.getCounter(HalvadeCounters.TOOLS_GATK).increment(1);
        gatk.runSplitNCigarReads(input, output, ref, region, newMaxQualScore);
        
        HalvadeFileUtils.removeLocalFile(keep, input, context, HalvadeCounters.FOUT_GATK_TMP);
        HalvadeFileUtils.removeLocalFile(keep, input.replaceAll(".bam", ".bai"));
    }
    
    // TODO improve annotate/filter
    protected void filterVariants(Context context, String region, GATKTools gatk, String input, String output) throws InterruptedException {      
        Logger.DEBUG("run VariantFiltration");
        context.setStatus("run VariantFiltration");
        context.getCounter(HalvadeCounters.TOOLS_GATK).increment(1);
        gatk.runVariantFiltration(input, output, ref, region, windows, cluster, minFS, maxQD);
        
        HalvadeFileUtils.removeLocalFile(keep, input, context, HalvadeCounters.FOUT_GATK_TMP);
    }
    
    protected void annotateVariants(Context context, String region, GATKTools gatk, String input, String output) throws InterruptedException { 
        Logger.DEBUG("run VariantAnnotator");
        context.setStatus("run VariantAnnotator");
        context.getCounter(HalvadeCounters.TOOLS_GATK).increment(1);
        gatk.runVariantAnnotator(input, output, ref, region);
        
        HalvadeFileUtils.removeLocalFile(keep, input, context, HalvadeCounters.FOUT_GATK_TMP);
        
    }
}
