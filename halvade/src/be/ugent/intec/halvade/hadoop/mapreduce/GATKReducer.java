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
import fi.tkk.ics.hadoop.bam.SAMRecordWritable;
import be.ugent.intec.halvade.hadoop.datatypes.ChromosomeRegion;
import java.io.File;
import java.io.IOException;
import net.sf.samtools.*;
import org.apache.hadoop.fs.FileSystem;
import be.ugent.intec.halvade.tools.GATKTools;
import be.ugent.intec.halvade.tools.PreprocessingTools;
import be.ugent.intec.halvade.tools.ProcessException;
import be.ugent.intec.halvade.tools.QualityException;
import be.ugent.intec.halvade.utils.ChromosomeRange;
import be.ugent.intec.halvade.utils.HDFSFileIO;
import be.ugent.intec.halvade.utils.MyConf;
import be.ugent.intec.halvade.utils.Logger;
import java.net.URI;
import java.net.URISyntaxException;

/**
 *
 * @author ddecap
 */
public class GATKReducer extends HalvadeReducer {

    private boolean useBedTools;
    private boolean useUnifiedGenotyper;
    private double sec, scc;
    private String exomeBedFile;    
    
    @Override
    protected void reduce(ChromosomeRegion key, Iterable<SAMRecordWritable> values, Context context) throws IOException, InterruptedException {
        super.reduce(key, values, context);
        try {
            processAlignments(key, values, context);
        } catch (URISyntaxException ex) {
            Logger.EXCEPTION(ex);
            throw new InterruptedException();
        } catch (QualityException ex) {
            Logger.EXCEPTION(ex);
        } catch (ProcessException ex) {
        throw new InterruptedException(ex.toString());
        }
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        scc = MyConf.getSCC(context.getConfiguration());
        sec = MyConf.getSEC(context.getConfiguration());
        exomeBedFile = MyConf.getExomeBed(context.getConfiguration());
        useBedTools = MyConf.getUseBedTools(context.getConfiguration());
        useUnifiedGenotyper = MyConf.getUseUnifiedGenotyper(context.getConfiguration());
    }
    
    private void processAlignments(ChromosomeRegion key, Iterable<SAMRecordWritable> values, Context context) throws IOException, InterruptedException, URISyntaxException, QualityException {
        Logger.DEBUG("Processing key: " + key);
        long startTime = System.currentTimeMillis();
        boolean useIPrep = MyConf.getUseIPrep(context.getConfiguration());
        ChromosomeRange r = new ChromosomeRange();
        SAMRecordIterator SAMit = new SAMRecordIterator(values.iterator(), header, r);
        if(useIPrep)
            elPrepPreprocess(context, SAMit);
        else 
            PicardPreprocess(context, SAMit);
        
        runGATK(context, r);
        
        long estimatedTime = System.currentTimeMillis() - startTime;
        Logger.DEBUG("total estimated time: " + estimatedTime / 1000);
    }
        
    private void elPrepPreprocess(Context context, SAMRecordIterator SAMit) throws InterruptedException, IOException, QualityException {
        String dictF = ref.substring(0, ref.lastIndexOf(".fa")) + ".dict";
        PreprocessingTools tools = new PreprocessingTools(bin);
        tools.setContext(context);
        String rg = createReadGroupRecordString(RGID, RGLB, RGPL, RGPU, RGSM);
        String preSamOut = tmpFileBase + "_.sam";
        String samOut = tmpFileBase + ".sam";
        String bamOut = tmpFileBase + ".bam";
        
        outHeader = header.clone();
        outHeader.setSortOrder(SAMFileHeader.SortOrder.coordinate);
        
        Logger.DEBUG("call elPrep");
        context.setStatus("call elPrep");
        int reads;
        if(keepTmpFiles) 
            reads = tools.callElPrep(preSamOut, samOut, rg, dataThreads, SAMit, outHeader, dictF);
        else
            reads = tools.streamElPrep(context, samOut, rg, dataThreads, SAMit, outHeader, dictF);
        
        Logger.DEBUG(reads + " reads processed in elPrep");
        context.getCounter(HalvadeCounters.OUT_PREP_READS).increment(reads);
        context.setStatus("convert SAM to BAM");
        Logger.DEBUG("convert SAM to BAM");
        tools.callSAMToBAM(samOut, bamOut);
        context.setStatus("build bam index");
        Logger.DEBUG("build bam index");
        tools.runBuildBamIndex(bamOut);
        // remove temporary files
        removeLocalFile(preSamOut, context, HalvadeCounters.FOUT_GATK_TMP);
        removeLocalFile(samOut, context, HalvadeCounters.FOUT_GATK_TMP);
    }
    
    private void PicardPreprocess(Context context, SAMRecordIterator SAMit) throws InterruptedException, QualityException {
        outHeader = header.clone();
        outHeader.setSortOrder(SAMFileHeader.SortOrder.coordinate);
        // tmp files
        String tmpOut1 = tmpFileBase + "-1.bam";
        String tmpOut2 = tmpFileBase + "-2.bam";
        String tmpOut3 = tmpFileBase + "-3.bam";
        String tmpMetrics = tmpFileBase + "-metrics.txt";
        String bamOut = tmpFileBase + ".bam";
        SAMFileWriterFactory factory = new SAMFileWriterFactory();
        SAMFileWriter writer = factory.makeBAMWriter(outHeader, true, new File(tmpOut1));
        
        long startTime = System.currentTimeMillis();
        
        SAMRecord sam;
        while(SAMit.hasNext()) {
            sam = SAMit.next();
            writer.addAlignment(sam);
        }
        int reads = SAMit.getCount();
        writer.close();
        
        context.getCounter(HalvadeCounters.OUT_PREP_READS).increment(reads);
        long estimatedTime = System.currentTimeMillis() - startTime;
        context.getCounter(HalvadeCounters.TIME_HADOOP_SAMTOBAM).increment(estimatedTime);
        
        //preprocess steps of iprep
        PreprocessingTools tools = new PreprocessingTools(bin);
        if(java !=null) tools.setJava(java);
        tools.setContext(context);
        Logger.DEBUG("clean sam");
        context.setStatus("clean sam");
        tools.runCleanSam(tmpOut1, tmpOut2);
        Logger.DEBUG("mark duplicates");
        context.setStatus("mark duplicates");
        tools.runMarkDuplicates(tmpOut2, tmpOut3, tmpMetrics);
        Logger.DEBUG("add read-group");
        context.setStatus("add read-group");
        tools.runAddOrReplaceReadGroups(tmpOut3, bamOut, RGID, RGLB, RGPL, RGPU, RGSM);
        Logger.DEBUG("build bam index");
        context.setStatus("build bam index");
        tools.runBuildBamIndex(bamOut);
        
        estimatedTime = System.currentTimeMillis() - startTime;
        Logger.DEBUG("estimated time: " + estimatedTime / 1000);
        
        // remove all temporary files now!
        removeLocalFile(tmpMetrics, context, HalvadeCounters.FOUT_GATK_TMP);
        removeLocalFile(tmpOut1, context, HalvadeCounters.FOUT_GATK_TMP);
        removeLocalFile(tmpOut2, context, HalvadeCounters.FOUT_GATK_TMP);
        removeLocalFile(tmpOut3, context, HalvadeCounters.FOUT_GATK_TMP);       
    }
    
    private void runGATK(Context context, ChromosomeRange r) throws IOException, InterruptedException, URISyntaxException {
        // get some ref seq and known snps
        String[] snpslocal = HDFSFileIO.downloadSites(context, taskId);
        // wrappers to call external programs
        PreprocessingTools tools = new PreprocessingTools(bin);
        GATKTools gatk = new GATKTools(ref, bin);
        gatk.setContext(context);
        tools.setContext(context);
        gatk.setThreadsPerType(dataThreads, cpuThreads);
        if(java !=null) gatk.setJava(java);
        
        // temporary files
        String region = tmpFileBase + "-region.intervals";
        String preprocess = tmpFileBase + ".bam";
        String table = tmpFileBase + ".table";
        String tmpFile1 = tmpFileBase + "-2.bam";
        String tmpFile2 = tmpFileBase + "-3.bam";
        String targets = tmpFileBase + ".intervals";
        String snps = tmpFileBase + ".vcf";
        
        // download exomebed 
        if(exomeBedFile != null) {
            String exomebed = tmpFileBase  + "exome.bed";
            if(exomeBedFile.endsWith(".gz"))
                exomebed += ".gz";
            HDFSFileIO.downloadFileFromHDFS(context, FileSystem.get(new URI(exomeBedFile), context.getConfiguration()),
                exomeBedFile, exomebed);
            if(exomebed.endsWith(".gz"))
                exomebed = HDFSFileIO.Unzip(exomebed);
            region = tools.filterExomeBed(exomebed, r);
            if(region == null) {
                Logger.DEBUG("empty region file, no vcf results!!");
                return;
            }
        } else 
            r.writeToPicardRegionFile(region);
        
        String[] newKnownSites = new String[snpslocal.length];
        for(int i = 0 ; i < snpslocal.length; i++) {
            if(useBedTools) newKnownSites[i] = tools.filterDBSnps(snpslocal[i], r); 
            else newKnownSites[i] = snpslocal[i]; 
            if(newKnownSites[i].endsWith(".gz"))
                newKnownSites[i] = HDFSFileIO.Unzip(newKnownSites[i]);
        }
        
        // run GATK
        Logger.DEBUG("run RealignerTargetCreator");
        context.setStatus("run RealignerTargetCreator");
        context.getCounter(HalvadeCounters.TOOLS_GATK).increment(1);
        gatk.runRealignerTargetCreator(preprocess, targets, ref, region);
        Logger.DEBUG("run IndelRealigner");
        context.setStatus("run IndelRealigner");
        context.getCounter(HalvadeCounters.TOOLS_GATK).increment(1);
        gatk.runIndelRealigner(preprocess, targets, tmpFile1, ref, region);
        // should be created automatically by GATK if v3.x
//        Logger.DEBUG("build bam index");
//        context.setStatus("build bam index");
//        tools.runBuildBamIndex(tmpFile1); 
        Logger.DEBUG("run baseRecalibrator");
        context.setStatus("run baseRecalibrator");
//        try {
        context.getCounter(HalvadeCounters.TOOLS_GATK).increment(1);
        gatk.runBaseRecalibrator(tmpFile1, table, ref, newKnownSites, region);
//        } catch (InterruptedException ie) {
//            Logger.DEBUG("Caught BaseRecalibrator Exception");
//            Logger.DEBUG("\tCan happen when the recalibration table is empty.");
//            Logger.DEBUG("\tCaught in order to continue with other keys.");
//            return;
//        }
        Logger.DEBUG("run printReads");
        context.setStatus("run printReads");
        context.getCounter(HalvadeCounters.TOOLS_GATK).increment(1);
        gatk.runPrintReads(tmpFile1, tmpFile2, ref, table, region);
        // choose between unifiendgenotyper vs haplotypegenotyper
        Logger.DEBUG("run variantCaller");
        context.setStatus("run variantCaller");
        context.getCounter(HalvadeCounters.TOOLS_GATK).increment(1);
        gatk.runVariantCaller(tmpFile2, snps, useUnifiedGenotyper, scc, sec, 
                              ref, null, region);
                
        context.setStatus("cleanup");
        context.getCounter(HalvadeCounters.OUT_VCF_FILES).increment(1);
        // remove all temporary files now!
        removeLocalFile(preprocess, context, HalvadeCounters.FOUT_GATK_TMP);
        removeLocalFile(table, context, HalvadeCounters.FOUT_GATK_TMP);
        removeLocalFile(tmpFile1, context, HalvadeCounters.FOUT_GATK_TMP);
        removeLocalFile(tmpFile2, context, HalvadeCounters.FOUT_GATK_TMP);
        removeLocalFile(targets, context, HalvadeCounters.FOUT_GATK_TMP);
        removeLocalFile(region);
        removeLocalFile(tmpFileBase + ".bai");
        removeLocalFile(tmpFileBase + "-2.bai");
        removeLocalFile(tmpFileBase + "-3.bai");
        variantFiles.add(snps);
        for(int i = 0 ; i < newKnownSites.length; i++) {
            if(useBedTools) removeLocalFile(newKnownSites[i], context, HalvadeCounters.FOUT_GATK_TMP);
        }
    }
}
