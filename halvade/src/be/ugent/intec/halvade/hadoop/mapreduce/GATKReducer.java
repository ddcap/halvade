/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package be.ugent.intec.halvade.hadoop.mapreduce;

import fi.tkk.ics.hadoop.bam.SAMRecordWritable;
import fi.tkk.ics.hadoop.bam.VariantContextWritable;
import be.ugent.intec.halvade.hadoop.datatypes.ChromosomeRegion;
import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import net.sf.samtools.*;
import net.sf.samtools.util.Iso8601Date;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.Reducer;
import be.ugent.intec.halvade.tools.GATKTools;
import be.ugent.intec.halvade.tools.PreprocessingTools;
import be.ugent.intec.halvade.tools.ProcessException;
import be.ugent.intec.halvade.utils.ChromosomeRange;
import be.ugent.intec.halvade.utils.HDFSFileIO;
import be.ugent.intec.halvade.utils.MyConf;
import be.ugent.intec.halvade.utils.Logger;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

/**
 *
 * @author ddecap
 */
public class GATKReducer extends Reducer<ChromosomeRegion, SAMRecordWritable, ChromosomeRegion, VariantContextWritable> {

    private int count;
    private ArrayList<String> variantFiles;
    private String tmp;
    private String ref;
    private String java;
    private String tmpFileBase;
    private String taskId;
    private String bin;
    private SAMFileHeader header;
    private boolean useBedTools;
    private boolean useUnifiedGenotyper;
    private SAMSequenceDictionary dict;
    double sec, scc;
    String RGID = "GROUP1";    
    String RGLB = "LIB1";
    String RGPL = "ILLUMINA";
    String RGPU = "UNIT1";
    String RGSM = "SAMPLE1";
    int alignmentStart = -1;
    int alignmentEnd = -1;
    int dataThreads, cpuThreads;
    String referenceName;
    SAMFileHeader outHeader;
    private boolean keep = false;
    private String exomeBedFile;
    
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
        Logger.DEBUG("count: " + count);
        String output = null;
        String outputdir = MyConf.getOutDir(context.getConfiguration());   
        if(variantFiles.size() > 1) {
            GATKTools gatk = new GATKTools(ref, bin);
            gatk.setThreadsPerType(dataThreads, cpuThreads);
            gatk.setContext(context);
            if(java !=null) gatk.setJava(java);
            output = tmp + context.getTaskAttemptID().toString() + ".vcf";
            Logger.DEBUG("run CombineVariants");
            gatk.runCombineVariants(variantFiles.toArray(new String[variantFiles.size()]), 
                    output, ref);
            context.getCounter(HalvadeCounters.TOOLS_GATK).increment(1);
        } else if (variantFiles.size() == 1) {
            output = variantFiles.get(0);
        }
        if(output != null) {        
            try {
                HDFSFileIO.uploadFileToHDFS(context, FileSystem.get(new URI(outputdir), context.getConfiguration()),
                        output, outputdir + context.getTaskAttemptID().toString() + ".vcf");
                HDFSFileIO.uploadFileToHDFS(context, FileSystem.get(new URI(outputdir), context.getConfiguration()), 
                        output + ".idx", outputdir + context.getTaskAttemptID().toString() + ".vcf.idx");
            } catch (URISyntaxException ex) {
                Logger.EXCEPTION(ex);
                throw new InterruptedException();
            }
        }
        
        // delete the files from local scratch
        if(variantFiles.size() > 1){
            for(String snps : variantFiles){
                removeLocalFile(snps, context, HalvadeCounters.FOUT_GATK_VCF);
                removeLocalFile(snps + ".idx");
            }
        }
        if(output != null) {
            removeLocalFile(output, context, HalvadeCounters.FOUT_GATK_VCF);
                removeLocalFile(output + ".idx");
        }
    }

    @Override
    protected void reduce(ChromosomeRegion key, Iterable<SAMRecordWritable> values, Context context) throws IOException, InterruptedException {
        Logger.DEBUG("key: " + key);
        try {
            tmpFileBase = context.getTaskAttemptID().toString() + key;
            try {
                processAlignments(key, values, context);
            } catch (URISyntaxException ex) {
                Logger.EXCEPTION(ex);
                throw new InterruptedException();
            }
        } catch (ProcessException ex) {
            throw new InterruptedException(ex.toString());
        }
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        keep = MyConf.getKeepFiles(context.getConfiguration());
        java = MyConf.getJava(context.getConfiguration());
        tmp = MyConf.getScratchTempDir(context.getConfiguration());
        ref = MyConf.findRefOnScratch(context.getConfiguration());
        dataThreads = MyConf.getGATKNumDataThreads(context.getConfiguration());
        cpuThreads = MyConf.getGATKNumCPUThreads(context.getConfiguration());
        dict = MyConf.getSequenceDictionary(context.getConfiguration());
        scc = MyConf.getSCC(context.getConfiguration());
        sec = MyConf.getSEC(context.getConfiguration());
        getReadGroupData(context.getConfiguration());
        taskId = context.getTaskAttemptID().toString();
        taskId = taskId.substring(taskId.indexOf("r_"));
        SAMSequenceDictionary dict = MyConf.getSequenceDictionary(context.getConfiguration());
        header = new SAMFileHeader();
        header.setSequenceDictionary(dict);
        count = 0;
        exomeBedFile = MyConf.getExomeBed(context.getConfiguration());
        useBedTools = MyConf.getUseBedTools(context.getConfiguration());
        useUnifiedGenotyper = MyConf.getUseUnifiedGenotyper(context.getConfiguration());
        variantFiles = new ArrayList<String>();
        bin  = checkBinaries(context);
    }
    
    private String getChromosomeName(ChromosomeRegion key, ChromosomeRange r) {
        String chr = dict.getSequence(key.getChromosome()).getSequenceName();
        return chr + " [" + r.getAlignmentStart() + ", " + r.getAlignmentEnd() + "]"; 
    }
    
    private void processAlignments(ChromosomeRegion key, Iterable<SAMRecordWritable> values, Context context) throws IOException, InterruptedException, URISyntaxException {
        Logger.DEBUG("Processing key: " + key.toString());
        long startTime = System.currentTimeMillis();
        boolean useIPrep = MyConf.getUseIPrep(context.getConfiguration());
        ChromosomeRange r;
        String chr = dict.getSequence(key.getChromosome()).getSequenceName();
        if(useIPrep) 
            r = elPrepPreprocess(context, chr, values.iterator());
        else 
            r = PicardPreprocess(context, chr, values.iterator());
        
        runGATK(context, r);
        
        
        long estimatedTime = System.currentTimeMillis() - startTime;
        Logger.DEBUG(getChromosomeName(key, r) + " estimated time: " + estimatedTime / 1000);
    }
    
    private ChromosomeRange elPrepPreprocess(Context context, String chr, Iterator<SAMRecordWritable> it) throws InterruptedException, IOException {
    ChromosomeRange r = new ChromosomeRange(chr);
        String dictF = ref.substring(0, ref.lastIndexOf(".fa")) + ".dict";
        PreprocessingTools tools = new PreprocessingTools(bin);
        tools.setContext(context);
        String rg = createReadGroupRecordString(RGID, RGLB, RGPL, RGPU, RGSM);
        String preSamOut = tmp + tmpFileBase + "_.sam";
        String samOut = tmp + tmpFileBase + ".sam";
        String bamOut = tmp + tmpFileBase + ".bam";
        
        outHeader = header.clone();
        outHeader.setSortOrder(SAMFileHeader.SortOrder.coordinate);
        
        Logger.DEBUG("call elPrep");
        context.setStatus("call elPrep");
        int reads;
//        reads = tools.callElPrep(preSamOut, samOut, rg, dataThreads, it, outHeader, dictF, r);
        reads = tools.streamElPrep(context, samOut, rg, dataThreads, it, outHeader, dictF, r);
        Logger.DEBUG(reads + " reads processed in iPrep");
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
        
        return r;
    }
    
    private ChromosomeRange PicardPreprocess(Context context, String chr, Iterator<SAMRecordWritable> it) throws InterruptedException {
        ChromosomeRange r = new ChromosomeRange(chr);
        outHeader = header.clone();
        outHeader.setSortOrder(SAMFileHeader.SortOrder.coordinate);
        SAMReadGroupRecord rg = createReadGroupRecord(RGID, RGLB, RGPL, RGPU, RGSM);
        outHeader.setReadGroups(Arrays.asList(new SAMReadGroupRecord[] {rg}));
        // tmp files
        String tmpOut1 = tmp + tmpFileBase + "-1.bam";
        String tmpOut2 = tmp + tmpFileBase + "-2.bam";
        String tmpOut3 = tmp + tmpFileBase + "-3.bam";
        String tmpMetrics = tmp + tmpFileBase + "-metrics.txt";
        String bamOut = tmp + tmpFileBase + ".bam";
        SAMFileWriterFactory factory = new SAMFileWriterFactory();
        SAMFileWriter writer = factory.makeBAMWriter(outHeader, true, new File(tmpOut1));
        
        long startTime = System.currentTimeMillis();
        
        SAMRecord sam = null;
        int reads = 0;
        int currentStart = -1, currentEnd = -1;
        if(it.hasNext()) {
            sam = it.next().get();
            sam.setHeader(header);
//            Logger.DEBUG(sam.getReferenceName() + "[" + sam.getAlignmentStart() + "-" + sam.getAlignmentEnd() + "] -- " +
//                    sam.getMateReferenceName() + "[" + sam.getMateAlignmentStart() + "]");
            writer.addAlignment(sam);
            reads++;
            currentStart = sam.getAlignmentStart();
            currentEnd = sam.getAlignmentEnd();
        }
        while(it.hasNext()) {
            sam = it.next().get();
            sam.setHeader(header);
//            Logger.DEBUG(sam.getReferenceName() + "[" + sam.getAlignmentStart() + "-" + sam.getAlignmentEnd() + "] -- " +
//                    sam.getMateReferenceName() + "[" + sam.getMateAlignmentStart() + "]");
             writer.addAlignment(sam);
            reads++;            
            if(sam.getAlignmentStart() <= currentEnd){
                if (sam.getAlignmentEnd() > currentEnd) {
                    currentEnd = sam.getAlignmentEnd();
                }
            } else {
                // new region to start here, add current!
                r.addRange(currentStart, currentEnd);
                currentStart = sam.getAlignmentStart();
                currentEnd = sam.getAlignmentEnd();
            }
        }
        if(sam != null){
            r.addRange(currentStart, currentEnd);
        }
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
        
        return r;
    }
    
    private void runGATK(Context context, ChromosomeRange r) throws IOException, InterruptedException, URISyntaxException {
        // get some ref seq and known snps
        String[] snpslocal = HDFSFileIO.downloadSites(context, taskId);
        String base = tmp + tmpFileBase;  
        // wrappers to call external programs
        PreprocessingTools tools = new PreprocessingTools(bin);
        GATKTools gatk = new GATKTools(ref, bin);
        gatk.setContext(context);
        tools.setContext(context);
        gatk.setThreadsPerType(dataThreads, cpuThreads);
        if(java !=null) gatk.setJava(java);
        
        // temporary files
        String region = base + "-region.intervals";
        String preprocess = base + ".bam";
        String table = base + ".table";
        String tmpFile1 = base + "-2.bam";
        String tmpFile2 = base + "-3.bam";
        String targets = base + ".intervals";
        String snps = base + ".vcf";
        
        // download exomebed
        if(exomeBedFile != null) {
            String exomebed = base  + "exome.bed";
            if(exomeBedFile.endsWith(".gz"))
                exomebed += ".gz";
            HDFSFileIO.downloadFileFromHDFS(context, FileSystem.get(new URI(exomeBedFile), context.getConfiguration()),
                exomeBedFile, exomebed);
            if(exomebed.endsWith(".gz"))
                exomebed = HDFSFileIO.Unzip(exomebed);
            region = tools.filterExomeBed(exomebed, r);
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
        Logger.DEBUG("build bam index");
        context.setStatus("build bam index");
        tools.runBuildBamIndex(tmpFile1); 
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
                
        // TODO get vcf records ( from gatk file/memory?) and give as output!
        // for now just copy file to hdfs
        context.setStatus("cleanup");
        context.getCounter(HalvadeCounters.OUT_VCF_FILES).increment(1);
        // remove all temporary files now!
        removeLocalFile(preprocess, context, HalvadeCounters.FOUT_GATK_TMP);
        removeLocalFile(table, context, HalvadeCounters.FOUT_GATK_TMP);
        removeLocalFile(tmpFile1, context, HalvadeCounters.FOUT_GATK_TMP);
        removeLocalFile(tmpFile2, context, HalvadeCounters.FOUT_GATK_TMP);
        removeLocalFile(targets, context, HalvadeCounters.FOUT_GATK_TMP);
        removeLocalFile(region);
        removeLocalFile(base + ".bai");
        removeLocalFile(base + "-2.bai");
        removeLocalFile(base + "-3.bai");
        variantFiles.add(snps);
        for(int i = 0 ; i < newKnownSites.length; i++) {
            if(useBedTools) removeLocalFile(newKnownSites[i], context, HalvadeCounters.FOUT_GATK_TMP);
        }
    }
    
    protected boolean removeLocalFile(String filename) {
        if(keep) return false;
        File f = new File(filename);
        return f.exists() && f.delete();
    }
    
    protected boolean removeLocalFile(String filename, Context context, HalvadeCounters counter) {
        if(keep) return false;
        File f = new File(filename);
        if(f.exists()) context.getCounter(counter).increment(f.length());
        return f.exists() && f.delete();
    }
    
    private void getReadGroupData(Configuration conf) {
//            String RGID,String RGLB, String RGPL, 
//            String RGPU, String RGSM
        // format: "ID:g LB:lib1 PL:illumina PU:unit1 SM:sample1"
        String readGroup = MyConf.getReadGroup(conf);
        String[] elements = readGroup.split(" ");
        for(String ele : elements) {
            String[] val = ele.split(":");
            if(val[0].equalsIgnoreCase("id"))
                RGID = val[1];
            else if(val[0].equalsIgnoreCase("lb"))
                RGLB = val[1];
            else if(val[0].equalsIgnoreCase("pl"))
                RGPL = val[1];
            else if(val[0].equalsIgnoreCase("pu"))
                RGPU = val[1];
            else if(val[0].equalsIgnoreCase("sm"))
                RGSM = val[1];
        }
    }
    
    private String createReadGroupRecordString(
            String RGID, String RGLB, String RGPL, 
            String RGPU, String RGSM){
        return "ID:" + RGID + " LB:" + RGLB + " PL:" + RGPL + " PU:" + RGPU + " SM:" + RGSM;
    }
           
    private SAMReadGroupRecord createReadGroupRecord(
            String RGID, String RGLB, String RGPL, 
            String RGPU, String RGSM) {
        return createReadGroupRecord(RGID, RGLB, RGPL, RGPU, RGSM, null, null, null, null);
    }
    
    private SAMReadGroupRecord createReadGroupRecord(
            String RGID, String RGLB, String RGPL, 
            String RGPU, String RGSM, String RGCN, 
            String RGDS, Iso8601Date RGDT, Integer RGPI) {
        SAMReadGroupRecord rg = new SAMReadGroupRecord(RGID);
        rg.setLibrary(RGLB);
        rg.setPlatform(RGPL);
        rg.setSample(RGSM);
        rg.setPlatformUnit(RGPU);
        if(RGCN != null)
            rg.setSequencingCenter(RGCN);
        if(RGDS != null)
            rg.setDescription(RGDS);
        if(RGDT != null)
            rg.setRunDate(RGDT);
        if(RGPI != null)
            rg.setPredictedMedianInsertSize(RGPI);
        return rg;
    }
    
    protected String checkBinaries(Context context) throws IOException {
        Logger.DEBUG("Checking for binaries...");
        String binDir = MyConf.getBinDir(context.getConfiguration());
        if(binDir != null) {
            return binDir;
        }
        Path[] localPath = context.getLocalCacheArchives();
        for(int i = 0; i < localPath.length; i++ ) {
            if(localPath[i].getName().equals("bin.tar.gz")) {
                binDir = localPath[i] + "/bin/";
            }
        }
        printDirectoryTree(new File(binDir), 0);
        return binDir;
    }
    
    protected void printDirectoryTree(File dir, int level) {
        String whitespace = "";
        for(int i = 0; i < level; i++)
            whitespace += "\t";
        File[] list = dir.listFiles();
        if(list != null) {
            for(int i = 0; i < list.length; i++ ) {
                java.nio.file.Path path = FileSystems.getDefault().getPath(list[i].getAbsolutePath());
                String attr = "";
                if(list[i].isDirectory()) 
                    attr += "D ";
                else 
                    attr += "F ";
                if(list[i].canExecute()) 
                    attr += "E ";
                else 
                    attr += "NE ";
                if(list[i].canRead()) 
                    attr += "R ";
                else 
                    attr += "NR ";
                if(list[i].canWrite()) 
                    attr += "W ";
                else 
                    attr += "NW ";
                if(Files.isSymbolicLink(path)) 
                    attr += "S ";
                else 
                    attr += "NS ";
                    
                Logger.DEBUG(whitespace + attr + "\t" + list[i].getName());
                if(list[i].isDirectory())
                    printDirectoryTree(list[i], level + 1);
            }
        } else {
                    Logger.DEBUG(whitespace + "N");
        }
    }
}
