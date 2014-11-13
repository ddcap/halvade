/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package be.ugent.intec.halvade.hadoop.mapreduce;

import be.ugent.intec.halvade.hadoop.datatypes.ChromosomeRegion;
import be.ugent.intec.halvade.tools.GATKTools;
import be.ugent.intec.halvade.utils.HDFSFileIO;
import be.ugent.intec.halvade.utils.Logger;
import be.ugent.intec.halvade.utils.MyConf;
import fi.tkk.ics.hadoop.bam.SAMRecordWritable;
import fi.tkk.ics.hadoop.bam.VariantContextWritable;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.util.ArrayList;
import net.sf.samtools.SAMFileHeader;
import net.sf.samtools.SAMReadGroupRecord;
import net.sf.samtools.SAMSequenceDictionary;
import net.sf.samtools.util.Iso8601Date;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author ddecap
 */
public class HalvadeReducer extends Reducer<ChromosomeRegion, SAMRecordWritable, ChromosomeRegion, VariantContextWritable> {

    protected int count;
    protected ArrayList<String> variantFiles;
    protected String tmp;
    protected String ref;
    protected String java;
    protected String tmpFileBase;
    protected String taskId;
    protected String bin;
    protected SAMFileHeader header;
    protected SAMSequenceDictionary dict;
    protected String RGID = "GROUP1";    
    protected String RGLB = "LIB1";
    protected String RGPL = "ILLUMINA";
    protected String RGPU = "UNIT1";
    protected String RGSM = "SAMPLE1";
    protected int dataThreads, cpuThreads;
    protected String referenceName;
    protected SAMFileHeader outHeader;
    protected boolean keepTmpFiles = false;
    
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
        Logger.DEBUG("count: " + count);
        String output = null;
        String outputdir = MyConf.getOutDir(context.getConfiguration());   
        if(variantFiles.size() > 1) { // should not happen -> multiple keys per reducer
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
        tmpFileBase = tmp + taskId + key;
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        keepTmpFiles = MyConf.getKeepFiles(context.getConfiguration());
        java = MyConf.getJava(context.getConfiguration());
        tmp = MyConf.getScratchTempDir(context.getConfiguration());
        ref = MyConf.findRefOnScratch(context.getConfiguration());
        dataThreads = MyConf.getGATKNumDataThreads(context.getConfiguration());
        cpuThreads = MyConf.getGATKNumCPUThreads(context.getConfiguration());
        dict = MyConf.getSequenceDictionary(context.getConfiguration());
        getReadGroupData(context.getConfiguration());
        taskId = context.getTaskAttemptID().toString();
        taskId = taskId.substring(taskId.indexOf("r_"));
        header = new SAMFileHeader();
        header.setSequenceDictionary(dict);
        count = 0;
        variantFiles = new ArrayList<>();
        bin  = checkBinaries(context);
        
    }
    
    protected boolean removeLocalFile(String filename) {
        if(keepTmpFiles) return false;
        File f = new File(filename);
        return f.exists() && f.delete();
    }
    
    protected boolean removeLocalFile(String filename, Reducer.Context context, HalvadeCounters counter) {
        if(keepTmpFiles) return false;
        File f = new File(filename);
        if(f.exists()) context.getCounter(counter).increment(f.length());
        return f.exists() && f.delete();
    }
    
    protected void getReadGroupData(Configuration conf) {
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
    
    protected String createReadGroupRecordString(
            String RGID, String RGLB, String RGPL, 
            String RGPU, String RGSM){
        return "ID:" + RGID + " LB:" + RGLB + " PL:" + RGPL + " PU:" + RGPU + " SM:" + RGSM;
    }
           
    protected SAMReadGroupRecord createReadGroupRecord(
            String RGID, String RGLB, String RGPL, 
            String RGPU, String RGSM) {
        return createReadGroupRecord(RGID, RGLB, RGPL, RGPU, RGSM, null, null, null, null);
    }
    
    protected SAMReadGroupRecord createReadGroupRecord(
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
    
    protected String checkBinaries(Reducer.Context context) throws IOException {
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