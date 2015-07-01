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

package be.ugent.intec.halvade.utils;

import be.ugent.intec.halvade.hadoop.mapreduce.HalvadeCounters;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.util.zip.GZIPInputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;

/**
 *
 * @author ddecap
 */
public class HalvadeFileUtils {
    protected static final int RETRIES = 3;

    public static String Unzip(String inFilePath) throws IOException  {
        GZIPInputStream gzipInputStream = new GZIPInputStream(new FileInputStream(inFilePath));

        String outFilePath = inFilePath.replace(".gz", "");
        OutputStream out = new FileOutputStream(outFilePath);

        byte[] buf = new byte[64*1024];
        int len;
        while ((len = gzipInputStream.read(buf)) > 0)
            out.write(buf, 0, len);

        gzipInputStream.close();
        out.close();

        new File(inFilePath).delete();

        return outFilePath;
    }
    
    protected static boolean checkCorrectSize(String onHDFS, String onScratch, FileSystem fs) throws IOException {
        File f = new File(onScratch);
        return fs.getFileStatus(new Path(onHDFS)).getLen() == f.length();
    }
    
    /**
     * @return returns 0 if successfull, -1 if filesize is incorrect and -2 if an exception occurred
     */
    protected static int privateDownloadFileFromHDFS(TaskInputOutputContext context, FileSystem fs, String from, String to) {
        try {
            // check if file is present on local scratch
            File f = new File(to);
            if(!f.exists()) {
                Logger.DEBUG("attempting download of \"" + to + "\"");
                fs.copyToLocalFile(new Path(from), new Path(to));
                context.getCounter(HalvadeCounters.FIN_FROM_HDFS).increment(fs.getFileStatus(new Path(from)).getLen());
            } else {
                // check if filesize is correct
                if(fs.getFileStatus(new Path(from)).getLen() != f.length()) {
                    // incorrect filesize, remove and download again
                    Logger.DEBUG("incorrect filesize: " + f.length() + " =/= " + 
                            fs.getFileStatus(new Path(from)).getLen());
                    f.delete();
                    fs.copyToLocalFile(new Path(from), new Path(to));
                    context.getCounter(HalvadeCounters.FIN_FROM_HDFS).increment(fs.getFileStatus(new Path(from)).getLen());
            
                } else {
                    Logger.DEBUG("file \"" + to + "\" exists");
                }
            }
            if(fs.getFileStatus(new Path(from)).getLen() != f.length())
                return -1;
            else
                return 0;
        } catch (IOException ex) {
            Logger.DEBUG("failed to download " + from + " from HDFS: " + ex.getLocalizedMessage());
            Logger.EXCEPTION(ex);
            return -2;
        }
    }
    
    protected static int attemptDownloadFileFromHDFS(TaskInputOutputContext context, FileSystem fs, String from, String to, int tries) throws IOException {
        int val = privateDownloadFileFromHDFS(context, fs, from, to);
        int try_ = 1;
        while (val != 0 && try_ < tries) {
            val = privateDownloadFileFromHDFS(context, fs, from, to);
            try_++;
        }
        if(val == 0)
            Logger.DEBUG(from + " downloaded");
        else {
            Logger.DEBUG(from + " failed to download");   
            throw new IOException();
        }
        return val;            
    }
    
    public static int downloadFileFromHDFS(TaskInputOutputContext context, FileSystem fs, String from, String to) throws IOException {         
        return attemptDownloadFileFromHDFS(context, fs, from, to, RETRIES);            
    }
    
    /**
     * @return returns 0 if successfull, -1 if filesize is incorrect and -2 if an exception occurred
     */
    protected static int privateUploadFileToHDFS(TaskInputOutputContext context, FileSystem fs, String from, String to) {
        try {
            // check if file is present on HDFS
            Path toPath = new Path(to);
            Path fromPath = new Path(from);
            File f = new File(from);
            if(!fs.exists(toPath)) {
                fs.copyFromLocalFile(fromPath, toPath);
                context.getCounter(HalvadeCounters.FOUT_TO_HDFS).increment(f.length());
            } else {
                // check if filesize is correct
                if(fs.getFileStatus(toPath).getLen() != f.length()) {
                    // incorrect filesize, remove and download again
                    fs.delete(toPath, false);
                    fs.copyFromLocalFile(fromPath, toPath);
                context.getCounter(HalvadeCounters.FOUT_TO_HDFS).increment(f.length());
                }
            }
            if(fs.getFileStatus(toPath).getLen() != f.length())
                return -1;
            else
                return 0;
        } catch (IOException ex) {
            Logger.DEBUG("failed to upload " + from + " to HDFS: " + ex.getLocalizedMessage());
            Logger.EXCEPTION(ex);
            return -2;
        }
    }
    
    protected static int attemptUploadFileToHDFS(TaskInputOutputContext context, FileSystem fs, String from, String to, int tries) throws IOException {
        int val = privateUploadFileToHDFS(context, fs, from, to);
        int try_ = 1;
        while (val != 0 && try_ < tries) {
            val = privateUploadFileToHDFS(context, fs, from, to);
            try_++;
        }
        if(val == 0)
            Logger.DEBUG(from + " uploaded");
        else {
            Logger.DEBUG(from + " failed to upload");
            throw new IOException();
        }
        return val;
    }
    
    public static int uploadFileToHDFS(TaskInputOutputContext context, FileSystem fs, String from, String to) throws IOException {         
        return attemptUploadFileToHDFS(context, fs, from, to, RETRIES);            
    }   

    
    protected String downloadGFF(TaskInputOutputContext context, String id) {
        Configuration conf = context.getConfiguration();
        String refDir = HalvadeConf.getRefDirOnScratch(conf);
        if(!refDir.endsWith("/")) refDir = refDir + "/";
        HalvadeFileLock lock = new HalvadeFileLock(context, refDir, GFF_LOCK);
        String gffFile = null;
        String gffSuffix = null;
        try {
            lock.getLock();

            ByteBuffer bytes = ByteBuffer.allocate(4);
            if (lock.read(bytes) > 0) {
                bytes.flip();
                long val = bytes.getInt();
                if(val == DEFAULT_LOCK_VAL)
                    Logger.DEBUG("gff has been downloaded to local scratch: " + val);
                else {
                    Logger.INFO("downloading missing gff file to local scratch");
                    String gff = HalvadeConf.getGff(context.getConfiguration());
                    FileSystem fs = FileSystem.get(new URI(gff), conf);
                    int si = gff.lastIndexOf('.');
                    if (si > 0)
                        suffix = gff.substring(si);
                    else 
                        Throw new InterruptedException("Illegal filename for gff file: " + gff);
                    gffFile = findFile(refDir, suffix, false);
                    if (gffFile == null)
                        gffFile = refDir + id; 

                    attemptDownloadFileFromHDFS(context, fs, gff, gffFile + suffix, RETRIES);
                    Logger.INFO("FINISHED downloading the complete reference index to local scratch");
                    bytes.clear();
                    bytes.putInt(DEFAULT_LOCK_VAL).flip();
                    lock.forceWrite(bytes);
                }
            } else {
                Logger.INFO("downloading missing gff file to local scratch");
                String gff = HalvadeConf.getGff(context.getConfiguration());
                FileSystem fs = FileSystem.get(new URI(gff), conf);
                int si = gff.lastIndexOf('.');
                if (si > 0)
                    suffix = gff.substring(si);
                else 
                    Throw new InterruptedException("Illegal filename for gff file: " + gff);
                gffFile = findFile(refDir, suffix, false);
                if (gffFile == null)
                    gffFile = refDir + id; 

                attemptDownloadFileFromHDFS(context, fs, gff, gffFile + suffix, RETRIES);
                Logger.INFO("FINISHED downloading the complete reference index to local scratch");
                bytes.clear();
                bytes.putInt(DEFAULT_LOCK_VAL).flip();
                lock.forceWrite(bytes);
            }
            
        } catch (InterruptedException ex) {
            Logger.EXCEPTION(ex);
        } finally {
            lock.releaseLock();
        }
        if(gffFile == null)
            gffFile = findFile(refDir, ".gff", false);
        return gffFile + ".gff";

    }


    // functions download BWA/STAR/GATK references/dbSNP database
    
    protected static String REF_LOCK = "down_ref.lock";
    protected static String GFF_LOCK = "down_gff.lock";
    protected static String STARG_LOCK = "down_starg.lock";
    protected static String DBSNP_LOCK = "down_snpdb.lock";
    
    
    protected static int REF_BOTH = 2;
    protected static int DEFAULT_LOCK_VAL = 1;
    protected static String HALVADE_BWA_SUFFIX = ".bwa_ref";
    protected static String HALVADE_BOWTIE2_SUFFIX = ".bowtie2_ref";
    protected static String HALVADE_CUSHAW2_SUFFIX = ".cushaw2_ref";
    protected static String HALVADE_GATK_SUFFIX = ".gatk_ref";
    protected static String HALVADE_STAR_SUFFIX_P1 = ".star_ref";
    public static String HALVADE_STAR_SUFFIX_P2 = ".star_ref_p2";
    protected static String HALVADE_DBSNP_SUFFIX = ".dbsnp";
    
    protected static String[] BWA_REF_FILES = 
        {".fasta", ".fasta.amb", ".fasta.ann", ".fasta.bwt", ".fasta.pac", ".fasta.sa", ".fasta.fai", ".dict" }; 
    protected static String[] BOWTIE2_REF_FILES = 
        {".fasta", ".fasta.1.bt2",".fasta.2.bt2",".fasta.3.bt2",".fasta.4.bt2",".fasta.rev.1.bt2",".fasta.rev.2.bt2", ".fasta.fai", ".dict" }; 
    protected static String[] CUSHAW2_REF_FILES = 
        {".fasta", ".fasta.amb", ".fasta.ann", ".fasta.pac", ".fasta.rbwt", ".fasta.rpac", ".fasta.rsa", ".fasta.fai", ".dict" }; 
    protected static String[] GATK_REF_FILES =  {".fasta", ".fasta.fai", ".dict" }; 
    protected static String[] STAR_REF_FILES = 
        {"chrLength.txt", "chrNameLength.txt", "chrName.txt", "chrStart.txt", 
         "Genome", "genomeParameters.txt", "SA", "SAindex"};
    protected static String[] STAR_REF_OPTIONAL_FILES =  {"sjdbInfo.txt", "sjdbList.out.tab"};
    
    protected static String findFile(String directory, String suffix, boolean recursive) {
        File dir  = new File(directory);
        if(dir.isDirectory() && dir.listFiles() != null) {
            String foundPrefix = null;
            int i = 0;
            File[] files = dir.listFiles();
            while (foundPrefix == null && i < files.length) {
                File file = files[i];
                if(file.isDirectory() && recursive) {
                    foundPrefix = findFile(file.getAbsolutePath(), suffix, recursive);
                } else if (file.getAbsolutePath().endsWith(suffix)) {
                    foundPrefix = file.getAbsolutePath().substring(0, file.getAbsolutePath().lastIndexOf('.'));
                    Logger.DEBUG("found existing ref: \"" + foundPrefix + "\"");
                }
                i++;
            }
            return foundPrefix;
        } else 
            return null;
    }
        
    public static String downloadBWAIndex(TaskInputOutputContext context, String id) throws IOException, URISyntaxException {
        return downloadAlignerIndex(context, id, "bwa_ref-", HALVADE_BWA_SUFFIX, BWA_REF_FILES);
    }   
    public static String downloadBowtie2Index(TaskInputOutputContext context, String id) throws IOException, URISyntaxException {
        return downloadAlignerIndex(context, id, "bowtie2_ref-", HALVADE_BOWTIE2_SUFFIX, BOWTIE2_REF_FILES);
    }   
    public static String downloadCushaw2Index(TaskInputOutputContext context, String id) throws IOException, URISyntaxException {
        return downloadAlignerIndex(context, id, "cushaw2_ref-", HALVADE_CUSHAW2_SUFFIX, CUSHAW2_REF_FILES);
    }
    
    protected static String downloadAlignerIndex(TaskInputOutputContext context, String id, String refName, String refSuffix, String[] refFiles) throws IOException, URISyntaxException {
        Configuration conf = context.getConfiguration();
        String refDir = HalvadeConf.getRefDirOnScratch(conf);
        if(!refDir.endsWith("/")) refDir = refDir + "/";
        HalvadeFileLock lock = new HalvadeFileLock(context, refDir, REF_LOCK);
        String refBase = null;
        try {
            lock.getLock();

            ByteBuffer bytes = ByteBuffer.allocate(4);
            if (lock.read(bytes) > 0) {
                bytes.flip();
                long val = bytes.getInt();
                if(val == REF_BOTH)
                    Logger.DEBUG("reference has been downloaded to local scratch: " + val);
                else {
                    Logger.INFO("downloading missing reference index files to local scratch");
                    String HDFSRef = HalvadeConf.getRefOnHDFS(conf);
                    FileSystem fs = FileSystem.get(new URI(HDFSRef), conf);
                    refBase = findFile(refDir, refSuffix, false); // refSuffix = HALVADE_BWA_SUFFIX
                    boolean foundExisting = (refBase != null);
                    if (!foundExisting)
                        refBase = refDir + refName + id; // refName = bwa_ref-

                    for (String suffix : refFiles) { //  refFiles = BWA_REF_FILES
                        attemptDownloadFileFromHDFS(context, fs, HDFSRef + suffix, refBase + suffix, RETRIES);                
                    }
                    Logger.INFO("FINISHED downloading the complete reference index to local scratch");
                    if(!foundExisting) {
                        File f = new File(refBase + refSuffix);
                        f.createNewFile();
                        f = new File(refBase + HALVADE_GATK_SUFFIX);
                        f.createNewFile();
                    }
                    bytes.clear();
                    bytes.putInt(REF_BOTH).flip();
                    lock.forceWrite(bytes);
                }
            } else {
                Logger.INFO("downloading missing reference index files to local scratch");
                String HDFSRef = HalvadeConf.getRefOnHDFS(conf);
                FileSystem fs = FileSystem.get(new URI(HDFSRef), conf);
                refBase = findFile(refDir, refSuffix, false);
                boolean foundExisting = (refBase != null);
                if (!foundExisting)
                    refBase = refDir + refName + id;

                for (String suffix : refFiles) {
                    attemptDownloadFileFromHDFS(context, fs, HDFSRef + suffix, refBase + suffix, RETRIES);                
                }
                Logger.INFO("FINISHED downloading the complete reference index to local scratch");
                if(!foundExisting) {
                    File f = new File(refBase + refSuffix);
                    f.createNewFile();
                    f = new File(refBase + HALVADE_GATK_SUFFIX);
                    f.createNewFile();
                }
                bytes.clear();
                bytes.putInt(REF_BOTH).flip();
                lock.forceWrite(bytes);
            }
            
        } catch (InterruptedException ex) {
            Logger.EXCEPTION(ex);
        } finally {
            lock.releaseLock();
        }
        if(refBase == null)
            refBase = findFile(refDir, refSuffix, false);
        return refBase + refFiles[0];
    }
    
    public static String downloadGATKIndex(TaskInputOutputContext context, String id) throws IOException, URISyntaxException {
        Configuration conf = context.getConfiguration();
        String tmpDir = HalvadeConf.getScratchTempDir(conf);
        String refDir = HalvadeConf.getRefDirOnScratch(conf);
        if(!refDir.endsWith("/")) refDir = refDir + "/";
        HalvadeFileLock lock = new HalvadeFileLock(context, refDir, REF_LOCK);
        String refBase = null;
        try {
            lock.getLock();

            ByteBuffer bytes = ByteBuffer.allocate(4);
            if (lock.read(bytes) > 0) {
                bytes.flip();
                long val = bytes.getInt();
                if(val == REF_BOTH || val == DEFAULT_LOCK_VAL)
                    Logger.DEBUG("reference has been downloaded to local scratch: " + val);
                else {
                    Logger.INFO("downloading missing reference index files to local scratch");
                    String HDFSRef = HalvadeConf.getRefOnHDFS(conf);
                    FileSystem fs = FileSystem.get(new URI(HDFSRef), conf);
                    refBase = findFile(refDir, HALVADE_GATK_SUFFIX, false);
                    boolean foundExisting = (refBase != null);
                    if (!foundExisting)
                        refBase = refDir + "bwa_ref-" + id;

                    for (String suffix : GATK_REF_FILES) {
                        attemptDownloadFileFromHDFS(context, fs, HDFSRef + suffix, refBase + suffix, RETRIES);                
                    }
                    Logger.INFO("FINISHED downloading the complete reference index to local scratch");
                    if(!foundExisting) {
                        File f = new File(refBase + HALVADE_GATK_SUFFIX);
                        f.createNewFile();
                    }
                    bytes.clear();
                    bytes.putInt(DEFAULT_LOCK_VAL).flip();
                    lock.forceWrite(bytes);
                }
            } else {
                Logger.INFO("downloading missing reference index files to local scratch");
                String HDFSRef = HalvadeConf.getRefOnHDFS(conf);
                FileSystem fs = FileSystem.get(new URI(HDFSRef), conf);
                refBase = findFile(refDir, HALVADE_GATK_SUFFIX, false);
                boolean foundExisting = (refBase != null);
                if (!foundExisting)
                    refBase = refDir + "bwa_ref-" + id;

                for (String suffix : GATK_REF_FILES) {
                    attemptDownloadFileFromHDFS(context, fs, HDFSRef + suffix, refBase + suffix, RETRIES);                
                }
                Logger.INFO("FINISHED downloading the complete reference index to local scratch");
                if(!foundExisting) {
                    File f = new File(refBase + HALVADE_GATK_SUFFIX);
                    f.createNewFile();
                }
                bytes.clear();
                bytes.putInt(DEFAULT_LOCK_VAL).flip();
                lock.forceWrite(bytes);
            }
        
            
        } catch (InterruptedException ex) {
            Logger.EXCEPTION(ex);
        } finally {
            lock.releaseLock();
        }
        if(refBase == null)
            refBase = findFile(refDir, HALVADE_GATK_SUFFIX, false);
        return refBase + GATK_REF_FILES[0];
    }
    
    public static String downloadSTARIndex(TaskInputOutputContext context, String id, boolean usePass2Genome) throws IOException, URISyntaxException {
        Configuration conf = context.getConfiguration();
        String tmpDir = HalvadeConf.getScratchTempDir(conf);
        String refDir = HalvadeConf.getRefDirOnScratch(conf);
        if(!refDir.endsWith("/")) refDir = refDir + "/";
        HalvadeFileLock lock = new HalvadeFileLock(context, tmpDir, STARG_LOCK);
        String refBase = null;
        try {
            lock.getLock();

            ByteBuffer bytes = ByteBuffer.allocate(4);
            if (lock.read(bytes) > 0) {
                bytes.flip();
                long val = bytes.getInt();
                if(val == DEFAULT_LOCK_VAL)
                    Logger.DEBUG("reference has been downloaded to local scratch: " + val);
                else {
                    Logger.INFO("downloading missing reference index files to local scratch");
                    if(usePass2Genome) Logger.DEBUG("using Pass2 genome");
                    String HDFSRef = usePass2Genome ? HalvadeConf.getStarDirPass2HDFS(conf) : HalvadeConf.getStarDirOnHDFS(conf);
                    Logger.DEBUG("downloading STAR genome from: " + HDFSRef);
                    FileSystem fs = FileSystem.get(new URI(HDFSRef), conf);
                    refBase = findFile(refDir, usePass2Genome ? HALVADE_STAR_SUFFIX_P2 : HALVADE_STAR_SUFFIX_P1, true);
                    boolean foundExisting = (refBase != null);
                    if (!foundExisting) {
                        refBase = refDir + id + "-star/";
                        //make dir
                        File makeRefDir = new File (refBase);
                        makeRefDir.mkdir();
                    }
                    Logger.DEBUG("STAR dir: " + refBase);
                    if(!usePass2Genome) {
                        for (String suffix : STAR_REF_FILES) {
                            attemptDownloadFileFromHDFS(context, fs, HDFSRef + suffix, refBase + suffix, RETRIES);                
                        }
                        for (String suffix : STAR_REF_OPTIONAL_FILES) {
                            if(fs.exists(new Path(HDFSRef + suffix))) 
                                attemptDownloadFileFromHDFS(context, fs, HDFSRef + suffix, refBase + suffix, RETRIES); 
                        }
                    }
                    Logger.INFO("FINISHED downloading the complete reference index to local scratch");
                    if(!foundExisting) {
                        File f = new File(refBase + (usePass2Genome ? HALVADE_STAR_SUFFIX_P2 : HALVADE_STAR_SUFFIX_P1));
                        f.createNewFile();
                    }
                    bytes.clear();
                    bytes.putInt(DEFAULT_LOCK_VAL).flip();
                    lock.forceWrite(bytes);
                }
            } else {
                Logger.INFO("downloading missing reference index files to local scratch");
                if(usePass2Genome) Logger.DEBUG("using Pass2 genome");
                String HDFSRef = usePass2Genome ? HalvadeConf.getStarDirPass2HDFS(conf) : HalvadeConf.getStarDirOnHDFS(conf);
                Logger.DEBUG("downloading STAR genome from: " + HDFSRef);
                FileSystem fs = FileSystem.get(new URI(HDFSRef), conf);
                refBase = findFile(refDir, usePass2Genome ? HALVADE_STAR_SUFFIX_P2 : HALVADE_STAR_SUFFIX_P1, true);
                boolean foundExisting = (refBase != null);
                if (!foundExisting) {
                    refBase = refDir + id + "-star/";
                    //make dir
                    File makeRefDir = new File (refBase);
                    makeRefDir.mkdir();
                }
                Logger.DEBUG("STAR dir: " + refBase);
                if(!usePass2Genome) {
                    for (String suffix : STAR_REF_FILES) {
                        attemptDownloadFileFromHDFS(context, fs, HDFSRef + suffix, refBase + suffix, RETRIES);                
                    }
                    for (String suffix : STAR_REF_OPTIONAL_FILES) {
                        if(fs.exists(new Path(HDFSRef + suffix))) 
                            attemptDownloadFileFromHDFS(context, fs, HDFSRef + suffix, refBase + suffix, RETRIES); 
                    }
                }
                Logger.INFO("FINISHED downloading the complete reference index to local scratch");
                if(!foundExisting) {
                    File f = new File(refBase + (usePass2Genome ? HALVADE_STAR_SUFFIX_P2 : HALVADE_STAR_SUFFIX_P1));
                    f.createNewFile();
                }
                bytes.clear();
                bytes.putInt(DEFAULT_LOCK_VAL).flip();
                lock.forceWrite(bytes);
            }
        
        
        } catch (InterruptedException ex) {
            Logger.EXCEPTION(ex);
        } finally {
            lock.releaseLock();
        }
        if(refBase == null)
            refBase = findFile(refDir, usePass2Genome ? HALVADE_STAR_SUFFIX_P2 : HALVADE_STAR_SUFFIX_P1, true);
        return refBase;
    }
    
    public static String[] downloadSites(TaskInputOutputContext context, String id) throws IOException, URISyntaxException, InterruptedException {  
        Configuration conf = context.getConfiguration();
        String tmpDir = HalvadeConf.getScratchTempDir(conf);
        String refDir = HalvadeConf.getRefDirOnScratch(conf);
        String HDFSsites[] = HalvadeConf.getKnownSitesOnHDFS(conf);
        String[] localSites = new String[HDFSsites.length];
        if(!refDir.endsWith("/")) refDir = refDir + "/";
        HalvadeFileLock lock = new HalvadeFileLock(context, refDir, DBSNP_LOCK);
        String refBase = null;
        try {
            lock.getLock();
            
            ByteBuffer bytes = ByteBuffer.allocate(4);
            if (lock.read(bytes) > 0) {
                bytes.flip();
                long val = bytes.getInt();
                if(val == DEFAULT_LOCK_VAL)
                    Logger.DEBUG("dbSNP has been downloaded to local scratch: " + val);
                else {
                    Logger.INFO("downloading missing dbSNP to local scratch");   
                    refBase = findFile(refDir, HALVADE_DBSNP_SUFFIX, true);
                    boolean foundExisting = (refBase != null);
                    if (!foundExisting) {
                        refBase = refDir + id + "-dbsnp/";
                        //make dir
                        File makeRefDir = new File (refBase);
                        makeRefDir.mkdir();
                    }
                    Logger.DEBUG("dbSNP dir: " + refBase);

                    for (int i = 0; i < HDFSsites.length; i++) {
                        String fullName = HDFSsites[i];
                        String name = fullName.substring(fullName.lastIndexOf('/') + 1);
                        Logger.DEBUG("Downloading " + name);
                        FileSystem fs = FileSystem.get(new URI(fullName), conf);
                        attemptDownloadFileFromHDFS(context, fs, fullName, refBase + name, RETRIES);
                        localSites[i] = refBase + name;
                        // attempt to download .idx file
                        if(!foundExisting && fs.exists(new Path(fullName + ".idx")))
                            attemptDownloadFileFromHDFS(context, fs, fullName + ".idx", refBase + name + ".idx", RETRIES);
                    }

                    Logger.INFO("finished downloading the new sites to local scratch");
                    if(!foundExisting) {
                        File f = new File(refBase + HALVADE_DBSNP_SUFFIX);
                        f.createNewFile();
                    } 
                    bytes.clear();
                    bytes.putInt(DEFAULT_LOCK_VAL).flip();
                    lock.forceWrite(bytes);
                }
            } else {
                Logger.INFO("downloading missing dbSNP to local scratch");   
                refBase = findFile(refDir, HALVADE_DBSNP_SUFFIX, true);
                boolean foundExisting = (refBase != null);
                if (!foundExisting) {
                    refBase = refDir + id + "-dbsnp/";
                    //make dir
                    File makeRefDir = new File (refBase);
                    makeRefDir.mkdir();
                }
                Logger.DEBUG("dbSNP dir: " + refBase);

                for (int i = 0; i < HDFSsites.length; i++) {
                    String fullName = HDFSsites[i];
                    String name = fullName.substring(fullName.lastIndexOf('/') + 1);
                    Logger.DEBUG("Downloading " + name);
                    FileSystem fs = FileSystem.get(new URI(fullName), conf);
                    attemptDownloadFileFromHDFS(context, fs, fullName, refBase + name, RETRIES);
                    localSites[i] = refBase + name;
                    // attempt to download .idx file
                    if(!foundExisting && fs.exists(new Path(fullName + ".idx")))
                        attemptDownloadFileFromHDFS(context, fs, fullName + ".idx", refBase + name + ".idx", RETRIES);
                }

                Logger.INFO("finished downloading the new sites to local scratch");
                if(!foundExisting) {
                    File f = new File(refBase + HALVADE_DBSNP_SUFFIX);
                    f.createNewFile();
                } 
                bytes.clear();
                bytes.putInt(DEFAULT_LOCK_VAL).flip();
                lock.forceWrite(bytes);
            }
        } catch (InterruptedException ex) {
            Logger.EXCEPTION(ex);
        } finally {
            lock.releaseLock();
        }
        if(refBase == null){
            refBase = findFile(refDir, HALVADE_DBSNP_SUFFIX, true);
            File dir = new File(refBase);
            File[] directoryListing = dir.listFiles();
            if (directoryListing != null) {
                int found = 0;
                for (int i = 0; i < HDFSsites.length; i ++) {
                    String fullName = HDFSsites[i];
                    String name = fullName.substring(fullName.lastIndexOf('/') + 1);
                    localSites[i] = refBase + name;
                    if((new File(localSites[i])).exists())
                        found++;
                    else
                        Logger.DEBUG(name + " not found in local scratch");
                }
                if(found != HDFSsites.length) {
                    throw new IOException(refBase + " has different number of files: " + 
                            directoryListing.length + " vs " + localSites.length);
                }
            } else {
                throw new IOException(refBase + " has no files");
            }
        }
        return localSites;
    }
    
    
    public static boolean removeLocalFile(String filename) {
        return removeLocalFile(false, filename);
    }
    public static boolean removeLocalFile(boolean keep, String filename) {
        if(keep) return false;
        File f = new File(filename);
        return f.exists() && f.delete();
    }
    
    public static boolean removeLocalFile(String filename, TaskInputOutputContext context, HalvadeCounters counter) {
        return removeLocalFile(false, filename, context, counter);
    }
    
    public static boolean removeLocalFile(boolean keep, String filename, TaskInputOutputContext context, HalvadeCounters counter) {
        if(keep) return false;
        File f = new File(filename);
        if(f.exists()) context.getCounter(counter).increment(f.length());
        return f.exists() && f.delete();
    }
    
    
    public static boolean removeLocalDir(String filename) {
        return removeLocalDir(false, filename);
    }
    public static boolean removeLocalDir(boolean keep, String filename) {
        if(keep) return false;
        File f = new File(filename);
        return f.exists() && deleteDir(f); // f.delete();
    }
    public static  boolean removeLocalDir(String filename, TaskInputOutputContext context, HalvadeCounters counter) {
        return removeLocalDir(false, filename, context, counter);
    } 
    public static  boolean removeLocalDir(boolean keep, String filename, TaskInputOutputContext context, HalvadeCounters counter) {
        if(keep) return false;
        File f = new File(filename);
        if(f.exists()) context.getCounter(counter).increment(f.length());
        return f.exists() && deleteDir(f); // f.delete();
    } 
    protected static  boolean deleteDir(File dir) {
    	if(dir.exists()) {
    		File[] files = dir.listFiles();
    		if(files != null) {
    			for ( int i = 0; i < files.length; i++) {
    				if(files[i].isDirectory()) 
    					deleteDir(files[i]);
    				else
    					files[i].delete();
    			}
    		}
    	}
    	return dir.delete();
    }
}
