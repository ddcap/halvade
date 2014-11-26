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
import java.util.ArrayList;
import java.util.zip.GZIPInputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;

/**
 *
 * @author ddecap
 */
public class HDFSFileIO {
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

    
    // functions download BWA/STAR/GATK references/dbSNP database
    
    protected static String HALVADE_BWA_SUFFIX = ".bwa_ref";
    protected static String HALVADE_GATK_SUFFIX = ".gatk_ref";
    protected static String HALVADE_STAR_SUFFIX = ".star_ref";
    protected static String HALVADE_DBSNP_SUFFIX = ".dbsnps";
    protected static String[] BWA_REF_FILES = 
        {".fasta", ".fasta.amb", ".fasta.ann", ".fasta.bwt", ".fasta.pac", ".fasta.sa", ".fasta.fai", ".dict" }; 
    protected static String[] GATK_REF_FILES =  {".fasta", ".fasta.fai", ".dict" }; 
    protected static String[] STAR_REF_FILES = 
        {"chrLength.txt", "chrNameLength.txt", "chrName.txt", "chrStart.txt", 
         "Genome", "genomeParameters.txt", "SA", "SAindex"};
        
        
    protected static String findFile(String directory, String suffix, boolean recursive) {
        File dir  = new File(directory);
        if(dir != null && dir.listFiles() != null) {
            String foundPrefix = null;
            for (File file : dir.listFiles()) {
                if(file.isDirectory() && recursive) {
                    foundPrefix = findFile(file.getAbsolutePath(), suffix, recursive);
                } else if (file.getName().endsWith(suffix)) {
                    foundPrefix = file.getAbsolutePath().substring(0, file.getAbsolutePath().lastIndexOf('.'));
                    Logger.DEBUG("found existing ref: \"" + foundPrefix + "\"");
                }
            }
            return foundPrefix;
        } else 
            return null;
    }
    
    public static String downloadBWAIndex(TaskInputOutputContext context, String id) throws IOException, URISyntaxException {
        Logger.INFO("downloading missing reference index files to local scratch");
        Configuration conf = context.getConfiguration();
        String refDir = HalvadeConf.getRefDirOnScratch(conf);
        if(!refDir.endsWith("/")) refDir = refDir + "/";
        String HDFSRef = HalvadeConf.getRefOnHDFS(conf);
        FileSystem fs = FileSystem.get(new URI(HDFSRef), conf);
        
        String refBase = findFile(refDir, HALVADE_BWA_SUFFIX, false);
        boolean foundExisting = (refBase != null);
        if (!foundExisting)
            refBase = refDir + "bwa_ref-" + id;
        
        for (String suffix : BWA_REF_FILES) {
            attemptDownloadFileFromHDFS(context, fs, HDFSRef + suffix, refBase + suffix, RETRIES);                
        }
        Logger.INFO("FINISHED downloading the complete reference index to local scratch");
        if(!foundExisting) {
            File f = new File(refBase + HALVADE_BWA_SUFFIX);
            f.createNewFile();
            f = new File(refBase + HALVADE_GATK_SUFFIX);
            f.createNewFile();
        }
        return refBase + BWA_REF_FILES[0];
    }
    
    public static String downloadGATKIndex(TaskInputOutputContext context, String id) throws IOException, URISyntaxException {
        Logger.INFO("downloading missing reference index files to local scratch");
        Configuration conf = context.getConfiguration();
        String refDir = HalvadeConf.getRefDirOnScratch(conf);
        if(!refDir.endsWith("/")) refDir = refDir + "/";
        String HDFSRef = HalvadeConf.getRefOnHDFS(conf);
        FileSystem fs = FileSystem.get(new URI(HDFSRef), conf);
        
        String refBase = findFile(refDir, HALVADE_GATK_SUFFIX, false);
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
        return refBase + GATK_REF_FILES[0];
    }
    
    public static String downloadSTARIndex(TaskInputOutputContext context, String id) throws IOException, URISyntaxException {
        Logger.INFO("downloading missing reference index files to local scratch");
        Configuration conf = context.getConfiguration();
        String refDir = HalvadeConf.getRefDirOnScratch(conf);
        if(!refDir.endsWith("/")) refDir = refDir + "/";
        String HDFSRef = HalvadeConf.getStarDirOnHDFS(conf);
        FileSystem fs = FileSystem.get(new URI(HDFSRef), conf);
        
        String refBase = findFile(refDir, HALVADE_STAR_SUFFIX, true);
        boolean foundExisting = (refBase != null);
        if (!foundExisting) {
            refBase = refDir + id + "-star/";
            //make dir
            File makeRefDir = new File (refBase);
            makeRefDir.mkdir();
        }
        Logger.DEBUG("STAR dir: " + refBase);
        
        for (String suffix : STAR_REF_FILES) {
            attemptDownloadFileFromHDFS(context, fs, HDFSRef + suffix, refBase + suffix, RETRIES);                
        }
        Logger.INFO("FINISHED downloading the complete reference index to local scratch");
        if(!foundExisting) {
            File f = new File(refBase + HALVADE_STAR_SUFFIX);
            f.createNewFile();
        }
        return refBase;
    }
    
    public static String[] downloadSites(TaskInputOutputContext context, String id) throws IOException, URISyntaxException, InterruptedException {  
        Logger.INFO("downloading missing sites to local scratch");
        Configuration conf = context.getConfiguration();        
        String HDFSsites[] = HalvadeConf.getKnownSitesOnHDFS(conf);
        String refDir = HalvadeConf.getRefDirOnScratch(conf);
        
        String refBase = findFile(refDir, HALVADE_DBSNP_SUFFIX, true);
        boolean foundExisting = (refBase != null);
        if (!foundExisting) {
            refBase = refDir + id + "-dbsnp/";
            //make dir
            File makeRefDir = new File (refBase);
            makeRefDir.mkdir();
        }
        Logger.DEBUG("dbSNP dir: " + refBase);
        
        String[] localSites = new String[HDFSsites.length];
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
        return localSites;
    }
}
