/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
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
    private static final int RETRIES = 3;

    public static String Unzip(String inFilePath) throws IOException  {
        GZIPInputStream gzipInputStream = new GZIPInputStream(new FileInputStream(inFilePath));

        String outFilePath = inFilePath.replace(".gz", "");
        OutputStream out = new FileOutputStream(outFilePath);

        byte[] buf = new byte[1024];
        int len;
        while ((len = gzipInputStream.read(buf)) > 0)
            out.write(buf, 0, len);

        gzipInputStream.close();
        out.close();

        new File(inFilePath).delete();

        return outFilePath;
    }
    /**
     * @return returns 0 if successfull, -1 if filesize is incorrect and -2 if an exception occurred
     */
    protected static int privateDownloadFileFromHDFS(TaskInputOutputContext context, FileSystem fs, 
            String from, String to) {
        try {
            // check if file is present on local scratch
            File f = new File(to);
            if(!f.exists()) {
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
    
    public static int downloadFileFromHDFS(TaskInputOutputContext context, FileSystem fs, 
            String from, String to) throws IOException {         
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

    protected static String[] indexFiles = {".amb", ".ann", ".bwt", ".pac", ".sa", // for bwa
                                           ".fai" }; // for gatk
    /**
     * 
     * @param conf configuration of the current hadoop job 
     *      stores temporary file directory and location of the index
     * @throws IOException 
     */
    public static String downloadBWAIndex(TaskInputOutputContext context, String id) throws IOException, URISyntaxException {
        Logger.INFO("downloading missing reference index files to local scratch");
        Configuration conf = context.getConfiguration();
        String scratchRef = MyConf.getRefOnScratch(conf);
        String refBase = scratchRef.substring(0, scratchRef.length() - 2) + id + ".fa";        
        // look for a file starting with scratchRef.base.id
        boolean useExisting =false;
        File dir  = new File(scratchRef.substring(0, scratchRef.lastIndexOf("/")));
        if(dir != null && dir.listFiles() != null) {
            for (File file : dir.listFiles()) {
                if (file.getName().endsWith(".fa__")) {
                    refBase = file.getAbsolutePath().substring(0, file.getAbsolutePath().length() - 2);
                    Logger.DEBUG("found existing ref: \"" + refBase + "\"");
                    useExisting = true;
                }
            }
        }
        
        String HDFSRef = MyConf.getRefOnHDFS(conf);
        FileSystem fs = FileSystem.get(new URI(HDFSRef), conf);
        attemptDownloadFileFromHDFS(context, fs, HDFSRef, refBase, RETRIES);
        attemptDownloadFileFromHDFS(context, fs, HDFSRef.substring(0, HDFSRef.lastIndexOf('.')) + ".dict",
                    refBase.substring(0, refBase.lastIndexOf('.')) + ".dict", RETRIES);

        for (String suffix : indexFiles) {
            attemptDownloadFileFromHDFS(context, fs, HDFSRef + suffix, refBase + suffix, RETRIES);                
        }
        Logger.INFO("FINISHED downloading the complete reference index to local scratch");
        if(!useExisting) {
            File f = new File(refBase + "__");
            f.createNewFile();
        }
        return refBase;
    }
    
    protected static boolean correctSize(String onHDFS, String onScratch, FileSystem fs) throws IOException {
        File f = new File(onScratch);
        return fs.getFileStatus(new Path(onHDFS)).getLen() == f.length();
    }
    
    public static String[] downloadSites(TaskInputOutputContext context, String id) throws IOException, URISyntaxException, InterruptedException {
        Logger.INFO("downloading missing sites to local scratch");
        Configuration conf = context.getConfiguration();
        String HDFSsites[] = MyConf.getKnownSitesOnHDFS(conf);
        ArrayList<FileSystem> fs = new ArrayList();
        for (int i = 0; i < HDFSsites.length; i++) {
            fs.add(FileSystem.get(new URI(HDFSsites[i]), conf));
        }
        
        String scratchsites[] = null;
        boolean found = false;
        File f = null;
        while(!found) {
            scratchsites = MyConf.findKnownSitesOnScratch(conf, id);
            int p = 0;
            f = new File(scratchsites[0].substring(0, scratchsites[0].lastIndexOf("0.vcf")) + "__");
            while(f.exists() && 
                    p < HDFSsites.length && 
                    correctSize(HDFSsites[p], scratchsites[p], fs.get(p))) {
                p++;
            }
            if(p == HDFSsites.length) {
                Logger.DEBUG("use old dbsnp on scratch [" + f.getName() + "]");
                found = true;
            } else if (!f.exists()) {
                Logger.DEBUG("copy new dbsnp on scratch  [" + f.getName() + "]");
                for (int i = 0; i < HDFSsites.length; i++) {
                    attemptDownloadFileFromHDFS(context, fs.get(p), HDFSsites[i], scratchsites[i], RETRIES);
                    // if idx is present download it too
                    if(fs.get(p).exists(new Path(HDFSsites[i] + ".idx")))
                        attemptDownloadFileFromHDFS(context, fs.get(p), HDFSsites[i] + ".idx", scratchsites[i] + ".idx", RETRIES);
                }
                // make a file ending with dbsnps__ -> files are ..dbsnpsY.idx
                f = new File(scratchsites[0].substring(0, scratchsites[0].lastIndexOf("0.vcf")) + "__");
                f.createNewFile();
                found = true;
            } else {
                // remove everything
                Logger.DEBUG("delete old dbsnp on scratch [" + f.getName() + "]");
                f.delete();
                for (int i = 0; i < HDFSsites.length; i++) {
                    f = new File(scratchsites[i]);
                    f.delete();
                    f = new File(scratchsites[i] + ".idx");
                    f.delete();
                }
            }
        }
        Logger.INFO("finished downloading the new sites to local scratch");
        return scratchsites;
    }
}
