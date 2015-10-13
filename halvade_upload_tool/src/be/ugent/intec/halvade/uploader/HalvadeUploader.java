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

package be.ugent.intec.halvade.uploader;

import be.ugent.intec.halvade.uploader.input.FileReaderFactory;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 *
 * @author ddecap
 */
public class HalvadeUploader  extends Configured implements Tool {
    protected Options options = new Options();
    private int mthreads = 1;
    private boolean isInterleaved = false;
    private CompressionCodec codec;
    private String manifest;
    private String file1;
    private String file2;
    private String outputDir;
    private int bestFileSize = 60000000; // <64MB
    
    
    private AWSCredentials credentials;
    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws Exception {
        // TODO code application logic here
        Configuration c = new Configuration();
        HalvadeUploader hau = new HalvadeUploader();
        int res = ToolRunner.run(c, hau, args);
    }
    
    @Override
    public int run(String[] strings) throws Exception {
        try {
            parseArguments(strings);  
            processFiles();
        } catch (ParseException e) {
            // automatically generate the help statement
            System.err.println("Error parsing: " + e.getMessage());
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp( "java -jar HalvadeAWSUploader -1 <MANIFEST> -O <OUT> [options]", options );
        } catch (Throwable ex) {
            Logger.THROWABLE(ex);
        }
        return 0;
    }
    
    private int processFiles() throws IOException, InterruptedException, URISyntaxException, Throwable {    
        Timer timer = new Timer();
        timer.start();
        
        AWSUploader upl = null;
        FileSystem fs = null;
        // write to s3?
        boolean useAWS = false;
        if(outputDir.startsWith("s3")) {
            useAWS = true;
            String existingBucketName = outputDir.replace("s3://","").split("/")[0];
            outputDir = outputDir.replace("s3://" + existingBucketName, "");
            upl = new AWSUploader(existingBucketName);
        } else {
            Configuration conf = getConf();
            fs = FileSystem.get(new URI(outputDir), conf);
            Path outpath = new Path(outputDir);
            if (fs.exists(outpath) && !fs.getFileStatus(outpath).isDirectory()) {
                Logger.DEBUG("please provide an output directory");
                return 1;
            }
        }
        
        FileReaderFactory factory = FileReaderFactory.getInstance(mthreads);
        if(manifest != null) {
            Logger.DEBUG("reading input files from " + manifest);
            // read from file
            BufferedReader br = new BufferedReader(new FileReader(manifest)); 
            String line;
            while ((line = br.readLine()) != null) {
                String[] files = line.split("\t");
                if(files.length == 2) {
                    factory.addReader(files[0], files[1], false);
                } else if(files.length == 1) {
                    factory.addReader(files[0], null, isInterleaved);
                }
            }
        } else if (file1 != null && file2 != null) {
            Logger.DEBUG("Paired-end read input in 2 files.");    
            factory.addReader(file1, file2, false);            
        } else if (file1 != null) {
            if(isInterleaved) 
                Logger.DEBUG("Single-end read input in 1 files.");   
            else 
                Logger.DEBUG("Paired-end read input in 1 files.");     
            factory.addReader(file1, null, isInterleaved);   
        } else {
            Logger.DEBUG("Incorrect input, use either a manifest file or give both file1 and file2 as input.");     
        }
        
        // start reading
        (new Thread(factory)).start();
        
        int bestThreads = mthreads;
        long maxFileSize = getBestFileSize(); 
        if(useAWS) {
            AWSInterleaveFiles[] fileThreads = new AWSInterleaveFiles[bestThreads];
            // start interleaveFile threads
            for(int t = 0; t < bestThreads; t++) {
                fileThreads[t] = new AWSInterleaveFiles(
                        outputDir + "halvade_" + t + "_", 
                        maxFileSize, 
                        upl, t, codec);
                fileThreads[t].start();
            }
            for(int t = 0; t < bestThreads; t++)
                fileThreads[t].join();
            if(upl != null)
                upl.shutDownNow(); 
        } else {
            
            HDFSInterleaveFiles[] fileThreads = new HDFSInterleaveFiles[bestThreads];
            // start interleaveFile threads
            for(int t = 0; t < bestThreads; t++) {
                fileThreads[t] = new HDFSInterleaveFiles(
                        outputDir + "halvade_" + t + "_", 
                        maxFileSize, 
                        fs, t, codec);
                fileThreads[t].start();
            }
            for(int t = 0; t < bestThreads; t++)
                fileThreads[t].join();
        }
        factory.finalize();
        timer.stop();
        Logger.DEBUG("Time to process data: " + timer.getFormattedCurrentTime());     
        return 0;
    }
    
    private long getBestFileSize() {
        return bestFileSize;
    }
    
    
    public void createOptions() {
        Option optOut = OptionBuilder.withArgName( "output" )
                                .hasArg()
                                .isRequired(true)
                                .withDescription(  "Output directory on s3 (s3://bucketname/folder/) or HDFS (/dir/on/hdfs/)." )
                                .create( "O" );
        Option optFile1 = OptionBuilder.withArgName( "manifest/input1" )
                                .hasArg()
                                .isRequired(true)
                                .withDescription(  "The filename containing the input files to be put on S3/HDFS, must be .manifest. " + 
                                        "Or the first input file itself (fastq), '-' reads from stdin." )
                                .create( "1" );
        Option optFile2 = OptionBuilder.withArgName( "fastq2" )
                                .hasArg()
                                .withDescription(  "The second fastq file." )
                                .create( "2" );
        Option optSize = OptionBuilder.withArgName( "size" )
                                .hasArg()
                                .withDescription(  "Sets the maximum filesize of each split in MB." )
                                .create( "size" );
        Option optThreads = OptionBuilder.withArgName( "threads" )
                                .hasArg()
                                .withDescription(  "Sets the available threads [1]." )
                                .create( "t" );
        Option optInter = OptionBuilder.withArgName( "" )
                                .withDescription(  "The single file input files contain interleaved paired-end reads." )
                                .create( "i" );
        Option optSnappy = OptionBuilder.withArgName( "" )
                                .withDescription(  "Compress the output files with snappy (faster) instead of gzip. The snappy library needs to be installed in Hadoop." )
                                .create( "snappy" );
        Option optLz4 = OptionBuilder.withArgName( "" )
                                .withDescription(  "Compress the output files with lz4 (faster) instead of gzip. The lz4 library needs to be installed in Hadoop." )
                                .create( "lz4" );
        
        options.addOption(optOut);
        options.addOption(optFile1);
        options.addOption(optFile2);
        options.addOption(optThreads);
        options.addOption(optSize);
        options.addOption(optInter);
        options.addOption(optSnappy);
        options.addOption(optLz4);
    }
    
    public void parseArguments(String[] args) throws ParseException {
        createOptions();
        CommandLineParser parser = new GnuParser();
        CommandLine line = parser.parse(options, args);
        manifest = line.getOptionValue("1");
        if(!manifest.endsWith(".manifest")) {
            file1 = manifest;
            manifest = null;
        }
        outputDir = line.getOptionValue("O");
        if(!outputDir.endsWith("/")) outputDir += "/";
        
        if (line.hasOption("2"))
            file2 = line.getOptionValue("2");     
        if(line.hasOption("t"))
            mthreads = Integer.parseInt(line.getOptionValue("t"));
        if(line.hasOption("i"))
            isInterleaved = true;
        if(line.hasOption("snappy")) {       
            CompressionCodecFactory codecFactory = new CompressionCodecFactory(getConf());
            codec = codecFactory.getCodecByClassName("org.apache.hadoop.io.compress.SnappyCodec");
        }
        if(line.hasOption("lz4")) {       
            CompressionCodecFactory codecFactory = new CompressionCodecFactory(getConf());
            codec = codecFactory.getCodecByClassName("org.apache.hadoop.io.compress.Lz4Codec");
        }
        if(codec != null)
            Logger.DEBUG("Hadoop encryption: " + codec.getDefaultExtension().substring(1));
        if(line.hasOption("size"))
            bestFileSize = Integer.parseInt(line.getOptionValue("size")) * 1024 * 1024;
    }

}
