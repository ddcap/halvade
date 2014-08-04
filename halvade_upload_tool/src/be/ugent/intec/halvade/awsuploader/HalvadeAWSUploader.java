/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package be.ugent.intec.halvade.awsuploader;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import org.apache.commons.cli.*;

/**
 *
 * @author ddecap
 */
public class HalvadeAWSUploader {
    protected Options options = new Options();
    private int mthreads = 1;
    private String manifest;
    private String outputDir;
    private String credFile;
    private String existingBucketName;
    private int bestFileSize = 60000000;
    
    
    private String accessKey;
    private String secretKey;
    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws InterruptedException, IOException, ParseException {
        // TODO code application logic here
        HalvadeAWSUploader hau = new HalvadeAWSUploader();
        hau.run(args);
    }
    
    public void run(String[] args) throws IOException, InterruptedException {   
        try {
            parseArguments(args);
            readCredentials();
            AWSUploader upl = new AWSUploader(existingBucketName, accessKey, secretKey);        
            processFiles(upl);        
            upl.shutDownNow(); 
        } catch (ParseException e) {
            // automatically generate the help statement
            System.err.println("Error parsing: " + e.getMessage());
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp( "java -jar HalvadeAWSUploader -M <MANIFEST> -O <OUT> -B <BUCKET> [options]", options );
        }
    }
    
    private int processFiles(AWSUploader upl) throws IOException, InterruptedException {    
        Timer timer = new Timer();
        timer.start();
        
        FastQFileReader pairedReader = FastQFileReader.getPairedInstance();
        FastQFileReader singleReader = FastQFileReader.getSingleInstance();
        long filesize = 0L;
        if(manifest != null) {
            Logger.DEBUG("reading input files from " + manifest);
            // read from file
            BufferedReader br = new BufferedReader(new FileReader(manifest)); 
            String line;
            while ((line = br.readLine()) != null) {
                String[] files = line.split("\t");
                if(files.length == 2) {
                    pairedReader.addFilePair(files[0], files[1]);
                    File f = new File(files[0]);
                    filesize += f.length();
                    f = new File(files[1]);
                    filesize += f.length();
                } else if(files.length == 1) {
                    singleReader.addSingleFile(files[0]);
                    File f = new File(files[0]);
                    filesize += f.length();
                }
            }
        }
        
        int bestThreads = mthreads;
        long maxFileSize = getBestFileSize(); 
        InterleaveFiles[] fileThreads = new InterleaveFiles[bestThreads];
        // start interleaveFile threads
        for(int t = 0; t < bestThreads; t++) {
            fileThreads[t] = new InterleaveFiles(outputDir + "pthread" + t + "_",  outputDir + "sthread" + t + "_", maxFileSize, upl);
            fileThreads[t].start();
        }
        for(int t = 0; t < bestThreads; t++)
            fileThreads[t].join();
        timer.stop();
        Logger.DEBUG("Time to process data: " + timer.getFormattedCurrentTime());
        return 0;
    }
    
    private long getBestFileSize() {
        return bestFileSize;
    }
    
    private void readCredentials() throws FileNotFoundException, IOException {        
        ObjectMapper jsonMapper = new ObjectMapper();
        JsonNode root = jsonMapper.readTree(new File(credFile));
        accessKey = root.get("access_id").asText();
        secretKey = root.get("private_key").asText();    
    }
    
    public void createOptions() {
        Option optOut = OptionBuilder.withArgName( "output" )
                                .hasArg()
                                .isRequired(true)
                                .withDescription(  "Output directory on s3." )
                                .create( "O" );
        Option optBucket = OptionBuilder.withArgName( "bucket" )
                                .hasArg()
                                .isRequired(true)
                                .withDescription(  "Output bucket for s3." )
                                .create( "B" );
        Option optCred = OptionBuilder.withArgName( "credentials" )
                                .hasArg()
                                .isRequired(true)
                                .withDescription(  "Give the credential file to access AWS." )
                                .create( "cred" );
        Option optMan = OptionBuilder.withArgName( "manifest" )
                                .hasArg()
                                .isRequired(true)
                                .withDescription(  "Filename containing the input files to be put on S3/HDFS." )
                                .create( "M" );
        Option optSize = OptionBuilder.withArgName( "size" )
                                .hasArg()
                                .withDescription(  "Sets the maximum filesize of each split in MB." )
                                .create( "s" );
        Option optThreads = OptionBuilder.withArgName( "threads" )
                                .hasArg()
                                .withDescription(  "Sets the available threads [1]." )
                                .create( "t" );
        
        options.addOption(optOut);
        options.addOption(optMan);
        options.addOption(optThreads);
        options.addOption(optBucket);
        options.addOption(optCred);
        options.addOption(optSize);
    }
    
    public void parseArguments(String[] args) throws ParseException {
        createOptions();
        CommandLineParser parser = new GnuParser();
        CommandLine line = parser.parse(options, args);
        manifest = line.getOptionValue("M");
        outputDir = line.getOptionValue("O");
        if(outputDir.startsWith("/")) outputDir = outputDir.substring(1);
        if(!outputDir.endsWith("/")) outputDir += "/";
        existingBucketName = line.getOptionValue("B");
        credFile = line.getOptionValue("cred");
        
        if(line.hasOption("t"))
            mthreads = Integer.parseInt(line.getOptionValue("t"));
        if(line.hasOption("s"))
            bestFileSize = Integer.parseInt(line.getOptionValue("s"));
    }
}
