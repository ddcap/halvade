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

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.Upload;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;

/**
 *
 * @author ddecap
 */
public class AWSUploader {
    
    private String existingBucketName;
    private TransferManager tm;

    
    public AWSUploader(String existingBucketName) throws IOException {
        this.existingBucketName = existingBucketName;
        AWSCredentials c;
        try{
            DefaultAWSCredentialsProviderChain prov = new DefaultAWSCredentialsProviderChain();
            c= prov.getCredentials();
        } catch(AmazonClientException ex) {
            // read from ~/.aws/credentials
            String access = null;
            String secret = null;
            try (BufferedReader br = new BufferedReader(new FileReader(System.getProperty("user.home") + "/.aws/credentials"))) {
                String line;
                while ((line = br.readLine()) != null && !line.contains("[default]")) {}
                line = br.readLine();
                if(line != null)
                    access = line.split(" = ")[1];
                line = br.readLine();
                if(line != null)
                    secret = line.split(" = ")[1];
            }
            c = new BasicAWSCredentials(access, secret);
        }
        this.tm = new TransferManager(c);
    }
    
    public void shutDownNow() {
        tm.shutdownNow();
    }
    
    public void Upload(String key, InputStream input, long size) throws InterruptedException {
        ObjectMetadata meta = new ObjectMetadata();
        meta.setServerSideEncryption(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION);   
        meta.setContentLength(size);
        Upload upload = tm.upload(existingBucketName, key, input, meta);
        
        try {
        	// Or you can block and wait for the upload to finish
        	upload.waitForCompletion();
                Logger.DEBUG("Upload complete.");
        } catch (AmazonClientException amazonClientException) {
        	Logger.DEBUG("Unable to upload file, upload was aborted.");
        	Logger.EXCEPTION(amazonClientException);
        }
    }
}
