/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package be.ugent.intec.halvade.awsuploader;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.Upload;
import java.io.InputStream;

/**
 *
 * @author ddecap
 */
public class AWSUploader {
    
    private String existingBucketName;
    private BasicAWSCredentials credentials;
    private TransferManager tm;

    public AWSUploader(String existingBucketName, String accessKey, String secretKey) {
        this.existingBucketName = existingBucketName;
        this.credentials = new BasicAWSCredentials(accessKey, secretKey);
        this.tm = new TransferManager(credentials);
    }
    
    public void shutDownNow() {
        tm.shutdownNow();
    }
    
    public void Upload(String key, InputStream input, long size) throws InterruptedException {
        ObjectMetadata meta = new ObjectMetadata();
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
