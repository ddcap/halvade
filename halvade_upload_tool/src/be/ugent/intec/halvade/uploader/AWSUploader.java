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
