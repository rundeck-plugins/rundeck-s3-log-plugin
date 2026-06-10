package org.rundeck.plugins;

import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import org.junit.Assert;

import java.io.InputStream;

/**
* Mock S3 client for testing that fails on unexpected calls
*/
class FailS3 implements S3Client {

    @Override
    public String serviceName() {
        return "s3";
    }

    @Override
    public void close() {
        // Do nothing
    }

    @Override
    public HeadObjectResponse headObject(HeadObjectRequest headObjectRequest) throws NoSuchKeyException, S3Exception, SdkClientException {
        Assert.fail("unexpected");
        return null;
    }

    @Override
    public PutObjectResponse putObject(PutObjectRequest putObjectRequest, software.amazon.awssdk.core.sync.RequestBody requestBody) throws S3Exception, SdkClientException {
        Assert.fail("unexpected");
        return null;
    }

    @Override
    public DeleteObjectResponse deleteObject(DeleteObjectRequest deleteObjectRequest) throws S3Exception, SdkClientException {
        Assert.fail("unexpected");
        return null;
    }

    @Override
    public software.amazon.awssdk.core.ResponseInputStream<GetObjectResponse> getObject(GetObjectRequest getObjectRequest) throws NoSuchKeyException, S3Exception, SdkClientException {
        Assert.fail("unexpected");
        return null;
    }
}