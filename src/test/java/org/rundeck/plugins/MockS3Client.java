package org.rundeck.plugins;

import org.junit.Assert;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.awscore.exception.AwsServiceException;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

/**
 * Mock S3Client for testing
 */
class MockS3Client implements S3Client {

    // HeadObject (getObjectMetadata equivalent) mocking
    public boolean headObject404 = false;
    public String headObject404Match = null;
    public boolean headObjectS3Exception = false;
    public boolean headObjectClientException = false;
    public Map<String, String> headObjectMetadata;

    // GetObject mocking
    public String getObjectBucketName;
    public String getObjectKey;
    public boolean getObjectClientException = false;
    public boolean getObjectS3Exception = false;
    public byte[] getObjectContent;

    // PutObject mocking
    public boolean putObjectClientException = false;
    public boolean putObjectS3Exception = false;
    public PutObjectRequest putObjectRequest;
    public InputStream putObjectInputStream;

    // DeleteObject mocking
    public boolean deleteObjectExpect = false;
    public boolean deleteObjectError = false;
    public String[] deleteObjectCalled = new String[2];

    // Region/endpoint tracking
    private software.amazon.awssdk.regions.Region region;
    private String endpoint;

    @Override
    public HeadObjectResponse headObject(HeadObjectRequest request) throws S3Exception, SdkClientException {
        if (headObject404 || (headObject404Match != null && request.key().matches(headObject404Match))) {
            throw S3Exception.builder()
                    .message("test NOT Found")
                    .statusCode(404)
                    .build();
        }
        if (headObjectS3Exception) {
            throw S3Exception.builder()
                    .message("blah")
                    .statusCode(500)
                    .build();
        }
        if (headObjectClientException) {
            throw SdkClientException.builder()
                    .message("blah")
                    .build();
        }

        HeadObjectResponse.Builder responseBuilder = HeadObjectResponse.builder();
        if (headObjectMetadata != null) {
            responseBuilder.metadata(headObjectMetadata);
        }
        return responseBuilder.build();
    }

    @Override
    public <ReturnT> ReturnT getObject(GetObjectRequest request,
                                       software.amazon.awssdk.core.sync.ResponseTransformer<GetObjectResponse, ReturnT> responseTransformer)
            throws S3Exception, SdkClientException {
        getObjectBucketName = request.bucket();
        getObjectKey = request.key();

        if (getObjectClientException) {
            throw SdkClientException.builder()
                    .message("getObject")
                    .build();
        }
        if (getObjectS3Exception) {
            throw S3Exception.builder()
                    .message("getObject")
                    .statusCode(500)
                    .build();
        }

        GetObjectResponse response = GetObjectResponse.builder().build();
        InputStream inputStream = getObjectContent != null
                ? new ByteArrayInputStream(getObjectContent)
                : new ByteArrayInputStream(new byte[0]);

        ResponseInputStream<GetObjectResponse> responseInputStream =
                new ResponseInputStream<>(response, inputStream);

        return responseTransformer.transform(response, inputStream);
    }

    @Override
    public PutObjectResponse putObject(PutObjectRequest request,
                                       software.amazon.awssdk.core.sync.RequestBody requestBody)
            throws S3Exception, SdkClientException {
        this.putObjectRequest = request;

        if (putObjectClientException) {
            throw SdkClientException.builder()
                    .message("putObject")
                    .build();
        }
        if (putObjectS3Exception) {
            throw S3Exception.builder()
                    .message("putObject")
                    .statusCode(500)
                    .build();
        }

        return PutObjectResponse.builder().build();
    }

    @Override
    public DeleteObjectResponse deleteObject(DeleteObjectRequest request)
            throws S3Exception, SdkClientException {
        if (!deleteObjectExpect) {
            Assert.fail("unexpected deleteObject call");
        }
        deleteObjectCalled[0] = request.bucket();
        deleteObjectCalled[1] = request.key();

        if (deleteObjectError) {
            throw S3Exception.builder()
                    .message("deleteObject")
                    .statusCode(500)
                    .build();
        }

        return DeleteObjectResponse.builder().build();
    }

    @Override
    public String serviceName() {
        return "s3";
    }

    @Override
    public void close() {
        // No-op for mock
    }

    // Getters and setters for test verification
    public software.amazon.awssdk.regions.Region getRegion() {
        return region;
    }

    public void setRegion(software.amazon.awssdk.regions.Region region) {
        this.region = region;
    }

    public String getEndpoint() {
        return endpoint;
    }

    public void setEndpoint(String endpoint) {
        this.endpoint = endpoint;
    }
}
