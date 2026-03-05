package org.rundeck.plugins;

import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import com.dtolabs.rundeck.core.logging.ExecutionFileStorageException;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.*;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * Test class for S3LogFileStoragePlugin
 */
@RunWith(JUnit4.class)
public class S3LogFileStoragePluginTest {

    final String DEFAULT_FILETYPE = "rdlog";

    @Test
    public void expandPathLeadingSlashIsRemoved() {
        Assert.assertEquals("monkey", S3LogFileStoragePlugin.expandPath("/monkey", testContext()));
    }

    @Test
    public void expandPathMultiSlashRemoved() {
        Assert.assertEquals("monkey/test/blah", S3LogFileStoragePlugin.expandPath("/monkey//test///blah",
                testContext()));
    }

    @Test
    public void expandExecId() {
        Assert.assertEquals("monkey/testexecid/blah", S3LogFileStoragePlugin.expandPath("monkey/${job.execid}/blah",
                testContext()));
    }

    @Test
    public void expandProject() {
        Assert.assertEquals("monkey/testproject/blah", S3LogFileStoragePlugin.expandPath("monkey/${job.project}/blah",
                testContext()));
    }

    @Test
    public void missingKey() {
        Assert.assertEquals("monkey/blah", S3LogFileStoragePlugin.expandPath("monkey/${job.id}/blah", testContext()));
    }

    @Test
    public void expandJobId() {
        Assert.assertEquals("monkey/testjobid/blah", S3LogFileStoragePlugin.expandPath("monkey/${job.id}/blah",
                testContext2()));
    }


    private HashMap<String, Object> testContext() {
        HashMap<String, Object> stringHashMap = new HashMap<String, Object>();
        stringHashMap.put("execid", "testexecid");
        stringHashMap.put(S3LogFileStoragePlugin.META_ID_FOR_LOGSTORE, "testexecid");
        stringHashMap.put("project", "testproject");
        stringHashMap.put("url", "http://rundeck:4440/execution/5/show");
        stringHashMap.put("serverUrl", "http://rundeck:4440");
        stringHashMap.put("serverUUID", "123");
        return stringHashMap;
    }

    private HashMap<String, ?> testContext2() {
        HashMap<String, Object> stringHashMap = testContext();
        stringHashMap.put("id", "testjobid");
        return stringHashMap;
    }

    private HashMap<String, ?> testContext3() {
        HashMap<String, Object> stringHashMap = new HashMap<String, Object>(testContext2());
        stringHashMap.put("name", "jobname");
        stringHashMap.put("group", "ajob group/another group/");
        return stringHashMap;
    }

    class testS3 extends FailS3 {
        AwsCredentials creds;
        private Region region;
        private String endpoint;
        private software.amazon.awssdk.core.ResponseInputStream<GetObjectResponse> getObjectResponse;

        testS3(AwsCredentials creds) {
            this.creds = creds;
        }
        testS3(){
            this.creds=null;
        }

        public boolean headObject404 = false;
        public String headObject404Match = null;
        public boolean headObjectS3Exception = false;
        public boolean headObjectClientException = false;
        public HeadObjectResponse headObjectResponse;

        @Override
        public HeadObjectResponse headObject(HeadObjectRequest headObjectRequest) throws NoSuchKeyException, S3Exception, SdkClientException {
            if (headObject404 || null != headObject404Match && headObjectRequest.key().matches(headObject404Match)) {
                throw NoSuchKeyException.builder()
                        .message("test NOT Found")
                        .build();
            }
            if (headObjectS3Exception) {
                throw S3Exception.builder()
                        .message("blah")
                        .statusCode(500)
                        .build();
            }
            if (headObjectClientException) {
                throw SdkClientException.create("blah");
            }
            return headObjectResponse;
        }

        public String getObjectBucketName;
        public String getObjectkey;
        boolean getObjectClientException = false;
        boolean getObjectS3Exception = false;

        @Override
        public software.amazon.awssdk.core.ResponseInputStream<GetObjectResponse> getObject(GetObjectRequest getObjectRequest) throws NoSuchKeyException, S3Exception, SdkClientException {
            getObjectBucketName = getObjectRequest.bucket();
            getObjectkey = getObjectRequest.key();
            if (getObjectClientException) {
                throw SdkClientException.create("getObject");
            }
            if (getObjectS3Exception) {
                throw S3Exception.builder()
                        .message("getObject")
                        .statusCode(500)
                        .build();
            }
            return getObjectResponse;
        }

        public boolean putObjectClientException = false;
        public boolean putObjectS3Exception = false;
        public PutObjectResponse putObjectResponse;
        public PutObjectRequest putObjectRequest;
        public software.amazon.awssdk.core.sync.RequestBody putObjectRequestBody;

        @Override
        public PutObjectResponse putObject(PutObjectRequest putObjectRequest, software.amazon.awssdk.core.sync.RequestBody requestBody) throws S3Exception, SdkClientException {
            this.putObjectRequest = putObjectRequest;
            this.putObjectRequestBody = requestBody;
            if (putObjectClientException) {
                throw SdkClientException.create("putObject");
            }
            if (putObjectS3Exception) {
                throw S3Exception.builder()
                        .message("putObject")
                        .statusCode(500)
                        .build();
            }
            return putObjectResponse;
        }

        public boolean deleteObjectExpected = false;
        public boolean deleteObjectError;
        public String[] deleteObjectCalled = new String[2];

        @Override
        public DeleteObjectResponse deleteObject(DeleteObjectRequest deleteObjectRequest)
                throws S3Exception, SdkClientException
        {
            if (!deleteObjectExpected) {
                super.deleteObject(deleteObjectRequest);
            }
            deleteObjectCalled[0] = deleteObjectRequest.bucket();
            deleteObjectCalled[1] = deleteObjectRequest.key();
            if (deleteObjectError) {
                throw S3Exception.builder()
                        .message("deleteObject")
                        .statusCode(500)
                        .build();
            }
            return DeleteObjectResponse.builder().build();
        }

        public Region getRegion() {
            return region;
        }

        public void setRegion(Region region) {
            this.region = region;
        }

        public String getEndpoint() {
            return endpoint;
        }

        public void setEndpoint(String endpoint) {
            this.endpoint = endpoint;
        }
    }

    class testPlugin extends S3LogFileStoragePlugin {
        testPlugin() {
            setPath(S3LogFileStoragePlugin.DEFAULT_PATH_FORMAT);
            setRegion(S3LogFileStoragePlugin.DEFAULT_REGION);
        }

        testS3 testS3;

        @Override
        protected S3Client createS3Client(AwsCredentialsProvider credentialsProvider) {
            testS3 = new S3LogFileStoragePluginTest.testS3(credentialsProvider.resolveCredentials());
            return testS3;
        }

        @Override
        protected S3Client createS3Client() {
            testS3 = new S3LogFileStoragePluginTest.testS3();
            return testS3;
        }

        public S3LogFileStoragePluginTest.testS3 getTestS3() {
            return testS3;
        }

        public void setTestS3(S3LogFileStoragePluginTest.testS3 testS3) {
            this.testS3 = testS3;
        }
    }

    @Test
    public void initializeUnsetCredentialsAccessKey() {

        testPlugin testPlugin = new S3LogFileStoragePluginTest.testPlugin();
        testPlugin.setAWSSecretKey("blah");
        try {
            testPlugin.initialize(testContext());
            Assert.fail("Should thrown exception");
        } catch (IllegalArgumentException e) {
            Assert.assertTrue(e.getMessage().contains("must both be configured"));
            Assert.assertTrue(e.getMessage().contains("AWSAccessKeyId"));
            Assert.assertTrue(e.getMessage().contains("AWSSecretKey"));
        }
    }

    @Test
    public void initializeUnsetCredentialsSecretKey() {

        testPlugin testPlugin = new S3LogFileStoragePluginTest.testPlugin();
        testPlugin.setAWSAccessKeyId("blah");
        try {
            testPlugin.initialize(testContext());
            Assert.fail("Should thrown exception");
        } catch (IllegalArgumentException e) {
            Assert.assertTrue(e.getMessage().contains("must both be configured"));
            Assert.assertTrue(e.getMessage().contains("AWSAccessKeyId"));
            Assert.assertTrue(e.getMessage().contains("AWSSecretKey"));
        }
    }

    @Test
    public void initializeCredentialsFileDoesNotExist() {

        testPlugin testPlugin = new S3LogFileStoragePluginTest.testPlugin();
        testPlugin.setAWSCredentialsFile("/blah/file/does/not/exist");
        try {
            testPlugin.initialize(testContext());
            Assert.fail("Should thrown exception");
        } catch (IllegalArgumentException e) {
            Assert.assertTrue(e.getMessage().contains("Credentials file does not exist or cannot be read"));
        }
    }

    @Test
    public void initializeCredentialsFileMissingCredentials() throws IOException {
        testPlugin testPlugin = new S3LogFileStoragePluginTest.testPlugin();
        Properties p = new Properties();
        p.setProperty("a", "b");
        File t = File.createTempFile("test-credentials", ".properties");
        t.deleteOnExit();
        p.store(new FileOutputStream(t), "test");
        testPlugin.setAWSCredentialsFile(t.getAbsolutePath());
        try {
            testPlugin.initialize(testContext());
            Assert.fail("Should thrown exception");
        } catch (RuntimeException e) {
            Assert.assertTrue(e.getMessage().contains("must contain 'accessKey' and 'secretKey' properties"));
        }
    }

    @Test
    public void initializeCredentialsFileMissingSecretKey() throws IOException {
        testPlugin testPlugin = new S3LogFileStoragePluginTest.testPlugin();
        Properties p = new Properties();
        p.setProperty("accessKey", "blah");
        File t = File.createTempFile("test-credentials", ".properties");
        t.deleteOnExit();
        p.store(new FileOutputStream(t), "test");
        testPlugin.setAWSCredentialsFile(t.getAbsolutePath());
        try {
            testPlugin.initialize(testContext());
            Assert.fail("Should thrown exception");
        } catch (RuntimeException e) {
            Assert.assertTrue(e.getMessage().contains("must contain 'accessKey' and 'secretKey' properties"));
        }
    }
}