package org.rundeck.plugins;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.*;
import com.dtolabs.rundeck.core.logging.LogFileState;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.*;
import java.util.*;

/**
 * $INTERFACE is ... User: greg Date: 6/11/13 Time: 1:59 PM
 */
@RunWith(JUnit4.class)
public class S3LogFileStoragePluginTest {

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
        stringHashMap.put("project", "testproject");
        return stringHashMap;
    }

    private HashMap<String, ?> testContext2() {
        HashMap<String, Object> stringHashMap = testContext();
        stringHashMap.put("id", "testjobid");
        return stringHashMap;
    }

    class testS3 extends FailS3 {
        AWSCredentials creds;
        private Region region;
        private S3Object getObject;

        testS3(AWSCredentials creds) {
            this.creds = creds;
        }


        public boolean getObjectMetadata404 = false;
        public boolean getObjectMetadataS3Exception = false;
        public boolean getObjectMetadataClientException = false;
        public ObjectMetadata getObjectMetadata;

        public ObjectMetadata getObjectMetadata(GetObjectMetadataRequest getObjectMetadataRequest) throws
                AmazonClientException, AmazonServiceException {
            if (getObjectMetadata404) {
                AmazonS3Exception ase = new AmazonS3Exception("test NOT Found");
                ase.setStatusCode(404);
                ase.setRequestId("requestId");
                ase.setExtendedRequestId("extendedRequestId");
                throw ase;
            }
            if (getObjectMetadataS3Exception) {
                AmazonS3Exception ase = new AmazonS3Exception("blah");
                ase.setRequestId("requestId");
                ase.setExtendedRequestId("extendedRequestId");
                throw ase;
            }
            if (getObjectMetadataClientException) {
                AmazonClientException ase = new AmazonClientException("blah");
                throw ase;
            }
            return getObjectMetadata;
        }

        public String getObjectBucketName;
        public String getObjectkey;

        public S3Object getObject(String bucketName, String key) throws AmazonClientException, AmazonServiceException {
            getObjectBucketName = bucketName;
            getObjectkey = key;
            return getObject;
        }


        public boolean putObjectClientException = false;
        public boolean putObjectS3Exception = false;
        public PutObjectResult putObject;
        public PutObjectRequest putObjectRequest;

        public PutObjectResult putObject(PutObjectRequest putObjectRequest) throws AmazonClientException,
                AmazonServiceException {
            this.putObjectRequest = putObjectRequest;
            if (putObjectClientException) {
                throw new AmazonClientException("exception");
            }
            if (putObjectS3Exception) {
                throw new AmazonS3Exception("exception");
            }
            return putObject;
        }


        public Region getRegion() {
            return region;
        }

        public void setRegion(Region region) {
            this.region = region;
        }
    }

    class testPlugin extends S3LogFileStoragePlugin {
        testPlugin() {
            setPath(DEFAULT_PATH_FORMAT);
            setRegion(DEFAULT_REGION);
        }

        private testS3 testS3;

        @Override
        protected AmazonS3 createAmazonS3Client(AWSCredentials awsCredentials) {
            testS3 = new S3LogFileStoragePluginTest.testS3(awsCredentials);
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
    public void initializeUnsetCredentials() {

        testPlugin testPlugin = new S3LogFileStoragePluginTest.testPlugin();
        try {
            testPlugin.initialize(testContext());
            Assert.fail("Should thrown exception");
        } catch (IllegalArgumentException e) {
            Assert.assertTrue(e.getMessage().contains("must be configured"));
            Assert.assertTrue(e.getMessage().contains("AWSAccessKeyId"));
            Assert.assertTrue(e.getMessage().contains("AWSSecretKey"));
            Assert.assertTrue(e.getMessage().contains("AWSCredentialsFile"));
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
            Assert.assertTrue(e.getMessage().contains("must be configured"));
            Assert.assertTrue(e.getMessage().contains("AWSAccessKeyId"));
            Assert.assertTrue(e.getMessage().contains("AWSSecretKey"));
            Assert.assertTrue(e.getMessage().contains("AWSCredentialsFile"));
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
            Assert.assertTrue(e.getMessage().contains("must be configured"));
            Assert.assertTrue(e.getMessage().contains("AWSAccessKeyId"));
            Assert.assertTrue(e.getMessage().contains("AWSSecretKey"));
            Assert.assertTrue(e.getMessage().contains("AWSCredentialsFile"));
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
        } catch (IllegalArgumentException e) {
            Assert.assertTrue(e.getMessage().contains("doesn't contain the expected properties 'accessKey' and " +
                    "'secretKey'."));
        }
    }

    @Test
    public void initializeCredentialsFileMissingSecretKey() throws IOException {
        testPlugin testPlugin = new S3LogFileStoragePluginTest.testPlugin();
        Properties p = new Properties();
        p.setProperty("a", "b");
        p.setProperty("accessKey", "c");
        File t = File.createTempFile("test-credentials", ".properties");
        t.deleteOnExit();
        p.store(new FileOutputStream(t), "test");
        testPlugin.setAWSCredentialsFile(t.getAbsolutePath());
        try {
            testPlugin.initialize(testContext());
            Assert.fail("Should thrown exception");
        } catch (IllegalArgumentException e) {
            Assert.assertTrue(e.getMessage().contains("doesn't contain the expected properties 'accessKey' and " +
                    "'secretKey'."));
        }
    }

    @Test
    public void initializeCredentialsFileMissingAccessKey() throws IOException {
        testPlugin testPlugin = new S3LogFileStoragePluginTest.testPlugin();
        Properties p = new Properties();
        p.setProperty("a", "b");
        p.setProperty("secretKey", "c");
        File t = File.createTempFile("test-credentials", ".properties");
        t.deleteOnExit();
        p.store(new FileOutputStream(t), "test");
        testPlugin.setAWSCredentialsFile(t.getAbsolutePath());
        try {
            testPlugin.initialize(testContext());
            Assert.fail("Should thrown exception");
        } catch (IllegalArgumentException e) {
            Assert.assertTrue(e.getMessage().contains("doesn't contain the expected properties 'accessKey' and " +
                    "'secretKey'."));
        }
    }

    @Test
    public void initializeValidCredentials() {
        testPlugin testPlugin = new S3LogFileStoragePluginTest.testPlugin();
        testPlugin.setBucket("blah");
        testPlugin.setAWSAccessKeyId("blah");
        testPlugin.setAWSSecretKey("blah");
        testPlugin.initialize(testContext());
        Assert.assertNotNull(testPlugin.getTestS3().getRegion());
    }

    @Test
    public void initializeValidCredentialsFile() throws IOException {
        testPlugin testPlugin = new S3LogFileStoragePluginTest.testPlugin();
        testPlugin.setBucket("blah");
        Properties p = new Properties();
        p.setProperty("accessKey", "b");
        p.setProperty("secretKey", "c");
        File t = File.createTempFile("test-credentials", ".properties");
        t.deleteOnExit();
        p.store(new FileOutputStream(t), "test");
        testPlugin.setAWSCredentialsFile(t.getAbsolutePath());
        testPlugin.initialize(testContext());
        Assert.assertNotNull(testPlugin.getTestS3().getRegion());
    }

    @Test
    public void initializeInvalidRegion() {
        testPlugin testPlugin = new S3LogFileStoragePluginTest.testPlugin();
        testPlugin.setBucket("blah");
        testPlugin.setAWSAccessKeyId("blah");
        testPlugin.setAWSSecretKey("blah");
        testPlugin.setRegion("mulklahoma");
        try {
            testPlugin.initialize(testContext());
            Assert.fail("Expected failure");
        } catch (IllegalArgumentException e) {
            Assert.assertTrue(e.getMessage().contains("Region was not found"));
        }
    }
    @Test
    public void initializeInvalidBucket() {
        testPlugin testPlugin = new S3LogFileStoragePluginTest.testPlugin();
        testPlugin.setBucket("");
        testPlugin.setAWSAccessKeyId("blah");
        testPlugin.setAWSSecretKey("blah");
        try {
            testPlugin.initialize(testContext());
            Assert.fail("Expected failure");
        } catch (IllegalArgumentException e) {
            Assert.assertTrue(e.getMessage().contains("bucket was not set"));
        }
    }
    @Test
    public void initializeNullBucket() {
        testPlugin testPlugin = new S3LogFileStoragePluginTest.testPlugin();
        testPlugin.setBucket(null);
        testPlugin.setAWSAccessKeyId("blah");
        testPlugin.setAWSSecretKey("blah");
        try {
            testPlugin.initialize(testContext());
            Assert.fail("Expected failure");
        } catch (IllegalArgumentException e) {
            Assert.assertTrue(e.getMessage().contains("bucket was not set"));
        }
    }
    @Test
    public void initializeNullPath() {
        testPlugin testPlugin = new S3LogFileStoragePluginTest.testPlugin();
        testPlugin.setBucket("asdf");
        testPlugin.setPath(null);
        testPlugin.setAWSAccessKeyId("blah");
        testPlugin.setAWSSecretKey("blah");
        try {
            testPlugin.initialize(testContext());
            Assert.fail("Expected failure");
        } catch (IllegalArgumentException e) {
            Assert.assertTrue(e.getMessage().contains("path was not set"));
        }
    }
    @Test
    public void initializeInvalidPath() {
        testPlugin testPlugin = new S3LogFileStoragePluginTest.testPlugin();
        testPlugin.setBucket("basdf");
        testPlugin.setPath("");
        testPlugin.setAWSAccessKeyId("blah");
        testPlugin.setAWSSecretKey("blah");
        try {
            testPlugin.initialize(testContext());
            Assert.fail("Expected failure");
        } catch (IllegalArgumentException e) {
            Assert.assertTrue(e.getMessage().contains("path was not set"));
        }
    }
    @Test
    public void initializeInvalidPathNoExecID() {
        testPlugin testPlugin = new S3LogFileStoragePluginTest.testPlugin();
        testPlugin.setBucket("basdf");
        testPlugin.setPath("blah/logs");
        testPlugin.setAWSAccessKeyId("blah");
        testPlugin.setAWSSecretKey("blah");
        try {
            testPlugin.initialize(testContext());
            Assert.fail("Expected failure");
        } catch (IllegalArgumentException e) {
            Assert.assertTrue(e.getMessage().contains("path must contain ${job.execid} or end with /"));
        }
    }
    @Test
    public void initializePathNoExecIDWithSlash() {
        testPlugin testPlugin = new S3LogFileStoragePluginTest.testPlugin();
        testPlugin.setBucket("basdf");
        testPlugin.setPath("blah/logs/");
        testPlugin.setAWSAccessKeyId("blah");
        testPlugin.setAWSSecretKey("blah");
        testPlugin.initialize(testContext());
    }
    @Test
    public void initializePathWithExecIDEndsWithSlash() {
        testPlugin testPlugin = new S3LogFileStoragePluginTest.testPlugin();
        testPlugin.setBucket("basdf");
        testPlugin.setPath("blah/${job.execid}/");
        testPlugin.setAWSAccessKeyId("blah");
        testPlugin.setAWSSecretKey("blah");
        try {
            testPlugin.initialize(testContext());
            Assert.fail("Expected failure");
        } catch (IllegalArgumentException e) {
            Assert.assertTrue(e.getMessage().contains("path must not end with /"));
        }
    }
    @Test
    public void initializePathWithExecIDValid() {
        testPlugin testPlugin = new S3LogFileStoragePluginTest.testPlugin();
        testPlugin.setBucket("basdf");
        testPlugin.setPath("blah/${job.execid}.blah");
        testPlugin.setAWSAccessKeyId("blah");
        testPlugin.setAWSSecretKey("blah");
        testPlugin.initialize(testContext());
    }

    @Test
    public void getState404() {
        testPlugin testPlugin = initializeTestPlugin();
        testPlugin.getTestS3().getObjectMetadata404 = true;
        Assert.assertEquals(LogFileState.NOT_FOUND, testPlugin.getState());
    }

    @Test
    public void getStateOk() {
        testPlugin testPlugin = initializeTestPlugin();
        testPlugin.getTestS3().getObjectMetadata = new ObjectMetadata();
        Assert.assertEquals(LogFileState.AVAILABLE, testPlugin.getState());
    }

    @Test
    public void getStateS3Exception() {
        testPlugin testPlugin = initializeTestPlugin();
        testPlugin.getTestS3().getObjectMetadataS3Exception = true;
        Assert.assertEquals(LogFileState.NOT_FOUND, testPlugin.getState());
    }

    @Test
    public void getStateClientException() {
        testPlugin testPlugin = initializeTestPlugin();
        testPlugin.getTestS3().getObjectMetadataClientException = true;
        Assert.assertEquals(LogFileState.NOT_FOUND, testPlugin.getState());
    }

    @Test
    public void storeClientException() throws IOException {
        testPlugin testPlugin = initializeTestPlugin();
        testPlugin.getTestS3().putObjectClientException = true;
        boolean result = testPlugin.store(null, 0, null);
        Assert.assertFalse(result);
    }

    @Test
    public void storeS3Exception() throws IOException {
        testPlugin testPlugin = initializeTestPlugin();
        testPlugin.getTestS3().putObjectS3Exception = true;
        boolean result = testPlugin.store(null, 0, null);
        Assert.assertFalse(result);
    }

    @Test
    public void storeMetadata() throws IOException {
        testPlugin testPlugin = initializeTestPlugin();
        testPlugin.getTestS3().putObject = new PutObjectResult();
        Date lastModified = new Date();
        int length = 123;
        boolean result = testPlugin.store(null, length, lastModified);
        Assert.assertTrue(result);
        Assert.assertEquals(length, testPlugin.getTestS3().putObjectRequest.getMetadata().getContentLength());
        Assert.assertEquals(lastModified, testPlugin.getTestS3().putObjectRequest.getMetadata().getLastModified());
        Map<String, String> userMetadata = testPlugin.getTestS3().putObjectRequest.getMetadata().getUserMetadata();
        Assert.assertEquals(2, userMetadata.size());
        Assert.assertEquals(testContext().get("execid"), userMetadata.get("rundeck.execid"));
        Assert.assertEquals(testContext().get("project"), userMetadata.get("rundeck.project"));
    }

    class testOutputStream extends OutputStream{
        boolean wasWrite=false;
        @Override
        public void write(int i) throws IOException {
            wasWrite=true;
        }

        boolean wasClosed;
        @Override
        public void close() throws IOException {
            wasClosed = true;
        }

    }
    class testInputStream extends InputStream{
        boolean wasRead=false;
        int bytes=1;
        boolean readIOException=false;
        @Override
        public int read() throws IOException {
            wasRead=true;
            if(readIOException) {
                throw new IOException("testInputStream.readIOException");
            }
            --bytes;
            return bytes < 0 ? -1 : bytes;
        }

        boolean wasClosed=false;
        @Override
        public void close() throws IOException {
            wasClosed=true;
            super.close();
        }
    }
    @Test
    public void retrieve() throws IOException {
        testPlugin testPlugin = initializeTestPlugin();
        testOutputStream stream = new testOutputStream();
        testInputStream testInputStream = new testInputStream();

        testPlugin.getTestS3().getObject = new S3Object();
        testPlugin.getTestS3().getObject.setObjectContent(testInputStream);

        boolean result = testPlugin.retrieve(stream);
        Assert.assertTrue(result);
        Assert.assertEquals("testBucket", testPlugin.getTestS3().getObjectBucketName);
        Assert.assertEquals("project/testproject/testexecid.rdlog", testPlugin.getTestS3().getObjectkey);
        Assert.assertTrue(testInputStream.wasRead);
        Assert.assertTrue(testInputStream.bytes < 0);
        Assert.assertTrue(testInputStream.wasClosed);
        Assert.assertTrue(stream.wasWrite);
        Assert.assertFalse(stream.wasClosed);
    }
    @Test
    public void retrieveInputIOException() throws IOException {
        testPlugin testPlugin = initializeTestPlugin();
        testOutputStream stream = new testOutputStream();
        testInputStream testInputStream = new testInputStream();
        testInputStream.readIOException=true;

        testPlugin.getTestS3().getObject = new S3Object();
        testPlugin.getTestS3().getObject.setObjectContent(testInputStream);

        boolean result = false;
        try {
            result = testPlugin.retrieve(stream);
            Assert.fail("should throw exception");
        } catch (IOException e) {
            Assert.assertEquals("testInputStream.readIOException", e.getMessage());
        }
        Assert.assertFalse(result);
        Assert.assertEquals("testBucket", testPlugin.getTestS3().getObjectBucketName);
        Assert.assertEquals("project/testproject/testexecid.rdlog", testPlugin.getTestS3().getObjectkey);
        Assert.assertTrue(testInputStream.wasRead);
        Assert.assertFalse(testInputStream.bytes < 0);
        Assert.assertTrue(testInputStream.wasClosed);
        Assert.assertFalse(stream.wasWrite);
        Assert.assertFalse(stream.wasClosed);
    }

    private testPlugin initializeTestPlugin() {
        testPlugin testPlugin = new S3LogFileStoragePluginTest.testPlugin();
        testPlugin.setAWSAccessKeyId("blah");
        testPlugin.setAWSSecretKey("blah");
        testPlugin.setBucket("testBucket");
        testPlugin.initialize(testContext());
        return testPlugin;
    }
}
