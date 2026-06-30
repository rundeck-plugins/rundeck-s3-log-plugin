package org.rundeck.plugins;

import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.http.AbortableInputStream;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;
import com.dtolabs.rundeck.core.logging.ExecutionFileStorageException;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentCaptor;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for S3LogFileStoragePlugin (AWS SDK v2).
 */
@RunWith(JUnit4.class)
public class S3LogFileStoragePluginTest {

    final String DEFAULT_FILETYPE = "rdlog";

    // ─── Path expansion tests ─────────────────────────────────────────────────

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

    // ─── Contexts ─────────────────────────────────────────────────────────────

    private HashMap<String, Object> testContext() {
        HashMap<String, Object> map = new HashMap<>();
        map.put("execid", "testexecid");
        map.put(S3LogFileStoragePlugin.META_ID_FOR_LOGSTORE, "testexecid");
        map.put("project", "testproject");
        map.put("url", "http://rundeck:4440/execution/5/show");
        map.put("serverUrl", "http://rundeck:4440");
        map.put("serverUUID", "123");
        return map;
    }

    private HashMap<String, ?> testContext2() {
        HashMap<String, Object> map = testContext();
        map.put("id", "testjobid");
        return map;
    }

    private HashMap<String, ?> testContext3() {
        HashMap<String, Object> map = new HashMap<>(testContext2());
        map.put("name", "jobname");
        map.put("group", "ajob group/another group/");
        return map;
    }

    // ─── Inner test plugin ────────────────────────────────────────────────────

    /**
     * Overrides createS3Client to inject a Mockito mock and capture config values.
     */
    class testPlugin extends S3LogFileStoragePlugin {
        S3Client mockS3;
        Region capturedRegion;
        String capturedEndpoint;
        boolean capturedPathStyle;

        testPlugin() {
            setPath(DEFAULT_PATH_FORMAT);
            setRegion(DEFAULT_REGION);
        }

        @Override
        protected S3Client createS3Client(AwsCredentialsProvider credentials, Region region) {
            capturedRegion = region;
            capturedEndpoint = getEndpoint();
            capturedPathStyle = isPathStyle();
            mockS3 = mock(S3Client.class);
            return mockS3;
        }
    }

    // ─── initialize: credential validation ───────────────────────────────────

    @Test
    public void initializeUnsetCredentialsAccessKey() {
        testPlugin plugin = new testPlugin();
        plugin.setAWSSecretKey("blah");
        try {
            plugin.initialize(testContext());
            Assert.fail("Should throw exception");
        } catch (IllegalArgumentException e) {
            Assert.assertTrue(e.getMessage().contains("must both be configured"));
            Assert.assertTrue(e.getMessage().contains("AWSAccessKeyId"));
            Assert.assertTrue(e.getMessage().contains("AWSSecretKey"));
        }
    }

    @Test
    public void initializeUnsetCredentialsSecretKey() {
        testPlugin plugin = new testPlugin();
        plugin.setAWSAccessKeyId("blah");
        try {
            plugin.initialize(testContext());
            Assert.fail("Should throw exception");
        } catch (IllegalArgumentException e) {
            Assert.assertTrue(e.getMessage().contains("must both be configured"));
            Assert.assertTrue(e.getMessage().contains("AWSAccessKeyId"));
            Assert.assertTrue(e.getMessage().contains("AWSSecretKey"));
        }
    }

    @Test
    public void initializeCredentialsFileDoesNotExist() {
        testPlugin plugin = new testPlugin();
        plugin.setAWSCredentialsFile("/blah/file/does/not/exist");
        try {
            plugin.initialize(testContext());
            Assert.fail("Should throw exception");
        } catch (IllegalArgumentException e) {
            Assert.assertTrue(e.getMessage().contains("Credentials file does not exist or cannot be read"));
        }
    }

    @Test
    public void initializeCredentialsFileMissingCredentials() throws IOException {
        testPlugin plugin = new testPlugin();
        Properties p = new Properties();
        p.setProperty("a", "b");
        File t = File.createTempFile("test-credentials", ".properties");
        t.deleteOnExit();
        p.store(new FileOutputStream(t), "test");
        plugin.setAWSCredentialsFile(t.getAbsolutePath());
        try {
            plugin.initialize(testContext());
            Assert.fail("Should throw exception");
        } catch (IllegalArgumentException e) {
            Assert.assertTrue(e.getMessage().contains("doesn't contain the expected properties 'accessKey' and " +
                    "'secretKey'."));
        }
    }

    @Test
    public void initializeCredentialsFileMissingSecretKey() throws IOException {
        testPlugin plugin = new testPlugin();
        Properties p = new Properties();
        p.setProperty("a", "b");
        p.setProperty("accessKey", "c");
        File t = File.createTempFile("test-credentials", ".properties");
        t.deleteOnExit();
        p.store(new FileOutputStream(t), "test");
        plugin.setAWSCredentialsFile(t.getAbsolutePath());
        try {
            plugin.initialize(testContext());
            Assert.fail("Should throw exception");
        } catch (IllegalArgumentException e) {
            Assert.assertTrue(e.getMessage().contains("doesn't contain the expected properties 'accessKey' and " +
                    "'secretKey'."));
        }
    }

    @Test
    public void initializeCredentialsFileMissingAccessKey() throws IOException {
        testPlugin plugin = new testPlugin();
        Properties p = new Properties();
        p.setProperty("a", "b");
        p.setProperty("secretKey", "c");
        File t = File.createTempFile("test-credentials", ".properties");
        t.deleteOnExit();
        p.store(new FileOutputStream(t), "test");
        plugin.setAWSCredentialsFile(t.getAbsolutePath());
        try {
            plugin.initialize(testContext());
            Assert.fail("Should throw exception");
        } catch (IllegalArgumentException e) {
            Assert.assertTrue(e.getMessage().contains("doesn't contain the expected properties 'accessKey' and " +
                    "'secretKey'."));
        }
    }

    // ─── initialize: region / endpoint ────────────────────────────────────────

    @Test
    public void initializeValidCredentials() {
        testPlugin plugin = new testPlugin();
        plugin.setBucket("blah");
        plugin.setAWSAccessKeyId("blah");
        plugin.setAWSSecretKey("blah");
        plugin.initialize(testContext());
        Assert.assertNotNull(plugin.capturedRegion);
    }

    @Test
    public void initializeValidCredentialsDefault() {
        testPlugin plugin = new testPlugin();
        plugin.setBucket("blah");
        plugin.initialize(testContext());
        Assert.assertNotNull(plugin.capturedRegion);
    }

    @Test
    public void initializeValidCredentialsFile() throws IOException {
        testPlugin plugin = new testPlugin();
        plugin.setBucket("blah");
        Properties p = new Properties();
        p.setProperty("accessKey", "b");
        p.setProperty("secretKey", "c");
        File t = File.createTempFile("test-credentials", ".properties");
        t.deleteOnExit();
        p.store(new FileOutputStream(t), "test");
        plugin.setAWSCredentialsFile(t.getAbsolutePath());
        plugin.initialize(testContext());
        Assert.assertNotNull(plugin.capturedRegion);
    }

    @Test
    public void initializeInvalidRegion() {
        testPlugin plugin = new testPlugin();
        plugin.setBucket("blah");
        plugin.setAWSAccessKeyId("blah");
        plugin.setAWSSecretKey("blah");
        plugin.setRegion("mulklahoma");
        try {
            plugin.initialize(testContext());
            Assert.fail("Expected failure");
        } catch (IllegalArgumentException e) {
            Assert.assertTrue(e.getMessage().contains("Region was not found"));
        }
    }

    @Test
    public void initializeEndpoint() {
        testPlugin plugin = new testPlugin();
        plugin.setAWSAccessKeyId("blah");
        plugin.setAWSSecretKey("blah");
        plugin.setBucket("testBucket");
        plugin.setEndpoint("localhost");
        plugin.initialize(testContext());
        Assert.assertEquals("localhost", plugin.capturedEndpoint);
    }

    @Test
    public void initializeEndpointDefault() {
        testPlugin plugin = new testPlugin();
        plugin.setAWSAccessKeyId("blah");
        plugin.setAWSSecretKey("blah");
        plugin.setBucket("testBucket");
        plugin.initialize(testContext());
        Assert.assertNull(plugin.capturedEndpoint);
    }

    @Test
    public void initializeDefaultPathStyle() {
        testPlugin plugin = new testPlugin();
        plugin.setAWSAccessKeyId("blah");
        plugin.setAWSSecretKey("blah");
        plugin.setBucket("testBucket");
        plugin.initialize(testContext());
        Assert.assertFalse(plugin.capturedPathStyle);
    }

    @Test
    public void initializePathStyleFalse() {
        testPlugin plugin = new testPlugin();
        plugin.setAWSAccessKeyId("blah");
        plugin.setAWSSecretKey("blah");
        plugin.setBucket("testBucket");
        plugin.setPathStyle(false);
        plugin.initialize(testContext());
        Assert.assertFalse(plugin.capturedPathStyle);
    }

    @Test
    public void initializeWithPathStyleTrue() {
        testPlugin plugin = new testPlugin();
        plugin.setAWSAccessKeyId("blah");
        plugin.setAWSSecretKey("blah");
        plugin.setBucket("testBucket");
        plugin.setPathStyle(true);
        plugin.initialize(testContext());
        Assert.assertTrue(plugin.capturedPathStyle);
    }

    // ─── initialize: bucket / path validation ────────────────────────────────

    @Test
    public void initializeInvalidBucket() {
        testPlugin plugin = new testPlugin();
        plugin.setBucket("");
        plugin.setAWSAccessKeyId("blah");
        plugin.setAWSSecretKey("blah");
        try {
            plugin.initialize(testContext());
            Assert.fail("Expected failure");
        } catch (IllegalArgumentException e) {
            Assert.assertTrue(e.getMessage().contains("bucket was not set"));
        }
    }

    @Test
    public void initializeNullBucket() {
        testPlugin plugin = new testPlugin();
        plugin.setBucket(null);
        plugin.setAWSAccessKeyId("blah");
        plugin.setAWSSecretKey("blah");
        try {
            plugin.initialize(testContext());
            Assert.fail("Expected failure");
        } catch (IllegalArgumentException e) {
            Assert.assertTrue(e.getMessage().contains("bucket was not set"));
        }
    }

    @Test
    public void initializeNullPath() {
        testPlugin plugin = new testPlugin();
        plugin.setBucket("asdf");
        plugin.setPath(null);
        plugin.setAWSAccessKeyId("blah");
        plugin.setAWSSecretKey("blah");
        try {
            plugin.initialize(testContext());
            Assert.fail("Expected failure");
        } catch (IllegalArgumentException e) {
            Assert.assertTrue(e.getMessage().contains("path was not set"));
        }
    }

    @Test
    public void initializeInvalidPath() {
        testPlugin plugin = new testPlugin();
        plugin.setBucket("basdf");
        plugin.setPath("");
        plugin.setAWSAccessKeyId("blah");
        plugin.setAWSSecretKey("blah");
        try {
            plugin.initialize(testContext());
            Assert.fail("Expected failure");
        } catch (IllegalArgumentException e) {
            Assert.assertTrue(e.getMessage().contains("path was not set"));
        }
    }

    @Test
    public void initializeInvalidPathNoExecID() {
        testPlugin plugin = new testPlugin();
        plugin.setBucket("basdf");
        plugin.setPath("blah/logs");
        plugin.setAWSAccessKeyId("blah");
        plugin.setAWSSecretKey("blah");
        try {
            plugin.initialize(testContext());
            Assert.fail("Expected failure");
        } catch (IllegalArgumentException e) {
            Assert.assertTrue(e.getMessage().contains("path must contain ${job.execid} or end with /"));
        }
    }

    @Test
    public void initializePathNoExecIDWithSlash() {
        testPlugin plugin = new testPlugin();
        plugin.setBucket("basdf");
        plugin.setPath("blah/logs/");
        plugin.setAWSAccessKeyId("blah");
        plugin.setAWSSecretKey("blah");
        plugin.initialize(testContext());
    }

    @Test
    public void initializePathWithExecIDEndsWithSlash() {
        testPlugin plugin = new testPlugin();
        plugin.setBucket("basdf");
        plugin.setPath("blah/${job.execid}/");
        plugin.setAWSAccessKeyId("blah");
        plugin.setAWSSecretKey("blah");
        try {
            plugin.initialize(testContext());
            Assert.fail("Expected failure");
        } catch (IllegalArgumentException e) {
            Assert.assertTrue(e.getMessage().contains("path must not end with /"));
        }
    }

    @Test
    public void initializePathWithExecIDValid() {
        testPlugin plugin = new testPlugin();
        plugin.setBucket("basdf");
        plugin.setPath("blah/${job.execid}.blah");
        plugin.setAWSAccessKeyId("blah");
        plugin.setAWSSecretKey("blah");
        plugin.initialize(testContext());
        Assert.assertEquals("blah/testexecid.blah", plugin.getExpandedPath());
    }

    @Test
    public void initializePathJobGroupName() {
        testPlugin plugin = new testPlugin();
        plugin.setBucket("basdf");
        plugin.setPath("blah/${job.group}/${job.name}/${job.execid}.blah");
        plugin.setAWSAccessKeyId("blah");
        plugin.setAWSSecretKey("blah");
        plugin.initialize(testContext3());
        Assert.assertEquals("blah/ajob group/another group/jobname/testexecid.blah", plugin.getExpandedPath());
    }

    @Test
    public void initializePathNoJobGroupName() {
        testPlugin plugin = new testPlugin();
        plugin.setBucket("basdf");
        plugin.setPath("blah/${job.group}/${job.name}/${job.execid}.blah");
        plugin.setAWSAccessKeyId("blah");
        plugin.setAWSSecretKey("blah");
        plugin.initialize(testContext2());
        Assert.assertEquals("blah/testexecid.blah", plugin.getExpandedPath());
    }

    @Test
    public void initializePathNoJobId_GroupName() {
        testPlugin plugin = new testPlugin();
        plugin.setBucket("basdf");
        plugin.setAWSAccessKeyId("blah");
        plugin.setAWSSecretKey("blah");
        plugin.setPath("blah/${job.project}/${job.id}/${job.group}/${job.name}/${job.execid}.blah");
        plugin.initialize(testContext());
        Assert.assertEquals("blah/testproject/testexecid.blah", plugin.getExpandedPath());
    }

    // ─── isAvailable tests ────────────────────────────────────────────────────

    @Test
    public void isAvailable404() throws ExecutionFileStorageException {
        testPlugin plugin = initializeTestPlugin();
        when(plugin.mockS3.headObject(any(HeadObjectRequest.class)))
                .thenThrow(S3Exception.builder().statusCode(404).message("Not Found").build());
        Assert.assertFalse(plugin.isAvailable(DEFAULT_FILETYPE));
    }

    @Test
    public void isAvailableOk() throws ExecutionFileStorageException {
        testPlugin plugin = initializeTestPlugin();
        when(plugin.mockS3.headObject(any(HeadObjectRequest.class)))
                .thenReturn(HeadObjectResponse.builder().build());
        Assert.assertTrue(plugin.isAvailable(DEFAULT_FILETYPE));
    }

    @Test
    public void isAvailableS3Exception() {
        testPlugin plugin = initializeTestPlugin();
        when(plugin.mockS3.headObject(any(HeadObjectRequest.class)))
                .thenThrow(S3Exception.builder().statusCode(500).message("blah").build());
        try {
            plugin.isAvailable(DEFAULT_FILETYPE);
            Assert.fail("Should throw");
        } catch (ExecutionFileStorageException e) {
            Assert.assertTrue(e.getMessage().contains("blah"));
        }
    }

    @Test
    public void isAvailableClientException() {
        testPlugin plugin = initializeTestPlugin();
        when(plugin.mockS3.headObject(any(HeadObjectRequest.class)))
                .thenThrow(SdkClientException.create("blah"));
        try {
            plugin.isAvailable(DEFAULT_FILETYPE);
            Assert.fail("Should throw exception");
        } catch (ExecutionFileStorageException e) {
            Assert.assertEquals("blah", e.getMessage());
        }
    }

    // ─── store tests ──────────────────────────────────────────────────────────

    @Test
    public void storeClientException() throws IOException {
        testPlugin plugin = initializeTestPlugin();
        when(plugin.mockS3.putObject(any(PutObjectRequest.class), any(RequestBody.class)))
                .thenThrow(SdkClientException.create("putObject"));
        try {
            plugin.store(DEFAULT_FILETYPE, new ByteArrayInputStream(new byte[0]), 0, null);
            Assert.fail("should throw");
        } catch (ExecutionFileStorageException e) {
            Assert.assertEquals("putObject", e.getMessage());
        }
    }

    @Test
    public void storeS3Exception() throws IOException {
        testPlugin plugin = initializeTestPlugin();
        when(plugin.mockS3.putObject(any(PutObjectRequest.class), any(RequestBody.class)))
                .thenThrow(S3Exception.builder().statusCode(403).message("putObject").build());
        try {
            plugin.store(DEFAULT_FILETYPE, new ByteArrayInputStream(new byte[0]), 0, null);
            Assert.fail("should throw");
        } catch (ExecutionFileStorageException e) {
            Assert.assertTrue(e.getMessage().contains("putObject"));
        }
    }

    @Test
    public void storeMetadata() throws IOException, ExecutionFileStorageException {
        testPlugin plugin = initializeTestPlugin();
        when(plugin.mockS3.putObject(any(PutObjectRequest.class), any(RequestBody.class)))
                .thenReturn(PutObjectResponse.builder().build());

        int length = 123;
        boolean result = plugin.store(DEFAULT_FILETYPE, new ByteArrayInputStream(new byte[0]), length, new Date());
        Assert.assertTrue(result);

        ArgumentCaptor<PutObjectRequest> captor = ArgumentCaptor.forClass(PutObjectRequest.class);
        verify(plugin.mockS3).putObject(captor.capture(), any(RequestBody.class));

        PutObjectRequest req = captor.getValue();
        Assert.assertEquals(length, (long) req.contentLength());
        Map<String, String> userMetadata = req.metadata();
        Assert.assertEquals(5, userMetadata.size());
        Assert.assertEquals(testContext().get("execid"), userMetadata.get("rundeck.execid"));
        Assert.assertEquals(testContext().get("project"), userMetadata.get("rundeck.project"));
        Assert.assertEquals(testContext().get("url"), userMetadata.get("rundeck.url"));
        Assert.assertEquals(testContext().get("serverUrl"), userMetadata.get("rundeck.serverUrl"));
        Assert.assertEquals(testContext().get("serverUUID"), userMetadata.get("rundeck.serverUUID"));
    }

    @Test
    public void storeEncodedMetadata() throws IOException, ExecutionFileStorageException {
        testPlugin plugin = new testPlugin();
        plugin.setAWSAccessKeyId("blah");
        plugin.setAWSSecretKey("blah");
        plugin.setBucket("testBucket");
        plugin.setEncodeUserMetadata(true);
        plugin.initialize(testContext());

        when(plugin.mockS3.putObject(any(PutObjectRequest.class), any(RequestBody.class)))
                .thenReturn(PutObjectResponse.builder().build());

        int length = 123;
        boolean result = plugin.store(DEFAULT_FILETYPE, new ByteArrayInputStream(new byte[0]), length, new Date());
        Assert.assertTrue(result);

        ArgumentCaptor<PutObjectRequest> captor = ArgumentCaptor.forClass(PutObjectRequest.class);
        verify(plugin.mockS3).putObject(captor.capture(), any(RequestBody.class));

        Map<String, String> userMetadata = captor.getValue().metadata();
        Assert.assertEquals(5, userMetadata.size());
        Assert.assertEquals(URLEncoder.encode((String) testContext().get("execid"), StandardCharsets.UTF_8.toString()), userMetadata.get("rundeck.execid"));
        Assert.assertEquals(URLEncoder.encode((String) testContext().get("project"), StandardCharsets.UTF_8.toString()), userMetadata.get("rundeck.project"));
        Assert.assertEquals(URLEncoder.encode((String) testContext().get("url"), StandardCharsets.UTF_8.toString()), userMetadata.get("rundeck.url"));
        Assert.assertEquals(URLEncoder.encode((String) testContext().get("serverUrl"), StandardCharsets.UTF_8.toString()), userMetadata.get("rundeck.serverUrl"));
        Assert.assertEquals(URLEncoder.encode((String) testContext().get("serverUUID"), StandardCharsets.UTF_8.toString()), userMetadata.get("rundeck.serverUUID"));
    }

    // ─── retrieve tests ───────────────────────────────────────────────────────

    class testOutputStream extends OutputStream {
        boolean wasWrite = false;
        boolean writeIOException = false;

        @Override
        public void write(int i) throws IOException {
            wasWrite = true;
            if (writeIOException) {
                throw new IOException("testOutputStream.writeIOException");
            }
        }

        boolean wasClosed;

        @Override
        public void close() throws IOException {
            wasClosed = true;
        }
    }

    class testInputStream extends InputStream {
        boolean wasRead = false;
        int bytes = 1;
        boolean readIOException = false;

        @Override
        public int read() throws IOException {
            wasRead = true;
            if (readIOException) {
                throw new IOException("testInputStream.readIOException");
            }
            --bytes;
            return bytes < 0 ? -1 : bytes;
        }

        boolean wasClosed = false;

        @Override
        public void close() throws IOException {
            wasClosed = true;
            super.close();
        }
    }

    @SuppressWarnings("unchecked")
    private ResponseInputStream<GetObjectResponse> makeResponseStream(InputStream body) {
        return new ResponseInputStream<>(
                GetObjectResponse.builder().build(),
                AbortableInputStream.create(body)
        );
    }

    @Test
    public void retrieve() throws IOException, ExecutionFileStorageException {
        testPlugin plugin = initializeTestPlugin();
        testOutputStream stream = new testOutputStream();
        testInputStream testInputStream = new testInputStream();

        ResponseInputStream<GetObjectResponse> mockStream = makeResponseStream(testInputStream);
        doReturn(mockStream).when(plugin.mockS3).getObject(any(GetObjectRequest.class), any(ResponseTransformer.class));

        boolean result = plugin.retrieve(DEFAULT_FILETYPE, stream);
        Assert.assertTrue(result);

        ArgumentCaptor<GetObjectRequest> captor = ArgumentCaptor.forClass(GetObjectRequest.class);
        verify(plugin.mockS3).getObject(captor.capture(), any(ResponseTransformer.class));
        Assert.assertEquals("testBucket", captor.getValue().bucket());
        Assert.assertEquals("project/testproject/testexecid.rdlog", captor.getValue().key());

        Assert.assertTrue(testInputStream.wasRead);
        Assert.assertTrue(testInputStream.bytes < 0);
        Assert.assertTrue(testInputStream.wasClosed);
        Assert.assertTrue(stream.wasWrite);
        Assert.assertFalse(stream.wasClosed);
    }

    @Test
    public void retrieveClientException() throws IOException {
        testPlugin plugin = initializeTestPlugin();
        testOutputStream stream = new testOutputStream();

        doThrow(SdkClientException.create("getObject"))
                .when(plugin.mockS3).getObject(any(GetObjectRequest.class), any(ResponseTransformer.class));

        boolean result = false;
        try {
            result = plugin.retrieve(DEFAULT_FILETYPE, stream);
            Assert.fail("should throw");
        } catch (ExecutionFileStorageException e) {
            Assert.assertEquals("getObject", e.getMessage());
        }
        Assert.assertFalse(result);

        ArgumentCaptor<GetObjectRequest> captor = ArgumentCaptor.forClass(GetObjectRequest.class);
        verify(plugin.mockS3).getObject(captor.capture(), any(ResponseTransformer.class));
        Assert.assertEquals("testBucket", captor.getValue().bucket());
        Assert.assertEquals("project/testproject/testexecid.rdlog", captor.getValue().key());

        Assert.assertFalse(stream.wasWrite);
        Assert.assertFalse(stream.wasClosed);
    }

    @Test
    public void retrieveS3Exception() throws IOException {
        testPlugin plugin = initializeTestPlugin();
        testOutputStream stream = new testOutputStream();

        doThrow(S3Exception.builder().statusCode(403).message("getObject").build())
                .when(plugin.mockS3).getObject(any(GetObjectRequest.class), any(ResponseTransformer.class));

        boolean result = false;
        try {
            result = plugin.retrieve(DEFAULT_FILETYPE, stream);
            Assert.fail("should throw");
        } catch (ExecutionFileStorageException e) {
            Assert.assertTrue(e.getMessage().contains("getObject"));
        }
        Assert.assertFalse(result);

        ArgumentCaptor<GetObjectRequest> captor = ArgumentCaptor.forClass(GetObjectRequest.class);
        verify(plugin.mockS3).getObject(captor.capture(), any(ResponseTransformer.class));
        Assert.assertEquals("testBucket", captor.getValue().bucket());
        Assert.assertEquals("project/testproject/testexecid.rdlog", captor.getValue().key());

        Assert.assertFalse(stream.wasWrite);
        Assert.assertFalse(stream.wasClosed);
    }

    @Test
    public void retrieveInputIOException() throws IOException, ExecutionFileStorageException {
        testPlugin plugin = initializeTestPlugin();
        testOutputStream stream = new testOutputStream();
        testInputStream tis = new testInputStream();
        tis.readIOException = true;

        ResponseInputStream<GetObjectResponse> mockStream = makeResponseStream(tis);
        doReturn(mockStream).when(plugin.mockS3).getObject(any(GetObjectRequest.class), any(ResponseTransformer.class));

        boolean result = false;
        try {
            result = plugin.retrieve(DEFAULT_FILETYPE, stream);
            Assert.fail("should throw exception");
        } catch (IOException e) {
            Assert.assertEquals("testInputStream.readIOException", e.getMessage());
        }
        Assert.assertFalse(result);
        Assert.assertTrue(tis.wasRead);
        Assert.assertFalse(tis.bytes < 0);
        Assert.assertTrue(tis.wasClosed);
        Assert.assertFalse(stream.wasWrite);
        Assert.assertFalse(stream.wasClosed);
    }

    @Test
    public void retrieveOutputIOException() throws IOException, ExecutionFileStorageException {
        testPlugin plugin = initializeTestPlugin();
        testOutputStream stream = new testOutputStream();
        stream.writeIOException = true;
        testInputStream tis = new testInputStream();

        ResponseInputStream<GetObjectResponse> mockStream = makeResponseStream(tis);
        doReturn(mockStream).when(plugin.mockS3).getObject(any(GetObjectRequest.class), any(ResponseTransformer.class));

        boolean result = false;
        try {
            result = plugin.retrieve(DEFAULT_FILETYPE, stream);
            Assert.fail("should throw exception");
        } catch (IOException e) {
            Assert.assertEquals("testOutputStream.writeIOException", e.getMessage());
        }
        Assert.assertFalse(result);
        Assert.assertTrue(tis.wasRead);
        Assert.assertTrue(tis.wasClosed);
        Assert.assertTrue(stream.wasWrite);
        Assert.assertFalse(stream.wasClosed);
    }

    // ─── helpers ──────────────────────────────────────────────────────────────

    private testPlugin initializeTestPlugin() {
        testPlugin plugin = new testPlugin();
        plugin.setAWSAccessKeyId("blah");
        plugin.setAWSSecretKey("blah");
        plugin.setBucket("testBucket");
        plugin.initialize(testContext());
        return plugin;
    }
}
