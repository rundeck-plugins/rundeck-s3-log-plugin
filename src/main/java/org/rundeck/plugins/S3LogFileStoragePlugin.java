package org.rundeck.plugins;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.s3.S3Configuration;
import java.net.URI;
import com.dtolabs.rundeck.core.dispatcher.DataContextUtils;
import com.dtolabs.rundeck.core.logging.*;
import com.dtolabs.rundeck.core.plugins.Plugin;
import com.dtolabs.rundeck.plugins.ServiceNameConstants;
import com.dtolabs.rundeck.plugins.descriptions.PluginDescription;
import com.dtolabs.rundeck.plugins.descriptions.PluginProperty;
import com.dtolabs.rundeck.plugins.logging.ExecutionFileStoragePlugin;
import com.dtolabs.utils.Streams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;


/**
 * {@link ExecutionFileStoragePlugin} that stores files to Amazon S3.
 */
@Plugin(service = ServiceNameConstants.ExecutionFileStorage, name = "org.rundeck.amazon-s3")
@PluginDescription(title = "S3", description = "Stores log files into an S3 bucket")
public class S3LogFileStoragePlugin implements ExecutionFileStoragePlugin, ExecutionMultiFileStorage {
    public static final String DEFAULT_PATH_FORMAT = "project/${job.project}/${job.execid}";
    public static final String DEFAULT_REGION = "us-east-1";
    public static final String META_EXECID = "execid";

    public static final String META_ID_FOR_LOGSTORE = "execIdForLogStore";

    public static final String _PREFIX_META = "rundeck.";

    public static final String META_USERNAME = "username";
    public static final String META_PROJECT = "project";
    public static final String META_URL = "url";
    public static final String META_SERVERURL = "serverUrl";
    public static final String META_SERVER_UUID = "serverUUID";

    protected static Logger logger = LoggerFactory.getLogger(S3LogFileStoragePlugin.class.getName());

    @PluginProperty(title = "AWS Access Key", description = "AWS Access Key")
    private String AWSAccessKeyId;

    @PluginProperty(title = "AWS Secret Key", description = "AWS Secret Key")
    private String AWSSecretKey;

    @PluginProperty(title = "AWS Credentials File", description = "Path to a AWSCredentials.properties file " +
            "containing " +
            "'accessKey' and 'secretKey'.")
    private String AWSCredentialsFile;

    @PluginProperty(title = "Bucket name", required = true, description = "Bucket to store files in")
    private String bucket;

    @PluginProperty(title = "Encode user metadata", required = false, description = "Encode user metadata to URL format", defaultValue = "false")
    private boolean encodeUserMetadata = false;

    @PluginProperty(
            title = "Path",
            required = true,
            description = "The path in the bucket to store a log file. " +
                          " Default: "
                          + DEFAULT_PATH_FORMAT +
                          "\n\nYou can use these expansion variables: \n\n" +
                          "* `${job.execid}` = execution ID\n" +
                          "* `${job.project}` = project name\n" +
                          "* `${job.id}` = job UUID (or blank).\n" +
                          "* `${job.group}` = job group (or blank).\n" +
                          "* `${job.name}` = job name (or blank).\n" +
                          "",
            defaultValue = DEFAULT_PATH_FORMAT)
    private String path;

    @PluginProperty(
            title = "S3 Region",
            description = "AWS S3 Region to use.  You can use one of the supported region names",
            required = true,
            defaultValue = DEFAULT_REGION)
    private String region;

    @PluginProperty(
            title = "S3 Endpoint",
            description = "S3 endpoint to connect to, the region is ignored if this is set."
    )
    private String endpoint;

    @PluginProperty(
            title = "Force Signature v4",
            description = "Whether to force use of Signature Version 4 authentication. Default: false",
            defaultValue = "false")
    private boolean forceSigV4;

    @PluginProperty(
            title = "Use Signature v2",
            description = "Use of Signature Version 2 authentication for old container. Default: false",
            defaultValue = "false")
    private boolean useSigV2;

    @PluginProperty(
            title = "Use Path Style",
            description = "Whether to access the Endpoint using `endpoint/bucket` style, default: false. The default will " +
                          "use DNS style `bucket.endpoint`, which may be incompatible with non-AWS S3-compatible services",
            defaultValue = "false")
    private boolean pathStyle;

    protected String expandedPath;

    public S3LogFileStoragePlugin() {
        super();
    }

    protected S3Client s3Client;

    protected Map<String, ?> context;

    public void initialize(Map<String, ?> context) {
        this.context = context;
        if ((null != getAWSAccessKeyId() && null == getAWSSecretKey()) ||
                (null == getAWSAccessKeyId() && null != getAWSSecretKey())) {
            throw new IllegalArgumentException("AWSAccessKeyId and AWSSecretKey must both be configured.");
        }
        if (null != AWSAccessKeyId && null != AWSSecretKey) {
            AwsCredentials credentials = AwsBasicCredentials.create(getAWSAccessKeyId(), getAWSSecretKey());
            s3Client = createS3Client(StaticCredentialsProvider.create(credentials));
        } else if (null != getAWSCredentialsFile()) {
            File creds = new File(getAWSCredentialsFile());
            if (!creds.exists() || !creds.canRead()) {
                throw new IllegalArgumentException("Credentials file does not exist or cannot be read: " +
                        getAWSCredentialsFile());
            }
            try {
                // Note: AWS SDK v2 doesn't have PropertiesCredentials, we'll need to parse manually
                Properties props = new Properties();
                props.load(new FileInputStream(creds));
                String accessKey = props.getProperty("accessKey");
                String secretKey = props.getProperty("secretKey");
                if (accessKey == null || secretKey == null) {
                    throw new RuntimeException("Credentials file must contain 'accessKey' and 'secretKey' properties: " + getAWSCredentialsFile());
                }
                AwsCredentials credentials = AwsBasicCredentials.create(accessKey, secretKey);
                s3Client = createS3Client(StaticCredentialsProvider.create(credentials));
            } catch (IOException e) {
                throw new RuntimeException("Credentials file could not be read: " + getAWSCredentialsFile() + ": " + e
                        .getMessage(), e);
            }
        } else {
            //use credentials provider chain
            s3Client = createS3Client(DefaultCredentialsProvider.create());
        }

        // Log configuration for troubleshooting S3-compatible services
        if (null != getEndpoint() && !"".equals(getEndpoint().trim())) {
            logger.info("S3 plugin configured for custom endpoint: '{}', bucket: '{}', path style: enabled", getEndpoint(), getBucket());
        } else {
            logger.info("S3 plugin configured for AWS S3, region: '{}', bucket: '{}', path style: {}", getRegion(), getBucket(), isPathStyle());
        }

        // Skip region validation for custom endpoints (S3-compatible services)
        if (null == getEndpoint() || "".equals(getEndpoint().trim())) {
            Region awsRegion = Region.of(getRegion());
            if (awsRegion == null || awsRegion == Region.AWS_GLOBAL) {
                throw new IllegalArgumentException("Region was not found or is invalid: " + getRegion());
            }
        }

        if (null == bucket || "".equals(bucket.trim())) {
            throw new IllegalArgumentException("bucket was not set");
        }
        if (null == getPath() || "".equals(getPath().trim())) {
            throw new IllegalArgumentException("path was not set");
        }
        if (!getPath().contains("${job.execid}") && !getPath().endsWith("/")) {
            throw new IllegalArgumentException("path must contain ${job.execid} or end with /");
        }
        String configpath= getPath();
        if (!configpath.contains("${job.execid}") && configpath.endsWith("/")) {
            configpath = path + "/${job.execid}";
        }
        expandedPath = (context.get("isRemoteFilePath") != null && context.get("isRemoteFilePath").equals("true")) ? String.valueOf(context.get("outputfilepath").toString()) : expandPath(configpath, context);
        if (null == expandedPath || "".equals(expandedPath.trim())) {
            throw new IllegalArgumentException("expanded value of path was empty");
        }
        if (expandedPath.endsWith("/")) {
            throw new IllegalArgumentException("expanded value of path must not end with /");
        }

    }

    /**
     * can override for testing
     *
     * @param credentialsProvider credentials provider
     *
     * @return S3Client
     */
    protected S3Client createS3Client(AwsCredentialsProvider credentialsProvider) {
        S3ClientBuilder builder = S3Client.builder()
                .credentialsProvider(credentialsProvider);

        // Configure endpoint and region
        if (null != getEndpoint() && !"".equals(getEndpoint().trim())) {
            builder.endpointOverride(URI.create(getEndpoint()));
            // For custom S3-compatible endpoints, use path style by default and minimal region validation
            builder.serviceConfiguration(S3Configuration.builder()
                    .pathStyleAccessEnabled(true)
                    .checksumValidationEnabled(false)  // Some S3-compatible services have checksum issues
                    .build());
            // Use a dummy region for custom endpoints to avoid region validation issues
            builder.region(Region.US_EAST_1);
        } else {
            // For real AWS S3, use the specified region
            builder.region(Region.of(getRegion()));
            // Configure path style access only if explicitly requested for AWS
            if (isPathStyle()) {
                builder.serviceConfiguration(S3Configuration.builder()
                        .pathStyleAccessEnabled(true)
                        .build());
            }
        }

        return builder.build();
    }
    
    /**
     * can override for testing
     *
     * @return S3Client
     */
    protected S3Client createS3Client() {
        return createS3Client(DefaultCredentialsProvider.create());
    }

    /**
     * Expands the path format using the context data
     *
     * @param pathFormat format
     * @param context context data
     *
     * @return expanded path
     */
    static String expandPath(String pathFormat, Map<String, ?> context) {
        String result = pathFormat.replaceAll("^/+", "");
        if (null != context) {
            result = DataContextUtils.replaceDataReferences(
                    result,
                    DataContextUtils.addContext("job", stringMap(context), new HashMap<>()),
                    null,
                    false,
                    true
            );
        }
        result = result.replaceAll("/+", "/");

        return result;
    }

    private static Map<String, String> stringMap(final Map<String, ?> context) {
        HashMap<String, String> result = new HashMap<>();
        for (String s : context.keySet()) {
            Object o = context.get(s);
            if (o != null) {
                result.put(s, o.toString());
            }
        }
        return result;
    }

    public boolean isAvailable(final String filetype) throws ExecutionFileStorageException {
        HashMap<String, Object> expected = new HashMap<>();
        expected.put(metaKey(META_EXECID), context.get(META_ID_FOR_LOGSTORE));
        return isPathAvailable(resolvedFilepath(expandedPath, filetype), expected);
    }

    @Override
    public String getConfiguredPathTemplate() {
        return this.path;
    }

    protected boolean isPathAvailable(final String key, Map<String, Object> expectedMeta)
            throws ExecutionFileStorageException
    {
        HeadObjectRequest headObjectRequest = HeadObjectRequest.builder()
                .bucket(getBucket())
                .key(key)
                .build();
        logger.debug("getState for S3 bucket {}:{}", getBucket(),key);
        try {
            HeadObjectResponse response = s3Client.headObject(headObjectRequest);
            Map<String, String> userMetadata = response.metadata();

            logger.debug("Metadata {}", userMetadata);
            //execution ID is stored in the user metadata for the object
            //compare to the context execution ID we are expecting
            if (null != expectedMeta) {
                for (String s : expectedMeta.keySet()) {
                    String metaVal = null;
                    if (null != userMetadata) {
                        metaVal = userMetadata.get(s);
                    }
                    boolean matches = expectedMeta.get(s).equals(metaVal);
                    if (!matches) {
                        logger.warn("S3 Object metadata '{0}' was not expected: {1}, expected {2}",
                                   new Object[]{s, metaVal, expectedMeta.get(s)}
                        );
                    }
                }
            }
            return true;
        } catch (NoSuchKeyException e) {
            //not found
            logger.debug("getState: S3 Object not found for {}", key);
        } catch (S3Exception e) {
            if (e.statusCode() == 404) {
                //not found
                logger.debug("getState: S3 Object not found for {}", key);
            } else {
                String endpoint = (getEndpoint() != null && !getEndpoint().trim().isEmpty()) ? getEndpoint() : "default AWS endpoint for region " + getRegion();
                logger.error("S3 error getting metadata for bucket '{}', endpoint '{}', key '{}': {} (Status: {}, Request ID: {})", 
                        getBucket(), endpoint, key, e.getMessage(), e.statusCode(), e.requestId());
                throw new ExecutionFileStorageException("Failed to get metadata from S3 bucket '" + getBucket() + 
                        "' at endpoint '" + endpoint + "': " + e.getMessage(), e);
            }
        } catch (SdkClientException e) {
            String endpoint = (getEndpoint() != null && !getEndpoint().trim().isEmpty()) ? getEndpoint() : "default AWS endpoint for region " + getRegion();
            logger.error("AWS client error getting metadata for bucket '{}', endpoint '{}', key '{}': {}", 
                    getBucket(), endpoint, key, e.getMessage());
            throw new ExecutionFileStorageException("AWS client error getting metadata from bucket '" + getBucket() + 
                    "' at endpoint '" + endpoint + "': " + e.getMessage(), e);
        }

        return false;
    }

    public boolean store(final String filetype, InputStream stream, long length, Date lastModified)
            throws ExecutionFileStorageException
    {
        return storePath(
                stream,
                resolvedFilepath(expandedPath, filetype),
                createObjectMetadata(length, lastModified),
                length
        );

    }

    protected boolean storePath(
            final InputStream stream,
            final String key,
            final Map<String, String> metadata,
            final long contentLength
    )
            throws ExecutionFileStorageException
    {
        logger.debug("Storing content to S3 bucket '{}' path '{}' (length: {})", getBucket(), key, contentLength);
        String endpoint = (getEndpoint() != null && !getEndpoint().trim().isEmpty()) ? getEndpoint() : "default AWS endpoint for region " + getRegion();
        logger.debug("Using endpoint: '{}', path style: {}, metadata keys: {}", endpoint, isPathStyle(), metadata.keySet());
        
        PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                .bucket(getBucket())
                .key(key)
                .metadata(metadata)
                .build();
        try {
            s3Client.putObject(putObjectRequest, RequestBody.fromInputStream(stream, contentLength));
            logger.debug("Successfully stored object to S3: bucket='{}', key='{}'", getBucket(), key);
            return true;
        } catch (S3Exception e) {
            logger.error("S3 error storing to bucket '{}', endpoint '{}', key '{}': {} (Status: {}, Request ID: {})", 
                    getBucket(), endpoint, key, e.getMessage(), e.statusCode(), e.requestId());
            throw new ExecutionFileStorageException("Failed to store to S3 bucket '" + getBucket() + 
                    "' at endpoint '" + endpoint + "': " + e.getMessage(), e);
        } catch (SdkClientException e){
            logger.error("AWS client error storing to bucket '{}', endpoint '{}', key '{}': {}", 
                    getBucket(), endpoint, key, e.getMessage());
            throw new ExecutionFileStorageException("AWS client error storing to bucket '" + getBucket() + 
                    "' at endpoint '" + endpoint + "': " + e.getMessage(), e);
        }
    }

    @Override
    public void storeMultiple(final MultiFileStorageRequest files) throws IOException, ExecutionFileStorageException {
        Set<String> availableFiletypes = files.getAvailableFiletypes();
        logger.debug("Storing multiple files to S3 bucket {} filetypes: {}",
                getBucket(), availableFiletypes
        );
        for (String filetype : availableFiletypes) {
            StorageFile storageFile = files.getStorageFile(filetype);
            boolean success;
            try {
                success = store(
                        filetype,
                        storageFile.getInputStream(),
                        storageFile.getLength(),
                        storageFile.getLastModified()
                );
                files.storageResultForFiletype(filetype, success);
            } catch (ExecutionFileStorageException e) {
                if (files instanceof MultiFileStorageRequestErrors) {
                    MultiFileStorageRequestErrors errors = (MultiFileStorageRequestErrors) files;
                    errors.storageFailureForFiletype(filetype, e.getMessage());
                } else {
                    logger.error(e.getMessage());
                    logger.debug(e.getMessage(), e);
                    files.storageResultForFiletype(filetype, false);
                }
            }
        }
    }

    public boolean deleteFile(String filetype) throws IOException, ExecutionFileStorageException {
        HashMap<String, Object> expected = new HashMap<>();
        expected.put(metaKey(META_EXECID), context.get(META_ID_FOR_LOGSTORE));
        String filePath = resolvedFilepath(expandedPath, filetype);
        
        try{
            DeleteObjectRequest deleteRequest = DeleteObjectRequest.builder()
                    .bucket(getBucket())
                    .key(filePath)
                    .build();
            s3Client.deleteObject(deleteRequest);
            return true;
        } catch (S3Exception e) {
            String endpoint = (getEndpoint() != null && !getEndpoint().trim().isEmpty()) ? getEndpoint() : "default AWS endpoint for region " + getRegion();
            logger.error("S3 error deleting object from bucket '{}', endpoint '{}', key '{}': {} (Status: {}, Request ID: {})", 
                    getBucket(), endpoint, filePath, e.getMessage(), e.statusCode(), e.requestId());
            throw new ExecutionFileStorageException("Failed to delete object from S3 bucket '" + getBucket() + 
                    "' at endpoint '" + endpoint + "': " + e.getMessage(), e);
        } catch (SdkClientException e) {
            String endpoint = (getEndpoint() != null && !getEndpoint().trim().isEmpty()) ? getEndpoint() : "default AWS endpoint for region " + getRegion();
            logger.error("AWS client error deleting object from bucket '{}', endpoint '{}', key '{}': {}", 
                    getBucket(), endpoint, filePath, e.getMessage());
            throw new ExecutionFileStorageException("AWS client error deleting object from bucket '" + getBucket() + 
                    "' at endpoint '" + endpoint + "': " + e.getMessage(), e);
        }
    }


    /**
     * Metadata keys from the Execution context that will be stored as User Metadata in the S3 Object
     */
    private static final String[] STORED_META = new String[]{META_EXECID, META_USERNAME,
            META_PROJECT, META_URL, META_SERVERURL,
            META_SERVER_UUID};

    protected Map<String, String> createObjectMetadata(long length, Date lastModified) {
        Map<String, String> metadata = new HashMap<>();
        for (String s : STORED_META) {
            Object v = context.get(s);
            if (null != v) {
                metadata.put(metaKey(s), isEncodeUserMetadata() ? encodeStringToURLRequest(v.toString()) : v.toString());
            }
        }
        // Note: In AWS SDK v2, content-length and last-modified are not stored in user metadata
        // They are handled automatically by the SDK
        return metadata;
    }

    protected String metaKey(final String s) {
        return _PREFIX_META + s;
    }

    private String encodeStringToURLRequest(String value){
        try {
            return URLEncoder.encode(value, StandardCharsets.UTF_8.toString());
        } catch (UnsupportedEncodingException e) {
            logger.warn(e.getMessage(), e);
        }

        return value;
    }


    public boolean retrieve(final String filetype, OutputStream stream)
            throws IOException, ExecutionFileStorageException
    {
        return retrievePath(stream, resolvedFilepath(expandedPath, filetype));
    }


    protected boolean retrievePath(final OutputStream stream, final String key)
            throws IOException, ExecutionFileStorageException
    {
        try {
            GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                    .bucket(getBucket())
                    .key(key)
                    .build();
            
            try (InputStream objectContent = s3Client.getObject(getObjectRequest)) {
                Streams.copyStream(objectContent, stream);
            }

        } catch (S3Exception e) {
            String endpoint = (getEndpoint() != null && !getEndpoint().trim().isEmpty()) ? getEndpoint() : "default AWS endpoint for region " + getRegion();
            logger.error("S3 error retrieving object from bucket '{}', endpoint '{}', key '{}': {} (Status: {}, Request ID: {})", 
                    getBucket(), endpoint, key, e.getMessage(), e.statusCode(), e.requestId());
            throw new ExecutionFileStorageException("Failed to retrieve object from S3 bucket '" + getBucket() + 
                    "' at endpoint '" + endpoint + "': " + e.getMessage(), e);
        } catch (SdkClientException e) {
            String endpoint = (getEndpoint() != null && !getEndpoint().trim().isEmpty()) ? getEndpoint() : "default AWS endpoint for region " + getRegion();
            logger.error("AWS client error retrieving object from bucket '{}', endpoint '{}', key '{}': {}", 
                    getBucket(), endpoint, key, e.getMessage());
            throw new ExecutionFileStorageException("AWS client error retrieving object from bucket '" + getBucket() + 
                    "' at endpoint '" + endpoint + "': " + e.getMessage(), e);
        }

        return true;
    }

    public static void main(String[] args) throws IOException, ExecutionFileStorageException {
        S3LogFileStoragePlugin s3LogFileStoragePlugin = new S3LogFileStoragePlugin();
        String action = args[0];
        Map<String, Object> context = new HashMap<>();
        context.put(META_PROJECT, "test");
        context.put(META_EXECID, args[2]);
        context.put("name", "test job");
        context.put("group", "test group");
        context.put("id", "testjobid");
        context.put(META_USERNAME, "testuser");

        s3LogFileStoragePlugin.setAWSAccessKeyId("AKIAJ63ESPDAOS5FKWNQ");
        s3LogFileStoragePlugin.setAWSSecretKey(args[1]);
        s3LogFileStoragePlugin.setBucket("test-rundeck-logs");
        s3LogFileStoragePlugin.setPath("logs/$PROJECT/$ID.log");
        s3LogFileStoragePlugin.setRegion("us-east-1");
        s3LogFileStoragePlugin.setEndpoint("https://localhost");

        s3LogFileStoragePlugin.initialize(context);

        String filetype = ".rdlog";

        if ("store".equals(action)) {
            File file = new File(args[3]);
            s3LogFileStoragePlugin.store(filetype, new FileInputStream(file), file.length(), new Date(file.lastModified()));
        } else if ("retrieve".equals(action)) {
            s3LogFileStoragePlugin.retrieve(filetype, new FileOutputStream(new File(args[3])));
        } else if ("state".equals(action)) {
            System.out.println("available? " + s3LogFileStoragePlugin.isAvailable(filetype));
        }
    }

    public String getAWSAccessKeyId() {
        return AWSAccessKeyId;
    }

    public void setAWSAccessKeyId(String AWSAccessKeyId) {
        this.AWSAccessKeyId = AWSAccessKeyId;
    }

    public String getAWSSecretKey() {
        return AWSSecretKey;
    }

    public void setAWSSecretKey(String AWSSecretKey) {
        this.AWSSecretKey = AWSSecretKey;
    }

    public String getBucket() {
        return bucket;
    }

    public void setBucket(String bucket) {
        this.bucket = bucket;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public String getRegion() {
        return region;
    }

    public void setRegion(String region) {
        this.region = region;
    }

    public String getAWSCredentialsFile() {
        return AWSCredentialsFile;
    }

    public void setAWSCredentialsFile(String AWSCredentialsFile) {
        this.AWSCredentialsFile = AWSCredentialsFile;
    }

    public String getEndpoint() { return endpoint; }

    public void setEndpoint(String endpoint) {
        this.endpoint = endpoint;
    }

    public boolean isSignatureV4Enforced() { return forceSigV4; }

    public void setForceSignatureV4(boolean forceSigV4) {
        this.forceSigV4 = forceSigV4;
    }

    public boolean isSignatureV2Used() { return useSigV2; }

    public void setUseSignatureV2(boolean useSigV2) {
        this.useSigV2 = useSigV2;
    }

    protected String resolvedFilepath(final String path, final String filetype) {
        return path + "." + filetype;
    }

    public boolean isPathStyle() {
        return pathStyle;
    }

    public void setPathStyle(boolean pathStyle) {
        this.pathStyle = pathStyle;
    }

    public String getExpandedPath() {
        return expandedPath;
    }

    public S3Client getS3Client() {
        return s3Client;
    }

    public boolean isEncodeUserMetadata() {
        return encodeUserMetadata;
    }

    public void setEncodeUserMetadata(boolean encodeUserMetadata) {
        this.encodeUserMetadata = encodeUserMetadata;
    }
}
