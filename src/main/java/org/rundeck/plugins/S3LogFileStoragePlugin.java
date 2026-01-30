package org.rundeck.plugins;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.core.exception.SdkClientException;
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

        // Validate credentials configuration
        if ((null != getAWSAccessKeyId() && null == getAWSSecretKey()) ||
                (null == getAWSAccessKeyId() && null != getAWSSecretKey())) {
            throw new IllegalArgumentException("AWSAccessKeyId and AWSSecretKey must both be configured.");
        }

        // Determine credentials provider and create S3 client
        if (null != AWSAccessKeyId && null != AWSSecretKey) {
            // Use static credentials
            AwsCredentialsProvider credentialsProvider = StaticCredentialsProvider.create(
                    AwsBasicCredentials.create(AWSAccessKeyId, AWSSecretKey));
            s3Client = createS3Client(credentialsProvider);
        } else if (null != getAWSCredentialsFile()) {
            // Use credentials file - need to read and create provider
            File creds = new File(getAWSCredentialsFile());
            if (!creds.exists() || !creds.canRead()) {
                throw new IllegalArgumentException("Credentials file does not exist or cannot be read: " +
                        getAWSCredentialsFile());
            }
            try {
                // Read credentials from properties file
                java.util.Properties props = new java.util.Properties();
                try (FileInputStream fis = new FileInputStream(creds)) {
                    props.load(fis);
                }
                String accessKey = props.getProperty("accessKey");
                String secretKey = props.getProperty("secretKey");
                if (accessKey == null || secretKey == null) {
                    throw new IllegalArgumentException("Credentials file must contain 'accessKey' and 'secretKey' properties");
                }
                AwsCredentialsProvider credentialsProvider = StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(accessKey, secretKey));
                s3Client = createS3Client(credentialsProvider);
            } catch (IOException e) {
                throw new RuntimeException("Credentials file could not be read: " + getAWSCredentialsFile() + ": " + e
                        .getMessage(), e);
            }
        } else {
            // Use default credentials provider chain (IAM roles, environment variables, etc.)
            s3Client = createS3Client();
        }

        // Validate bucket and path configuration
        if (null == bucket || "".equals(bucket.trim())) {
            throw new IllegalArgumentException("bucket was not set");
        }
        if (null == getPath() || "".equals(getPath().trim())) {
            throw new IllegalArgumentException("path was not set");
        }
        if (!getPath().contains("${job.execid}") && !getPath().endsWith("/")) {
            throw new IllegalArgumentException("path must contain ${job.execid} or end with /");
        }
        String configpath = getPath();
        if (!configpath.contains("${job.execid}") && configpath.endsWith("/")) {
            configpath = path + "/${job.execid}";
        }
        expandedPath = (context.get("isRemoteFilePath") != null && context.get("isRemoteFilePath").equals("true"))
                ? String.valueOf(context.get("outputfilepath").toString())
                : expandPath(configpath, context);
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
     * @param awsCredentials credentials
     *
     * @return S3Client
     */
    protected S3Client createS3Client(AwsCredentialsProvider awsCredentials) {
        return buildS3Client(awsCredentials);
    }

    /**
     * can override for testing
     *
     * @return S3Client
     */
    protected S3Client createS3Client() {
        return buildS3Client(DefaultCredentialsProvider.create());
    }

    /**
     * Builds S3Client with all configuration
     */
    private S3Client buildS3Client(AwsCredentialsProvider credentialsProvider) {
        // Validate and parse region
        Region awsRegion;
        try {
            awsRegion = Region.of(getRegion());
        } catch (Exception e) {
            throw new IllegalArgumentException("Region was not found: " + getRegion(), e);
        }

        // Log warning if signature v2 or v4 enforcement is requested
        if (isSignatureV2Used()) {
            logger.warn("Signature V2 is not supported in AWS SDK v2. Using Signature V4 instead.");
        }
        if (isSignatureV4Enforced()) {
            logger.debug("Signature V4 is enforced by default in AWS SDK v2");
        }

        // Build S3 client with all configuration
        S3ClientBuilder clientBuilder = S3Client.builder()
                .credentialsProvider(credentialsProvider);

        // Configure S3-specific options (path style access)
        if (isPathStyle()) {
            clientBuilder.serviceConfiguration(
                    S3Configuration.builder()
                            .pathStyleAccessEnabled(true)
                            .build());
        }

        // Set region or endpoint
        if (null == getEndpoint() || "".equals(getEndpoint().trim())) {
            clientBuilder.region(awsRegion);
        } else {
            // When endpoint is specified, still need to set a region (can be any valid region)
            clientBuilder.region(awsRegion);
            clientBuilder.endpointOverride(java.net.URI.create(getEndpoint()));
        }

        return clientBuilder.build();
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
        logger.debug("getState for S3 bucket {}:{}", getBucket(), key);
        try {
            HeadObjectResponse headObjectResponse = s3Client.headObject(headObjectRequest);
            Map<String, String> userMetadata = headObjectResponse != null ? headObjectResponse.metadata() : null;

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
        } catch (S3Exception e) {
            if (e.statusCode() == 404) {
                //not found
                logger.debug("getState: S3 Object not found for {}", key);
            } else {
                logger.error("Failed get metadata", e);
                throw new ExecutionFileStorageException(e.getMessage(), e);
            }
        } catch (SdkClientException e) {
            logger.error("AWS client error", e);
            throw new ExecutionFileStorageException(e.getMessage(), e);
        }

        return false;
    }

    public boolean store(final String filetype, InputStream stream, long length, Date lastModified)
            throws ExecutionFileStorageException
    {
        return storePath(
                stream,
                resolvedFilepath(expandedPath, filetype),
                length,
                createObjectMetadata()
        );
    }


    protected boolean storePath(
            final InputStream stream,
            final String key,
            final long contentLength,
            final Map<String, String> userMetadata
    )
            throws ExecutionFileStorageException
    {
        logger.debug("Storing content to S3 bucket {} path {}", getBucket(), key);

        PutObjectRequest.Builder requestBuilder = PutObjectRequest.builder()
                .bucket(getBucket())
                .key(key)
                .contentLength(contentLength);

        if (userMetadata != null && !userMetadata.isEmpty()) {
            requestBuilder.metadata(userMetadata);
        }

        PutObjectRequest putObjectRequest = requestBuilder.build();

        try {
            s3Client.putObject(putObjectRequest, RequestBody.fromInputStream(stream, contentLength));
            return true;
        } catch (SdkClientException e) {
            logger.debug("Job could still be executing: {}", e.getMessage());
            throw new ExecutionFileStorageException(e.getMessage(), e);
        } catch (S3Exception e) {
            logger.error("AWS S3 error on store attempt", e);
            throw new ExecutionFileStorageException(e.getMessage(), e);
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
        try {
            HashMap<String, Object> expected = new HashMap<>();
            expected.put(metaKey(META_EXECID), context.get(META_ID_FOR_LOGSTORE));
            String filePath = resolvedFilepath(expandedPath, filetype);

            DeleteObjectRequest deleteObjectRequest = DeleteObjectRequest.builder()
                    .bucket(getBucket())
                    .key(filePath)
                    .build();

            s3Client.deleteObject(deleteObjectRequest);
            return true;
        } catch (S3Exception e) {
            logger.error("AWS S3 error on delete", e);
            throw new ExecutionFileStorageException(e.getMessage(), e);
        } catch (SdkClientException e) {
            logger.error("AWS client error on delete", e);
            throw new ExecutionFileStorageException(e.getMessage(), e);
        }
    }



    /**
     * Metadata keys from the Execution context that will be stored as User Metadata in the S3 Object
     */
    private static final String[] STORED_META = new String[]{META_EXECID, META_USERNAME,
            META_PROJECT, META_URL, META_SERVERURL,
            META_SERVER_UUID};

    protected Map<String, String> createObjectMetadata() {
        Map<String, String> metadata = new HashMap<>();
        for (String s : STORED_META) {
            Object v = context.get(s);
            if (null != v) {
                metadata.put(metaKey(s), isEncodeUserMetadata() ? encodeStringToURLRequest(v.toString()) : v.toString());
            }
        }
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
        GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                .bucket(getBucket())
                .key(key)
                .build();

        try {
            ResponseInputStream<GetObjectResponse> responseInputStream =
                    s3Client.getObject(getObjectRequest, ResponseTransformer.toInputStream());
            try (responseInputStream) {
                Streams.copyStream(responseInputStream, stream);
            }
        } catch (S3Exception e) {
            logger.error("AWS S3 error on get object", e);
            throw new ExecutionFileStorageException(e.getMessage(), e);
        } catch (SdkClientException e) {
            logger.error("AWS client error on get object", e);
            throw new ExecutionFileStorageException(e.getMessage(), e);
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
