package org.rundeck.plugins;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.RegionUtils;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.S3ClientOptions;
import com.amazonaws.services.s3.model.*;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.SDKGlobalConfiguration;
import com.dtolabs.rundeck.core.dispatcher.DataContextUtils;
import com.dtolabs.rundeck.core.logging.*;
import com.dtolabs.rundeck.core.plugins.Plugin;
import com.dtolabs.rundeck.plugins.ServiceNameConstants;
import com.dtolabs.rundeck.plugins.descriptions.PluginDescription;
import com.dtolabs.rundeck.plugins.descriptions.PluginProperty;
import com.dtolabs.rundeck.plugins.logging.ExecutionFileStoragePlugin;
import com.dtolabs.utils.Streams;

import java.io.*;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;


/**
 * {@link ExecutionFileStoragePlugin} that stores files to Amazon S3.
 */
@Plugin(service = ServiceNameConstants.ExecutionFileStorage, name = "org.rundeck.amazon-s3")
@PluginDescription(title = "S3", description = "Stores log files into an S3 bucket")
public class S3LogFileStoragePlugin implements ExecutionFileStoragePlugin, AWSCredentials, ExecutionMultiFileStorage {
    public static final String DEFAULT_PATH_FORMAT = "project/${job.project}/${job.execid}";
    public static final String DEFAULT_REGION = "us-east-1";
    public static final String META_EXECID = "execid";
    public static final String _PREFIX_META = "rundeck.";

    public static final String META_USERNAME = "username";
    public static final String META_PROJECT = "project";
    public static final String META_URL = "url";
    public static final String META_SERVERURL = "serverUrl";
    public static final String META_SERVER_UUID = "serverUUID";

    protected static Logger logger = Logger.getLogger(S3LogFileStoragePlugin.class.getName());

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

    protected AmazonS3 amazonS3;

    protected Map<String, ?> context;

    public void initialize(Map<String, ?> context) {
        this.context = context;
        if ((null != getAWSAccessKeyId() && null == getAWSSecretKey()) ||
                (null == getAWSAccessKeyId() && null != getAWSSecretKey())) {
            throw new IllegalArgumentException("AWSAccessKeyId and AWSSecretKey must both be configured.");
        }
        if (null != AWSAccessKeyId && null != AWSSecretKey) {
            amazonS3 = createAmazonS3Client(this);
        } else if (null != getAWSCredentialsFile()) {
            File creds = new File(getAWSCredentialsFile());
            if (!creds.exists() || !creds.canRead()) {
                throw new IllegalArgumentException("Credentials file does not exist or cannot be read: " +
                        getAWSCredentialsFile());
            }
            try {
                amazonS3 = createAmazonS3Client(new PropertiesCredentials(creds));
            } catch (IOException e) {
                throw new RuntimeException("Credentials file could not be read: " + getAWSCredentialsFile() + ": " + e
                        .getMessage(), e);
            }
        } else {
            //use credentials provider chain
            amazonS3 = createAmazonS3Client();
        }

        Region awsregion = RegionUtils.getRegions().stream()
            .filter(r -> r.getName().equals(getRegion()))
            .findAny()
            .orElse(null);

        if (null == awsregion) {
            throw new IllegalArgumentException("Region was not found: " + getRegion());
        }

        if (isSignatureV4Enforced()) {
            System.setProperty(SDKGlobalConfiguration.ENFORCE_S3_SIGV4_SYSTEM_PROPERTY, "true");
        }

        if (null == getEndpoint() || "".equals(getEndpoint().trim())) {
            amazonS3.setRegion(awsregion);
        } else {
            amazonS3.setEndpoint(getEndpoint());
        }
        if(isPathStyle()) {
            S3ClientOptions clientOptions = new S3ClientOptions();
            clientOptions.setPathStyleAccess(isPathStyle());
            amazonS3.setS3ClientOptions(clientOptions);
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
        expandedPath = expandPath(configpath, context);
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
     * @return amazons3
     */
    protected AmazonS3 createAmazonS3Client(AWSCredentials awsCredentials) {
        if (isSignatureV2Used()) {
            ClientConfiguration opts = new ClientConfiguration();
            opts.setSignerOverride("S3SignerType");
            return new AmazonS3Client(awsCredentials, opts);
        } else {
            return new AmazonS3Client(awsCredentials);
        }

    }
    /**
     * can override for testing
     *
     * @return amazons3
     */
    protected AmazonS3 createAmazonS3Client() {
        if (isSignatureV2Used()) {
            ClientConfiguration opts = new ClientConfiguration();
            opts.setSignerOverride("S3SignerType");
            return new AmazonS3Client(opts);
        } else {
            return new AmazonS3Client();
        }

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
        expected.put(metaKey(META_EXECID), context.get(META_EXECID));
        return isPathAvailable(resolvedFilepath(expandedPath, filetype), expected);
    }

    protected boolean isPathAvailable(final String key, Map<String, Object> expectedMeta)
            throws ExecutionFileStorageException
    {
        GetObjectMetadataRequest getObjectRequest = new GetObjectMetadataRequest(getBucket(), key);
        logger.log(Level.FINE, "getState for S3 bucket {0}:{1}", new Object[]{getBucket(),
                key});
        try {
            ObjectMetadata objectMetadata = amazonS3.getObjectMetadata(getObjectRequest);
            Map<String, String> userMetadata = objectMetadata != null ? objectMetadata.getUserMetadata() : null;

            logger.log(Level.FINE, "Metadata {0}", new Object[]{userMetadata});
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
                        logger.log(Level.WARNING, "S3 Object metadata '{0}' was not expected: {1}, expected {2}",
                                   new Object[]{s, metaVal, expectedMeta.get(s)}
                        );
                    }
                }
            }
            return true;
        } catch (AmazonS3Exception e) {
            if (e.getStatusCode() == 404) {
                //not found
                logger.log(Level.FINE, "getState: S3 Object not found for {0}", key);
            } else {
                logger.log(Level.SEVERE, e.getMessage());
                logger.log(Level.FINE, e.getMessage(), e);
                throw new ExecutionFileStorageException(e.getMessage(), e);
            }
        } catch (AmazonClientException e) {
            logger.log(Level.SEVERE, e.getMessage());
            logger.log(Level.FINE, e.getMessage(), e);
            throw new ExecutionFileStorageException(e.getMessage(), e);
        }

        return false;
    }

    public boolean store(final String filetype, InputStream stream, long length, Date lastModified)
            throws ExecutionFileStorageException
    {
        boolean result = storePath(
                stream,
                resolvedFilepath(expandedPath, filetype),
                createObjectMetadata(length, lastModified)
        );

        return result;
    }


    protected boolean storePath(
            final InputStream stream,
            final String key,
            final ObjectMetadata objectMetadata1
    )
            throws ExecutionFileStorageException
    {
        logger.log(Level.FINE, "Storing content to S3 bucket {0} path {1}", new Object[]{getBucket(), key});
        ObjectMetadata objectMetadata = objectMetadata1;
        PutObjectRequest putObjectRequest = new PutObjectRequest(
                getBucket(),
                key,
                stream,
                objectMetadata
        );
        try {
            amazonS3.putObject(putObjectRequest);
            return true;
        } catch (AmazonClientException e) {
            logger.log(Level.SEVERE, e.getMessage(), e);
            throw new ExecutionFileStorageException(e.getMessage(), e);
        }
    }

    @Override
    public void storeMultiple(final MultiFileStorageRequest files) throws IOException, ExecutionFileStorageException {
        Set<String> availableFiletypes = files.getAvailableFiletypes();
        logger.log(
                Level.FINE,
                "Storing multiple files to S3 bucket {0} filetypes: {1}",
                new Object[]{getBucket(), availableFiletypes}
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
                    logger.log(Level.SEVERE, e.getMessage());
                    logger.log(Level.FINE, e.getMessage(), e);
                    files.storageResultForFiletype(filetype, false);
                }
            }
        }
    }

    public boolean deleteFile(String filetype) throws IOException, ExecutionFileStorageException {
        try{

            HashMap<String, Object> expected = new HashMap<>();
            expected.put(metaKey(META_EXECID), context.get(META_EXECID));
            String filePath = resolvedFilepath(expandedPath, filetype);

            amazonS3.deleteObject(getBucket(), filePath);
            return true;
        } catch (AmazonClientException e) {
            logger.log(Level.SEVERE, e.getMessage(), e);
            throw new ExecutionFileStorageException(e.getMessage(), e);
        }
    }



    /**
     * Metadata keys from the Execution context that will be stored as User Metadata in the S3 Object
     */
    private static final String[] STORED_META = new String[]{META_EXECID, META_USERNAME,
            META_PROJECT, META_URL, META_SERVERURL,
            META_SERVER_UUID};

    protected ObjectMetadata createObjectMetadata(long length, Date lastModified) {
        ObjectMetadata metadata = new ObjectMetadata();
        for (String s : STORED_META) {
            Object v = context.get(s);
            if (null != v) {
                metadata.addUserMetadata(metaKey(s), isEncodeUserMetadata() ? encodeStringToURLRequest(v.toString()) : v.toString());
            }
        }
        metadata.setLastModified(lastModified);
        metadata.setContentLength(length);
        return metadata;
    }

    protected String metaKey(final String s) {
        return _PREFIX_META + s;
    }

    private String encodeStringToURLRequest(String value){
        try {
            return URLEncoder.encode(value, StandardCharsets.UTF_8.toString());
        } catch (UnsupportedEncodingException e) {
            logger.log(Level.WARNING, e.getMessage(), e);
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
        S3Object object;
        try {
            object = amazonS3.getObject(getBucket(), key);
            try (S3ObjectInputStream objectContent = object.getObjectContent()) {
                Streams.copyStream(objectContent, stream);
            }

        } catch (AmazonClientException e) {
            logger.log(Level.SEVERE, e.getMessage(), e);
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

    public AmazonS3 getAmazonS3() {
        return amazonS3;
    }

    public boolean isEncodeUserMetadata() {
        return encodeUserMetadata;
    }

    public void setEncodeUserMetadata(boolean encodeUserMetadata) {
        this.encodeUserMetadata = encodeUserMetadata;
    }
}
