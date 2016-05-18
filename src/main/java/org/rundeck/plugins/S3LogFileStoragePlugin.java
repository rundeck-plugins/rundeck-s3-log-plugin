package org.rundeck.plugins;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.RegionUtils;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.*;
import com.amazonaws.SDKGlobalConfiguration;
import com.dtolabs.rundeck.core.logging.ExecutionFileStorageException;
import com.dtolabs.rundeck.core.plugins.Plugin;
import com.dtolabs.rundeck.plugins.ServiceNameConstants;
import com.dtolabs.rundeck.plugins.descriptions.PluginDescription;
import com.dtolabs.rundeck.plugins.descriptions.PluginProperty;
import com.dtolabs.rundeck.plugins.logging.ExecutionFileStoragePlugin;
import com.dtolabs.utils.Streams;

import java.io.*;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;


/**
 * {@link ExecutionFileStoragePlugin} that stores files to Amazon S3.
 */
@Plugin(service = ServiceNameConstants.ExecutionFileStorage, name = "org.rundeck.amazon-s3")
@PluginDescription(title = "S3", description = "Stores log files into an S3 bucket")
public class S3LogFileStoragePlugin implements ExecutionFileStoragePlugin, AWSCredentials {

    public static final String DEFAULT_PATH_FORMAT = "project/${job.project}/${job.execid}";
    public static final String DEFAULT_REGION = "us-east-1";

    Logger logger = Logger.getLogger(S3LogFileStoragePlugin.class.getName());

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

    @PluginProperty(
            title = "Path",
            required = true,
            description = "The path in the bucket to store a log file. You can use these " +
                    "expansion variables: (${job.execid} = execution ID, ${job.project} = project name, " +
                    "${job.id} = job UUID (or blank)." +
                    " Default: "
                    + DEFAULT_PATH_FORMAT,
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
            description = "S3 endpoint to connect to.",
            required = false,
            defaultValue = "")
    private String endpoint;

    private String expandedPath;

    public S3LogFileStoragePlugin() {
    }

    private AmazonS3 amazonS3;

    private Map<String, ? extends Object> context;

    public void initialize(Map<String, ? extends Object> context) {
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

        Region awsregion = RegionUtils.getRegion(getRegion());
        if (null == awsregion) {
            throw new IllegalArgumentException("Region was not found: " + getRegion());
        }

        if (null == getEndpoint() || "".equals(getEndpoint().trim())) {
            amazonS3.setRegion(awsregion);
        } else {
            System.setProperty(SDKGlobalConfiguration.ENFORCE_S3_SIGV4_SYSTEM_PROPERTY, "true");
            amazonS3.setEndpoint(getEndpoint());
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
     * @param awsCredentials
     *
     * @return
     */
    protected AmazonS3 createAmazonS3Client(AWSCredentials awsCredentials) {
        return new AmazonS3Client(awsCredentials);
    }
    /**
     * can override for testing
     *
     *
     * @return
     */
    protected AmazonS3 createAmazonS3Client() {
        return new AmazonS3Client();
    }

    /**
     * Expands the path format using the context data
     *
     * @param pathFormat
     * @param context
     *
     * @return
     */
    static String expandPath(String pathFormat, Map<String, ? extends Object> context) {
        String result = pathFormat.replaceAll("^/+", "");
        if (null != context) {
            result = result.replaceAll("\\$\\{job.execid\\}", notNull(context, "execid", ""));
            result = result.replaceAll("\\$\\{job.id\\}", notNull(context, "id", ""));
            result = result.replaceAll("\\$\\{job.project\\}", notNull(context, "project", ""));
        }
        result = result.replaceAll("/+", "/");

        return result;
    }

    private static String notNull(Map<String, ?> context1, String execid1, String defaultValue) {
        Object value = context1.get(execid1);
        return value != null ? value.toString() : defaultValue;
    }

    public boolean isAvailable(final String filetype) throws ExecutionFileStorageException {
        GetObjectMetadataRequest getObjectRequest = new GetObjectMetadataRequest(getBucket(), resolvedFilepath(expandedPath, filetype));
        logger.log(Level.FINE, "getState for S3 bucket {0}:{1}", new Object[]{getBucket(), resolvedFilepath(expandedPath, filetype)});
        try {
            ObjectMetadata objectMetadata = amazonS3.getObjectMetadata(getObjectRequest);
            Map<String, String> userMetadata = objectMetadata.getUserMetadata();
            String metaId = null;
            if (null != userMetadata) {
                metaId = userMetadata.get("rundeck.execid");
            }
            logger.log(Level.FINE, "Metadata {0}", new Object[]{objectMetadata.getUserMetadata()});
            //execution ID is stored in the user metadata for the object
            //compare to the context execution ID we are expecting
            boolean matchesId = context.get("execid").equals(metaId);
            if (!matchesId) {
                logger.log(Level.WARNING, "S3 Object metadata 'rundeck.execid' was not expected: {0}, expected {1}",
                        new Object[]{metaId, context.get("execid")});
            }
            return true;
        } catch (AmazonS3Exception e) {
            if (e.getStatusCode() == 404) {
                //not found
                logger.log(Level.FINE, "getState: S3 Object not found for {0}", resolvedFilepath(expandedPath, filetype));
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

    public boolean store(final String filetype, InputStream stream, long length, Date lastModified) throws ExecutionFileStorageException {
        boolean success = false;
        logger.log(Level.FINE, "Storing content to S3 bucket {0} path {1}", new Object[]{getBucket(),
                resolvedFilepath(expandedPath, filetype)});
        ObjectMetadata objectMetadata = createObjectMetadata(length, lastModified);
        PutObjectRequest putObjectRequest = new PutObjectRequest(getBucket(), resolvedFilepath(expandedPath, filetype), stream, objectMetadata);
        try {
            amazonS3.putObject(putObjectRequest);
            success = true;
        } catch (AmazonClientException e) {
            logger.log(Level.SEVERE, e.getMessage(), e);
            throw new ExecutionFileStorageException(e.getMessage(), e);
        }
        return success;
    }

    /**
     * Metadata keys from the Execution context that will be stored as User Metadata in the S3 Object
     */
    private static final String[] STORED_META = new String[]{"execid", "username", "project", "url", "serverUrl",
            "serverUUID"};

    private ObjectMetadata createObjectMetadata(long length, Date lastModified) {
        ObjectMetadata metadata = new ObjectMetadata();
        for (String s : STORED_META) {
            Object v = context.get(s);
            if (null != v) {
                metadata.addUserMetadata("rundeck." + s, v.toString());
            }
        }
        metadata.setLastModified(lastModified);
        metadata.setContentLength(length);
        return metadata;
    }


    public boolean retrieve(final String filetype, OutputStream stream) throws IOException, ExecutionFileStorageException {
        S3Object object = null;
        boolean success = false;
        try {
            object = amazonS3.getObject(getBucket(), resolvedFilepath(expandedPath, filetype));
            S3ObjectInputStream objectContent = object.getObjectContent();
            try {
                Streams.copyStream(objectContent, stream);
                success = true;
            } finally {
                objectContent.close();
            }

        } catch (AmazonClientException e) {
            logger.log(Level.SEVERE, e.getMessage(), e);
            throw new ExecutionFileStorageException(e.getMessage(), e);
        }

        return success;
    }

    public static void main(String[] args) throws IOException, ExecutionFileStorageException {
        S3LogFileStoragePlugin s3LogFileStoragePlugin = new S3LogFileStoragePlugin();
        String action = args[0];
        Map<String, Object> context = new HashMap<String, Object>();
        context.put("project", "test");
        context.put("execid", args[2]);
        context.put("name", "test job");
        context.put("group", "test group");
        context.put("id", "testjobid");
        context.put("username", "testuser");

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

    private String resolvedFilepath(final String path, final String filetype) {
        return path + "." + filetype;
    }
}