package org.rundeck.plugins;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.RegionUtils;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.*;
import com.dtolabs.rundeck.core.logging.ExecutionFileStorageException;
import com.dtolabs.rundeck.core.logging.LogFileState;
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
 * {@link com.dtolabs.rundeck.plugins.logging.ExecutionFileStoragePlugin} that stores files to Amazon S3.
 */
@Plugin(service = ServiceNameConstants.LogFileStorage, name = "org.rundeck.amazon-s3")
@PluginDescription(title = "S3", description = "Stores log files into an S3 bucket")
public class S3LogFileStoragePlugin implements ExecutionFileStoragePlugin, AWSCredentials {

    private static final String DEFAULT_FILE_SPEC = "${job.execid}.${filetype}";
    public static final String DEFAULT_PATH_FORMAT = "project/${job.project}/"+DEFAULT_FILE_SPEC;
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
                    "${job.id} = job UUID (or blank), ${filetype} = type of file." +
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

        amazonS3.setRegion(awsregion);
        if (null == bucket || "".equals(bucket.trim())) {
            throw new IllegalArgumentException("bucket was not set");
        }
        if (null == getPath() || "".equals(getPath().trim())) {
            throw new IllegalArgumentException("path was not set");
        }
        if (!getPath().contains("${job.execid}") && !getPath().endsWith("/")) {
            throw new IllegalArgumentException("path must contain ${job.execid} or end with /");
        }
        String expandedPath=expandPath(context, null);
        if (null == expandedPath || "".equals(expandedPath.trim())) {
            throw new IllegalArgumentException("expanded value of path was empty");
        }
        if (expandedPath.endsWith("/")) {
            throw new IllegalArgumentException("expanded value of path must not end with /");
        }

    }

    /**
     * Expand the configured path template given the context and filetype, and using a default filename spec if only a directory path is specified
     * @param context
     * @param filetype
     * @return
     */
    private String expandPath(Map<String, ? extends Object> context, String filetype) {
        String configpath= getPath();
        if (!configpath.contains("${job.execid}") && configpath.endsWith("/")) {
            configpath = getPath() + DEFAULT_FILE_SPEC;
        }
        return expandPath(configpath, context, filetype);
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
     * @param filetype
     * @return
     */
    static String expandPath(String pathFormat, Map<String, ? extends Object> context, String filetype) {
        String result = pathFormat.replaceAll("^/+", "");
        if (null != context) {
            result = result.replaceAll("\\$\\{job.execid\\}", notNull(context, "execid", ""));
            result = result.replaceAll("\\$\\{job.id\\}", notNull(context, "id", ""));
            result = result.replaceAll("\\$\\{job.project\\}", notNull(context, "project", ""));
        }
        if (null != filetype) {
            result = result.replaceAll("\\$\\{filetype\\}", filetype);
        }
        result = result.replaceAll("/+", "/");

        return result;
    }

    private static String notNull(Map<String, ?> context1, String execid1, String defaultValue) {
        Object value = context1.get(execid1);
        return notNull(defaultValue, value);
    }

    private static String notNull(String defaultValue, Object value) {
        return value != null ? value.toString() : defaultValue;
    }

    public boolean isAvailable(String filetype) throws ExecutionFileStorageException {
        LogFileState state = LogFileState.NOT_FOUND;
        String fileTypepath = expandPath(path, context, filetype);
        GetObjectMetadataRequest getObjectRequest = new GetObjectMetadataRequest(getBucket(), fileTypepath);
        logger.log(Level.FINE, "getState for S3 bucket {0}:{1}", new Object[]{getBucket(), fileTypepath});
        try {
            ObjectMetadata objectMetadata = amazonS3.getObjectMetadata(getObjectRequest);
            Map<String, String> userMetadata = objectMetadata.getUserMetadata();
            String metaId = null;
            String metafiletype = null;
            if (null != userMetadata) {
                metaId = userMetadata.get("rundeck.execid");
                metafiletype = userMetadata.get("rundeck.filetype");
            }
            logger.log(Level.FINE, "Metadata {0}", new Object[]{objectMetadata.getUserMetadata()});
            //execution ID is stored in the user metadata for the object
            //compare to the context execution ID we are expecting
            boolean matchesId = context.get("execid").equals(metaId);
            if (!matchesId) {
                logger.log(Level.WARNING, "S3 Object metadata 'rundeck.execid' was not expected: {0}, expected {1}",
                        new Object[]{metaId, context.get("execid")});
            }
            if(null!=metafiletype && null!=filetype && !filetype.equals(metafiletype)){
                logger.log(Level.WARNING, "S3 Object metadata 'rundeck.filetype' was not expected: {0}, expected {1}",
                        new Object[]{metafiletype, filetype});
            }
            return true;
        } catch (AmazonS3Exception e) {
            if (e.getStatusCode() == 404) {
                //not found
                logger.log(Level.FINE, "getState: S3 Object not found for {0}", fileTypepath);
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

    public boolean store(String filetype, InputStream stream, long length, Date lastModified) throws ExecutionFileStorageException {
        boolean success = false;
        String fileTypepath = expandPath(path, context, filetype);
        logger.log(Level.FINE, "Storing content to S3 bucket {0} path {1}", new Object[]{getBucket(),
                fileTypepath});
        ObjectMetadata objectMetadata = createObjectMetadata(length, lastModified, filetype);
        PutObjectRequest putObjectRequest = new PutObjectRequest(getBucket(), fileTypepath, stream, objectMetadata);
        try {
            PutObjectResult putObjectResult = amazonS3.putObject(putObjectRequest);
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

    private ObjectMetadata createObjectMetadata(long length, Date lastModified, String filetype) {
        ObjectMetadata metadata = new ObjectMetadata();
        for (String s : STORED_META) {
            Object v = context.get(s);
            if (null != v) {
                metadata.addUserMetadata("rundeck." + s, v.toString());
            }
        }
        if(null!=filetype){
            metadata.addUserMetadata("rundeck.filetype", filetype);
        }
        metadata.setLastModified(lastModified);
        metadata.setContentLength(length);
        return metadata;
    }


    public boolean retrieve(String filetype, OutputStream stream) throws IOException, ExecutionFileStorageException {
        S3Object object = null;
        String fileTypepath = expandPath(path, context, filetype);
        boolean success = false;
        try {
            object = amazonS3.getObject(getBucket(), fileTypepath);
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
        context.put("filetype", args[3]);

        s3LogFileStoragePlugin.setAWSAccessKeyId("AKIAJ63ESPDAOS5FKWNQ");
        s3LogFileStoragePlugin.setAWSSecretKey(args[1]);
        s3LogFileStoragePlugin.setBucket("test-rundeck-logs");
        s3LogFileStoragePlugin.setPath("logs/$PROJECT/$ID.log");
        s3LogFileStoragePlugin.setRegion("us-east-1");

        s3LogFileStoragePlugin.initialize(context);

        if ("store".equals(action)) {
            File file = new File(args[3]);
            s3LogFileStoragePlugin.store(args[3],new FileInputStream(file), file.length(), new Date(file.lastModified()));
        } else if ("retrieve".equals(action)) {
            s3LogFileStoragePlugin.retrieve(args[3],new FileOutputStream(new File(args[3])));
        } else if ("state".equals(action)) {
            System.out.println("available? " + s3LogFileStoragePlugin.isAvailable(args[3]));
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
}
