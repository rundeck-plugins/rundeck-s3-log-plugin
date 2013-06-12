package org.rundeck.plugins;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.RegionUtils;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.*;
import com.dtolabs.rundeck.core.logging.LogFileState;
import com.dtolabs.rundeck.core.plugins.Plugin;
import com.dtolabs.rundeck.plugins.ServiceNameConstants;
import com.dtolabs.rundeck.plugins.descriptions.PluginDescription;
import com.dtolabs.rundeck.plugins.descriptions.PluginProperty;
import com.dtolabs.rundeck.plugins.logging.LogFileStoragePlugin;
import com.dtolabs.utils.Streams;

import java.io.*;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;


/**
 * {@link LogFileStoragePlugin} that stores files to Amazon S3.
 */
@Plugin(service = ServiceNameConstants.LogFileStorage, name = "org.rundeck.amazon-s3")
@PluginDescription(title = "S3", description = "Stores log files into an S3 bucket")
public class S3LogFileStoragePlugin implements LogFileStoragePlugin, AWSCredentials {

    public static final String DEFAULT_PATH_FORMAT = "rundeck/project/$PROJECT/logs/$ID";

    Logger logger = Logger.getLogger(S3LogFileStoragePlugin.class.getName());

    @PluginProperty(title = "AWS Access Key", required = true, description = "AWS Access Key")
    private String AWSAccessKeyId;

    @PluginProperty(title = "AWS Secret Key", required = true, description = "AWS Secret Key")
    private String AWSSecretKey;

    @PluginProperty(title = "Bucket name", required = true, description = "Bucket to store files in")
    private String bucket;

    @PluginProperty(
            title = "Path",
            required = true,
            description = "The path in the bucket to store a log file. You can use these " +
                    "expansion variables: ($ID = execution ID, $PROJECT = project name, $JOB = job name, $GROUP = " +
                    "group name, $JOBID = job ID, $RUN = blank if it is a job, otherwise the word 'run' ). Default: "
                    + DEFAULT_PATH_FORMAT,
            defaultValue = DEFAULT_PATH_FORMAT)
    private String path;

    @PluginProperty(
            title = "S3 Region",
            description = "AWS S3 Region to use.  You can use one of the supported region names",
            required = true,
            defaultValue = "us-east-1")
    private String region;

    private String expandedPath;

    public S3LogFileStoragePlugin() {
    }

    private AmazonS3 amazonS3;

    private Map<String, ? extends Object> context;

    public void initialize(Map<String, ? extends Object> context) {
        this.context = context;
        expandedPath = expandPath(getPath(), context);
        amazonS3 = new AmazonS3Client(this);
        Region awsregion = RegionUtils.getRegion(getRegion());
        if (null == awsregion) {
            throw new IllegalStateException("Region was not found: " + getRegion());
        }

        amazonS3.setRegion(awsregion);

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
            result = result.replaceAll("\\$ID", notNull(context, "execid", ""));
            result = result.replaceAll("\\$JOBID", notNull(context, "id", ""));
            result = result.replaceAll("\\$JOB", notNull(context, "name", ""));
            result = result.replaceAll("\\$GROUP", notNull(context, "group", ""));
            result = result.replaceAll("\\$PROJECT", notNull(context, "project", ""));
            result = result.replaceAll("\\$USER", notNull(context, "username", ""));
            result = result.replaceAll("\\$RUN", null != context.get("id") ? "" : "run");
        }
        result = result.replaceAll("/+", "/");

        return result;
    }

    private static String notNull(Map<String, ?> context1, String execid1, String defaultValue) {
        Object value = context1.get(execid1);
        return value != null ? value.toString() : defaultValue;
    }

    public LogFileState getState() {
        LogFileState state = LogFileState.NOT_FOUND;

        GetObjectMetadataRequest getObjectRequest = new GetObjectMetadataRequest(getBucket(), expandedPath);
        logger.log(Level.FINE, "getState for S3 bucket {0}:{1}", new Object[]{getBucket(), expandedPath});
        try {
            ObjectMetadata objectMetadata = amazonS3.getObjectMetadata(getObjectRequest);
            String metaId = objectMetadata.getUserMetadata().get("rundeck.execid");
            logger.log(Level.FINE, "Metadata {0}", new Object[]{objectMetadata.getUserMetadata()});
            boolean matchesId = context.get("execid").equals(metaId);
            if (!matchesId) {
                logger.log(Level.WARNING, "S3 Object metadata 'rundeck.execid' was not expected: {0}, expected {1}",
                        new Object[]{metaId, context.get("execid")});
            }
            return LogFileState.AVAILABLE;
        } catch (AmazonS3Exception e) {
            if (e.getStatusCode() == 404) {
                //not found
                logger.log(Level.FINE, "getState: S3 Object not found for {0}", expandedPath);
            } else {
                logger.log(Level.SEVERE, e.getMessage());
                logger.log(Level.FINE, e.getMessage(), e);
            }
        } catch (AmazonClientException e) {
            logger.log(Level.SEVERE, e.getMessage());
            logger.log(Level.FINE, e.getMessage(), e);
        }

        return state;
    }

    public boolean store(InputStream stream, long length, Date lastModified) throws IOException {
        boolean success = false;
        try {
            logger.log(Level.FINE, "Storing content to S3 bucket {0} path {1}", new Object[]{getBucket(),
                    expandedPath});
            ObjectMetadata objectMetadata = createObjectMetadata(length, lastModified);
            PutObjectRequest putObjectRequest = new PutObjectRequest(getBucket(), expandedPath, stream, objectMetadata);
            PutObjectResult putObjectResult = amazonS3.putObject(putObjectRequest);
            success = true;
        } catch (AmazonClientException e) {
            logger.log(Level.SEVERE, e.getMessage(), e);
        }
        return success;
    }

    private static final String[] STORED_META = new String[]{"execid", "username", "project"};

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


    public boolean retrieve(OutputStream stream) throws IOException {
        S3Object object = null;
        boolean success = false;
        try {
            object = amazonS3.getObject(getBucket(), expandedPath);
            S3ObjectInputStream objectContent = object.getObjectContent();
            try {
                Streams.copyStream(objectContent, stream);
                success = true;
            } finally {
                objectContent.close();
            }
        } catch (AmazonClientException e) {
            logger.log(Level.SEVERE, e.getMessage(), e);
        }

        return success;
    }

    public static void main(String[] args) throws IOException {
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

        s3LogFileStoragePlugin.initialize(context);

        if ("store".equals(action)) {
            File file = new File(args[3]);
            s3LogFileStoragePlugin.store(new FileInputStream(file), file.length(), new Date(file.lastModified()));
        } else if ("retrieve".equals(action)) {
            s3LogFileStoragePlugin.retrieve(new FileOutputStream(new File(args[3])));
        } else if ("state".equals(action)) {
            System.out.println(s3LogFileStoragePlugin.getState());
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
}
