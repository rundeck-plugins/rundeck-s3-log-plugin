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
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;


/**
 *
 */
@Plugin(service = ServiceNameConstants.LogFileStorage, name = "org.rundeck.amazon-s3")
@PluginDescription(title = "S3", description = "Stores log files into an S3 bucket")
public class S3LogFileStoragePlugin implements LogFileStoragePlugin {
    Logger logger = Logger.getLogger(S3LogFileStoragePlugin.class.getName());

    @PluginProperty(title = "AWS Access Key", required = true, description = "AWS Access Key")
    private String awsAccessKey;

    @PluginProperty(title = "AWS Secret Key", required = true, description = "AWS Secret Key")
    private String awsSecretKey;

    @PluginProperty(title = "S3 Bucket name", required = true, description = "S3 Bucket to store files in")
    private String bucket;

    @PluginProperty(title = "Path Format", description = "A string describing the path in the bucket to " +
            "store a log file. You can use these" +
            " expansion variables: ($ID = execution ID, $PROJECT = project name, $JOB = job name, " +
            "$GROUP = group name, " +
            "$JID = job ID). Default: logs/$ID", defaultValue = "logs/$ID")
    private String pathFormat;

    @PluginProperty(title = "S3 Region", description = "AWS S3 Region to use.  You can use one of the supported " +
            "region names", required = true, defaultValue = "us-east-1")
    private String region;

    private String expandedPathFormat;

    public S3LogFileStoragePlugin() {
    }

    private AmazonS3 amazonS3;

    private Map<String, ? extends Object> context;

    public void initialize(Map<String, ? extends Object> context) {
        this.context = context;
        expandedPathFormat = expandFormat(getPathFormat(), context);

        amazonS3 = new AmazonS3Client(new AWSCredentials() {
            public String getAWSAccessKeyId() {
                return getAwsAccessKey();
            }

            public String getAWSSecretKey() {
                return getAwsSecretKey();
            }
        });
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
    private String expandFormat(String pathFormat, Map<String, ? extends Object> context) {
        String result = pathFormat.replaceAll("^/+", "");
        result = result.replaceAll("\\$ID", context.get("execid").toString());
        Object id = context.get("id");
        Object jobName = context.get("name");
        Object jobGroup = context.get("group");
        result = result.replaceAll("\\$JID", null != id ? id.toString() : "");
        result = result.replaceAll("\\$JOB", null != jobName ? jobName.toString() : "");
        result = result.replaceAll("\\$GROUP", null != jobGroup ? jobGroup.toString() : "");
        result = result.replaceAll("\\$PROJECT", context.get("project").toString());
        result = result.replaceAll("\\$USER", context.get("username").toString());

        return result;
    }

    public LogFileState getState() {
        LogFileState state = LogFileState.NOT_FOUND;

        GetObjectMetadataRequest getObjectRequest = new GetObjectMetadataRequest(getBucket(), expandedPathFormat);
        logger.log(Level.FINE, "getState for S3 bucket {0}:{1}", new Object[]{getBucket(), expandedPathFormat});
        try {
            ObjectMetadata objectMetadata = amazonS3.getObjectMetadata(getObjectRequest);
            String metaId = objectMetadata.getUserMetadata().get("rundeck.execid");
            logger.log(Level.FINE, "Metadata {0}", new Object[]{objectMetadata.getUserMetadata()});
            boolean matchesId = context.get("execid").equals(metaId);
            if(!matchesId) {
                logger.log(Level.WARNING, "S3 Object metadata 'rundeck.execid' was not expected: {0}, expected {1}",
                        new Object[]{metaId, context.get("execid")});
            }
            return LogFileState.AVAILABLE;
        } catch (AmazonS3Exception e) {
            if(e.getStatusCode()==404) {
                //not found
                logger.log(Level.FINE, "getState: S3 Object not found for {0}", expandedPathFormat);
            }else{
                logger.log(Level.SEVERE, e.getMessage());
                logger.log(Level.FINE, e.getMessage(), e);
            }
        } catch (AmazonClientException e) {
            logger.log(Level.SEVERE, e.getMessage());
            logger.log(Level.FINE, e.getMessage(), e);
        }

        return state;
    }

    public boolean storeLogFile(InputStream stream) throws IOException {
        PutObjectResult putObjectResult = null;
        boolean success = false;
        File tempfile = File.createTempFile("S3LogFileStoragePlugin", "temp");
        tempfile.deleteOnExit();
        try {
            FileOutputStream out = new FileOutputStream(tempfile);
            try {
                Streams.copyStream(stream, out);
            } finally {
                out.close();
            }
            logger.log(Level.SEVERE, "Storing content to S3 bucket {0} path {1}", new Object[]{getBucket(),
                    expandedPathFormat});
            PutObjectRequest putObjectRequest = new PutObjectRequest(getBucket(), expandedPathFormat, tempfile);
            putObjectResult = amazonS3.putObject(putObjectRequest.withMetadata(createObjectMetadata()));
            success = true;
        } catch (AmazonClientException e) {
            logger.log(Level.SEVERE, e.getMessage(), e);
        } finally {
            if (tempfile.exists()) {
                tempfile.delete();
            }
        }

        return success;
    }

    private static final String[] STORED_META = new String[]{"execid", "username", "project"};

    private ObjectMetadata createObjectMetadata() {
        ObjectMetadata metadata = new ObjectMetadata();
        for (String s : STORED_META) {
            Object v = context.get(s);
            if (null != v) {
                metadata.addUserMetadata("rundeck." + s, v.toString());
            }
        }
        return metadata;
    }


    public boolean retrieveLogFile(OutputStream stream) throws IOException {
        S3Object object = null;
        boolean success = false;
        try {
            object = amazonS3.getObject(getBucket(), expandedPathFormat);
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
        context.put("execid", "165");
        context.put("name", "test job");
        context.put("group", "test group");
        context.put("id", "testjobid");
        context.put("username", "testuser");
        if ("state".equals(action)) {
            context.put("execid", args[2]);
        }

        s3LogFileStoragePlugin.setAwsAccessKey("AKIAJ63ESPDAOS5FKWNQ");
        s3LogFileStoragePlugin.setAwsSecretKey(args[1]);
        s3LogFileStoragePlugin.setBucket("test-rundeck-logs");
        s3LogFileStoragePlugin.setPathFormat("logs/$PROJECT/$ID.log");
        s3LogFileStoragePlugin.setRegion("us-east-1");

        s3LogFileStoragePlugin.initialize(context);

        if ("store".equals(action)) {
            s3LogFileStoragePlugin.storeLogFile(new FileInputStream(new File(args[2])));
        } else if ("retrieve".equals(action)) {
            s3LogFileStoragePlugin.retrieveLogFile(new FileOutputStream(new File(args[2])));
        } else if ("state".equals(action)) {
            System.out.println(s3LogFileStoragePlugin.getState());
        }
    }

    public String getAwsAccessKey() {
        return awsAccessKey;
    }

    public void setAwsAccessKey(String awsAccessKey) {
        this.awsAccessKey = awsAccessKey;
    }

    public String getAwsSecretKey() {
        return awsSecretKey;
    }

    public void setAwsSecretKey(String awsSecretKey) {
        this.awsSecretKey = awsSecretKey;
    }

    public String getBucket() {
        return bucket;
    }

    public void setBucket(String bucket) {
        this.bucket = bucket;
    }

    public String getPathFormat() {
        return pathFormat;
    }

    public void setPathFormat(String pathFormat) {
        this.pathFormat = pathFormat;
    }

    public String getRegion() {
        return region;
    }

    public void setRegion(String region) {
        this.region = region;
    }
}
