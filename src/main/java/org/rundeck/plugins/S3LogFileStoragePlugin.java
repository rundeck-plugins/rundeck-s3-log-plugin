package org.rundeck.plugins;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.dtolabs.rundeck.core.logging.LogFileState;
import com.dtolabs.rundeck.core.plugins.Plugin;
import com.dtolabs.rundeck.plugins.ServiceNameConstants;
import com.dtolabs.rundeck.plugins.descriptions.PluginDescription;
import com.dtolabs.rundeck.plugins.descriptions.PluginProperty;
import com.dtolabs.rundeck.plugins.logging.LogFileStoragePlugin;
import com.dtolabs.utils.Streams;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
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
            "$JID = job ID). Default: /logs/$ID", defaultValue = "/logs/$ID")
    private String pathFormat;

    @PluginProperty(title = "S3 Region", description = "AWS S3 Region to use.  You can use one of the supported " +
            "region names", required = true, defaultValue = "us-east-1")
    private String region;

    private String expandedPathFormat;

    public S3LogFileStoragePlugin() {
    }

    private AmazonS3 amazonS3;

    public void initialize(Map<String, ? extends Object> context) {

        expandedPathFormat = expandFormat(pathFormat, context);

        amazonS3 = new AmazonS3Client(new AWSCredentials() {
            public String getAWSAccessKeyId() {
                return awsAccessKey;
            }

            public String getAWSSecretKey() {
                return awsSecretKey;
            }
        });
        Region awsregion = Region.getRegion(Regions.valueOf(region));
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
        String result = pathFormat;
        result = result.replaceAll("\\$ID", context.get("execid").toString());
        Object id = context.get("id");
        Object jobName = context.get("name");
        Object jobGroup = context.get("group");
        result = result.replaceAll("\\$JID", null != id ? id.toString() : "");
        result = result.replaceAll("\\$JOB", null != jobName ? jobName.toString() : "");
        result = result.replaceAll("\\$GROUP", null != jobGroup ? jobGroup.toString() : "");
        result = result.replaceAll("\\$PROJECT", context.get("project").toString());
        result = result.replaceAll("\\$USER", context.get("user").toString());

        return result;
    }

    public LogFileState getState() {
        return null;
    }

    public boolean storeLogFile(InputStream stream) throws IOException {
        PutObjectResult putObjectResult = null;
        boolean success = false;
        try {
            putObjectResult = amazonS3.putObject(bucket, expandedPathFormat, stream, null);
            success = true;
        } catch (AmazonClientException e) {
            logger.log(Level.SEVERE, e.getMessage(), e);
        }

        return success;
    }


    public boolean retrieveLogFile(OutputStream stream) throws IOException {
        S3Object object = null;
        boolean success = false;
        try {
            object = amazonS3.getObject(bucket, expandedPathFormat);
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
}
