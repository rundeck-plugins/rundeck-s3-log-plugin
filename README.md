# Rundeck S3 Log Storage Plugin

This is a plugin for [Rundeck](http://rundeck.org) that uses [Amazon S3](http://aws.amazon.com/s3) to store execution log files, for backup or for cloud-friendly behavior.

## Build

    ./gradlew clean build

## Install

Copy the `rundeck-s3-log-plugin-1.0.jar` file to the `libext/` directory inside your Rundeck installation.

Enable the LogFileStorage provider named `org.rundeck.amazon-s3` in your `rundeck-config` file:

    rundeck.execution.logs.fileStoragePlugin=org.rundeck.amazon-s3

## Configuration

To configure the plugin set these plugin configuration property values:

`AWSAccessKeyId` : access key, required if using `AWSSecretKey`

`AWSSecretKey` : secret key, required if using `AWSAccessKeyId`

`AWSCredentialsFile` : properties file which contains `accessKey` and `secretKey` entries.  Alternative to specifying
the `AWSAccessKeyId and `AWSSecretKey`

`bucket` : name of the S3 bucket to use

`path` :  a path-like string that defines where in the bucket to store the log for a particular execution.  You can
 include variables to expand. Default value: `rundeck/project/$PROJECT/logs/$ID`

 `region` : name of the region to use. Default is `us-east-1`.

Variables include:

* `${job.execid}` - the execution ID
* `${job.project}` - the project name
* `${job.id}` - the Job UUID if it exists


`region` : AWS region name to use. Default: `us-east-1`

Add configuration properties for the plugin.  You can define these in `framework.properties` by prefixing the property name with the stem: `framework.plugin.LogFileStorage.org.rundeck.amazon-s3.`.  Or in a project's project.properties file with the stem `project.plugin.LogFileStorage.org.rundeck.amazon-s3.`.

For example:

    #AWSAccessKeyId and AWSSecretKey can be specified in the file
    framework.plugin.LogFileStorage.org.rundeck.amazon-s3.AWSAccessKeyId=ABC123...
    framework.plugin.LogFileStorage.org.rundeck.amazon-s3.AWSSecretKey=ABC321...

    #alternately, AWSCredentialsFile can point to a file which contains `accessKey` and `secretKey`
    framework.plugin.LogFileStorage.org.rundeck.amazon-s3.AWSCredentialsFile=/path/to/awscredentials.properties

    #name of the bucket
    framework.plugin.LogFileStorage.org.rundeck.amazon-s3.bucket=test-rundeck-logs

    #path to store the logs
    framework.plugin.LogFileStorage.org.rundeck.amazon-s3.path=logs/${job.project}/${job.execid}.log
