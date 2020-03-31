# Rundeck S3 Log Storage Plugin

This is a plugin for [Rundeck](http://rundeck.org) that uses [Amazon S3](http://aws.amazon.com/s3) to store execution
log files for backups or for cloud-friendly behavior.

## Build

``` bash
./gradlew clean build
```

## Installation

1. Copy the `rundeck-s3-log-plugin-x.y.jar` file to the `libext/` directory inside your Rundeck installation. You can find the releases [here](https://github.com/rundeck-plugins/rundeck-s3-log-plugin/releases).

2. Enable the ExecutionFileStorage provider named `org.rundeck.amazon-s3` in your `rundeck-config.properties` file:

``` bash
# Set log execution storage backend to Amazon S3
rundeck.execution.logs.fileStoragePlugin=org.rundeck.amazon-s3
```

## Configuration

You can define the configuration values `$RUNDECK_DIR/framework.properties` by prefixing the property name with the stem. For example:

``` bash
framework.plugin.ExecutionFileStorage.org.rundeck.amazon-s3.<key>=<value>
```

Alternatively, use project.properties file with the stem `project.plugin.ExecutionFileStorage.org.rundeck.amazon-s3` to achieve per-project configuration.

## Configure AWS Credentials

The plugin will use the "credentials provider chain" for AWS access credentials, which allows you to  externally configure the credentials in any of three ways:

1. Environment variables `AWS_ACCESS_KEY_ID` and `AWS_SECRET_KEY`
2. Java system properties `aws.accessKeyId` and `aws.secretKey`
3. Instance Profile credentials, if you are running on EC2. (See [the IAM user guide][1]).

[1]: http://docs.aws.amazon.com/IAM/latest/UserGuide/role-usecase-ec2app.html

If you want to specify access key and secret key, you can do so in the configuration:

The plugin allows you to set the following property key/values for **credentials**:

> NB: These are stemmed with `framework.plugin.ExecutionFileStorage.org.rundeck.amazon-s3`

`AWSAccessKeyId`: AWS access key, required if using `AWSSecretKey`
`AWSSecretKey`: AWS secret key, required if using `AWSAccessKeyId`
`AWSCredentialsFile`: Properties file which contains `accessKey` and `secretKey` entries. The alternative to specifying
the `AWSAccessKeyId` and `AWSSecretKey`

The plugin uses uses the following key/values for configuring the **AWS S3 Bucket**:

`bucket`: The name of the S3 bucket to use. This is the shorthand name, eg `test-rundeck-logs`

`path`: A path-like string that defines where in the bucket to store the log for a particular execution. You can
 include variables. Default value: `rundeck/project/$PROJECT/logs/$ID`

Variables in the `path` value include:

* `${job.execid}` - the execution ID
* `${job.project}` - the project name
* `${job.id}` - the Job UUID if it exists
* `${job.group}` - the Job Group if it exists
* `${job.path}` - the Job Name if it exists

`region`: AWS region name to use. Default: `us-east-1`

`endpoint`: Optional, a custom S3 compatible endpoint to use, such as `https://my-host.com/s3`

`pathStyle`: Optional, boolean, default=False, set to True if you need to define the bucket in your S3 like endpoint URL. e.g:
 `https://\<s3_like_end_point_url\>/\<your_bucket_name\>`

A custom way of defining buckets for your endpoint. Useful for non-AWS S3 like object storage technology e.g [SwiftStack](https://swiftstack.com), Optums, etc. This [background information](http://docs.aws.amazon.com/AmazonS3/latest/dev/VirtualHosting.html) should be useful.

## Basic Example (/etc/rundeck/framework.properties)

``` bash
# AWSAccessKeyId and AWSSecretKey can be specified in the file
framework.plugin.ExecutionFileStorage.org.rundeck.amazon-s3.AWSAccessKeyId=ABC123...
framework.plugin.ExecutionFileStorage.org.rundeck.amazon-s3.AWSSecretKey=ABC321...

# Alternately, AWSCredentialsFile can point to a custom file which contains `accessKey` and `secretKey`
framework.plugin.ExecutionFileStorage.org.rundeck.amazon-s3.AWSCredentialsFile=/path/to/awscredentials.properties

# Name of the bucket
framework.plugin.ExecutionFileStorage.org.rundeck.amazon-s3.bucket=test-rundeck-logs

# Path to store the logs
framework.plugin.ExecutionFileStorage.org.rundeck.amazon-s3.path=logs/${job.project}/${job.execid}.log
```

## Using with Rundeck SSL Configuration

If you want to use this plugin when you have Rundeck configured with a custom SSL truststore, you will need to import the Amazon S3 SSL certificates to your truststore.

~~~ bash
echo -n | openssl s_client -connect my-bucket.s3.amazonaws.com:443 > certs.out
keytool -importcert -trustcacerts -file certs.out -alias s3-amazonaws -keystore $RDECK_BASE/etc/truststore
~~~
