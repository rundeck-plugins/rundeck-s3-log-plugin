package org.rundeck.plugins;

import com.amazonaws.*;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AbstractAmazonS3;
import com.amazonaws.services.s3.S3ClientOptions;
import com.amazonaws.services.s3.S3ResponseMetadata;
import com.amazonaws.services.s3.model.*;
import com.amazonaws.services.s3.model.analytics.AnalyticsConfiguration;
import com.amazonaws.services.s3.model.inventory.InventoryConfiguration;
import com.amazonaws.services.s3.model.metrics.MetricsConfiguration;
import com.amazonaws.services.s3.waiters.AmazonS3Waiters;
import org.junit.Assert;

import java.io.File;
import java.io.InputStream;
import java.net.URL;
import java.util.Date;
import java.util.List;

/**
* $INTERFACE is ... User: greg Date: 6/12/13 Time: 7:28 PM
*/
class FailS3 extends AbstractAmazonS3 {

    public void setEndpoint(String endpoint) {
        Assert.fail("unexpected");
    }

    public void setRegion(com.amazonaws.regions.Region region) throws IllegalArgumentException {
        Assert.fail("unexpected");
    }

    public void setS3ClientOptions(S3ClientOptions clientOptions) {
        Assert.fail("unexpected");
    }

    public void changeObjectStorageClass(String bucketName, String key, StorageClass newStorageClass) throws AmazonClientException, AmazonServiceException {
        Assert.fail("unexpected");
    }

    public void setObjectRedirectLocation(String bucketName, String key, String newRedirectLocation) throws AmazonClientException, AmazonServiceException {
        Assert.fail("unexpected");
    }

    public ObjectListing listObjects(String bucketName) throws AmazonClientException, AmazonServiceException {
        Assert.fail("unexpected");
        return null;
    }

    public ObjectListing listObjects(String bucketName, String prefix) throws AmazonClientException, AmazonServiceException {
        Assert.fail("unexpected");
        return null;
    }

    public ObjectListing listObjects(ListObjectsRequest listObjectsRequest) throws AmazonClientException, AmazonServiceException {
        Assert.fail("unexpected");
        return null;
    }

    public ObjectListing listNextBatchOfObjects(ObjectListing previousObjectListing) throws AmazonClientException, AmazonServiceException {
        Assert.fail("unexpected");
        return null;
    }

    public ObjectListing listNextBatchOfObjects(ListNextBatchOfObjectsRequest listNextBatchOfObjectsRequest) throws AmazonClientException, AmazonServiceException {
        Assert.fail("unexpected");
        return null;
    }

    public VersionListing listVersions(String bucketName, String prefix) throws AmazonClientException, AmazonServiceException {
        Assert.fail("unexpected");
        return null;
    }

    public VersionListing listNextBatchOfVersions(VersionListing previousVersionListing) throws AmazonClientException, AmazonServiceException {
        Assert.fail("unexpected");
        return null;
    }

    public VersionListing listNextBatchOfVersions(ListNextBatchOfVersionsRequest listNextBatchOfVersionsRequest) throws AmazonClientException, AmazonServiceException {
        Assert.fail("unexpected");
        return null;
    }

    public VersionListing listVersions(String bucketName, String prefix, String keyMarker, String versionIdMarker, String delimiter, Integer maxResults) throws AmazonClientException, AmazonServiceException {
        Assert.fail("unexpected");
        return null;
    }

    public VersionListing listVersions(ListVersionsRequest listVersionsRequest) throws AmazonClientException, AmazonServiceException {
        Assert.fail("unexpected");
        return null;
    }

    public Owner getS3AccountOwner() throws AmazonClientException, AmazonServiceException {
        Assert.fail("unexpected");
        return null;
    }

    public Owner getS3AccountOwner(GetS3AccountOwnerRequest getS3AccountOwnerRequest) throws AmazonClientException, AmazonServiceException {
        Assert.fail("unexpected");
        return null;
    }

    public boolean doesBucketExist(String bucketName) throws AmazonClientException, AmazonServiceException {
        Assert.fail("unexpected");
        return false;
    }

    public HeadBucketResult headBucket(HeadBucketRequest headBucketRequest) throws AmazonClientException, AmazonServiceException {
        Assert.fail("unexpected");
        return null;
    }

    public List<Bucket> listBuckets() throws AmazonClientException, AmazonServiceException {
        Assert.fail("unexpected");
        return null;
    }

    public List<Bucket> listBuckets(ListBucketsRequest listBucketsRequest) throws AmazonClientException, AmazonServiceException {
        Assert.fail("unexpected");
        return null;
    }

    public String getBucketLocation(String bucketName) throws AmazonClientException, AmazonServiceException {
        Assert.fail("unexpected");
        return null;
    }

    public String getBucketLocation(GetBucketLocationRequest getBucketLocationRequest) throws AmazonClientException, AmazonServiceException {
        Assert.fail("unexpected");
        return null;
    }

    public Bucket createBucket(CreateBucketRequest createBucketRequest) throws AmazonClientException, AmazonServiceException {
        Assert.fail("unexpected");
        return null;
    }

    public Bucket createBucket(String bucketName) throws AmazonClientException, AmazonServiceException {
        Assert.fail("unexpected");
        return null;
    }

    public Bucket createBucket(String bucketName, Region region) throws AmazonClientException, AmazonServiceException {
        Assert.fail("unexpected");
        return null;
    }

    public Bucket createBucket(String bucketName, String region) throws AmazonClientException, AmazonServiceException {
        Assert.fail("unexpected");
        return null;
    }

    public AccessControlList getObjectAcl(String bucketName, String key) throws AmazonClientException, AmazonServiceException {
        Assert.fail("unexpected");
        return null;
    }

    public AccessControlList getObjectAcl(String bucketName, String key, String versionId) throws AmazonClientException, AmazonServiceException {
        Assert.fail("unexpected");
        return null;
    }

    public AccessControlList getObjectAcl(GetObjectAclRequest getObjectAclRequest) throws AmazonClientException, AmazonServiceException {
        Assert.fail("unexpected");
        return null;
    }

    public void setObjectAcl(String bucketName, String key, AccessControlList acl) throws AmazonClientException, AmazonServiceException {
        Assert.fail("unexpected");
    }

    public void setObjectAcl(String bucketName, String key, CannedAccessControlList acl) throws AmazonClientException, AmazonServiceException {
        Assert.fail("unexpected");
    }

    public void setObjectAcl(String bucketName, String key, String versionId, AccessControlList acl) throws AmazonClientException, AmazonServiceException {
        Assert.fail("unexpected");
    }

    public void setObjectAcl(String bucketName, String key, String versionId, CannedAccessControlList acl) throws AmazonClientException, AmazonServiceException {
        Assert.fail("unexpected");
    }

    public void setObjectAcl(SetObjectAclRequest setObjectAclRequest) throws AmazonClientException, AmazonServiceException {
        Assert.fail("unexpected");
    }

    public AccessControlList getBucketAcl(String bucketName) throws AmazonClientException, AmazonServiceException {
        Assert.fail("unexpected");
        return null;
    }

    public void setBucketAcl(SetBucketAclRequest setBucketAclRequest) throws AmazonClientException, AmazonServiceException {
        Assert.fail("unexpected");
    }

    public AccessControlList getBucketAcl(GetBucketAclRequest getBucketAclRequest) throws AmazonClientException, AmazonServiceException {
        Assert.fail("unexpected");
        return null;
    }

    public void setBucketAcl(String bucketName, AccessControlList acl) throws AmazonClientException, AmazonServiceException {
        Assert.fail("unexpected");
    }

    public void setBucketAcl(String bucketName, CannedAccessControlList acl) throws AmazonClientException, AmazonServiceException {
        Assert.fail("unexpected");
    }

    public ObjectMetadata getObjectMetadata(String bucketName, String key) throws AmazonClientException, AmazonServiceException {
        Assert.fail("unexpected");
        return null;
    }

    public ObjectMetadata getObjectMetadata(GetObjectMetadataRequest getObjectMetadataRequest) throws AmazonClientException, AmazonServiceException {
        Assert.fail("unexpected");
        return null;
    }

    public S3Object getObject(String bucketName, String key) throws AmazonClientException, AmazonServiceException {
        Assert.fail("unexpected");
        return null;
    }

    public S3Object getObject(GetObjectRequest getObjectRequest) throws AmazonClientException, AmazonServiceException {
        Assert.fail("unexpected");
        return null;
    }

    public ObjectMetadata getObject(GetObjectRequest getObjectRequest, File destinationFile) throws AmazonClientException, AmazonServiceException {
        Assert.fail("unexpected");
        return null;
    }

    public void deleteBucket(DeleteBucketRequest deleteBucketRequest) throws AmazonClientException, AmazonServiceException {
        Assert.fail("unexpected");
    }

    public void deleteBucket(String bucketName) throws AmazonClientException, AmazonServiceException {
        Assert.fail("unexpected");
    }

    public PutObjectResult putObject(PutObjectRequest putObjectRequest) throws AmazonClientException, AmazonServiceException {
        Assert.fail("unexpected");
        return null;
    }

    public PutObjectResult putObject(String bucketName, String key, File file) throws AmazonClientException, AmazonServiceException {
        Assert.fail("unexpected");
        return null;
    }

    public PutObjectResult putObject(String bucketName, String key, InputStream input, ObjectMetadata metadata) throws AmazonClientException, AmazonServiceException {
        Assert.fail("unexpected");
        return null;
    }

    public CopyObjectResult copyObject(String sourceBucketName, String sourceKey, String destinationBucketName, String destinationKey) throws AmazonClientException, AmazonServiceException {
        Assert.fail("unexpected");
        return null;
    }

    public CopyObjectResult copyObject(CopyObjectRequest copyObjectRequest) throws AmazonClientException, AmazonServiceException {
        Assert.fail("unexpected");
        return null;
    }

    public CopyPartResult copyPart(CopyPartRequest copyPartRequest) throws AmazonClientException, AmazonServiceException {
        Assert.fail("unexpected");
        return null;
    }

    public void deleteObject(String bucketName, String key) throws AmazonClientException, AmazonServiceException {
        Assert.fail("unexpected");
    }

    public void deleteObject(DeleteObjectRequest deleteObjectRequest) throws AmazonClientException, AmazonServiceException {
        Assert.fail("unexpected");
    }

    public DeleteObjectsResult deleteObjects(DeleteObjectsRequest deleteObjectsRequest) throws AmazonClientException, AmazonServiceException {
        Assert.fail("unexpected");
        return null;
    }

    public void deleteVersion(String bucketName, String key, String versionId) throws AmazonClientException, AmazonServiceException {
        Assert.fail("unexpected");
    }

    public void deleteVersion(DeleteVersionRequest deleteVersionRequest) throws AmazonClientException, AmazonServiceException {
        Assert.fail("unexpected");
    }

    public BucketLoggingConfiguration getBucketLoggingConfiguration(String bucketName) throws AmazonClientException, AmazonServiceException {
        Assert.fail("unexpected");
        return null;
    }

    public BucketLoggingConfiguration getBucketLoggingConfiguration(GetBucketLoggingConfigurationRequest getBucketLoggingConfigurationRequest) throws AmazonClientException, AmazonServiceException {
        Assert.fail("unexpected");
        return null;
    }

    public void setBucketLoggingConfiguration(SetBucketLoggingConfigurationRequest setBucketLoggingConfigurationRequest) throws AmazonClientException, AmazonServiceException {
        Assert.fail("unexpected");
    }

    public BucketVersioningConfiguration getBucketVersioningConfiguration(String bucketName) throws AmazonClientException, AmazonServiceException {
        Assert.fail("unexpected");
        return null;
    }

    public BucketVersioningConfiguration getBucketVersioningConfiguration(GetBucketVersioningConfigurationRequest getBucketVersioningConfigurationRequest) throws AmazonClientException, AmazonServiceException {
        Assert.fail("unexpected");
        return null;
    }

    public void setBucketVersioningConfiguration(SetBucketVersioningConfigurationRequest setBucketVersioningConfigurationRequest) throws AmazonClientException, AmazonServiceException {
        Assert.fail("unexpected");
    }

    public BucketLifecycleConfiguration getBucketLifecycleConfiguration(String bucketName) {
        Assert.fail("unexpected");
        return null;
    }

    public BucketLifecycleConfiguration getBucketLifecycleConfiguration(GetBucketLifecycleConfigurationRequest getBucketLifecycleConfigurationRequest) {
        Assert.fail("unexpected");
        return null;
    }

    public void setBucketLifecycleConfiguration(String bucketName, BucketLifecycleConfiguration bucketLifecycleConfiguration) {
        Assert.fail("unexpected");
    }

    public void setBucketLifecycleConfiguration(SetBucketLifecycleConfigurationRequest setBucketLifecycleConfigurationRequest) {
        Assert.fail("unexpected");
    }

    public void deleteBucketLifecycleConfiguration(String bucketName) {
        Assert.fail("unexpected");
    }

    public void deleteBucketLifecycleConfiguration(DeleteBucketLifecycleConfigurationRequest deleteBucketLifecycleConfigurationRequest) {
        Assert.fail("unexpected");
    }

    public BucketCrossOriginConfiguration getBucketCrossOriginConfiguration(String bucketName) {
        Assert.fail("unexpected");
        return null;
    }

    public BucketCrossOriginConfiguration getBucketCrossOriginConfiguration(GetBucketCrossOriginConfigurationRequest getBucketCrossOriginConfigurationRequest) {
        Assert.fail("unexpected");
        return null;
    }

    public void setBucketCrossOriginConfiguration(String bucketName, BucketCrossOriginConfiguration bucketCrossOriginConfiguration) {
        Assert.fail("unexpected");
    }

    public void setBucketCrossOriginConfiguration(SetBucketCrossOriginConfigurationRequest setBucketCrossOriginConfigurationRequest) {
        Assert.fail("unexpected");
    }

    public void deleteBucketCrossOriginConfiguration(String bucketName) {
        Assert.fail("unexpected");
    }

    public void deleteBucketCrossOriginConfiguration(DeleteBucketCrossOriginConfigurationRequest deleteBucketCrossOriginConfigurationRequest) {
        Assert.fail("unexpected");
    }

    public BucketTaggingConfiguration getBucketTaggingConfiguration(String bucketName) {
        Assert.fail("unexpected");
        return null;
    }

    public BucketTaggingConfiguration getBucketTaggingConfiguration(GetBucketTaggingConfigurationRequest getBucketTaggingConfigurationRequest) {
        Assert.fail("unexpected");
        return null;
    }

    public void setBucketTaggingConfiguration(String bucketName, BucketTaggingConfiguration bucketTaggingConfiguration) {
        Assert.fail("unexpected");
    }

    public void setBucketTaggingConfiguration(SetBucketTaggingConfigurationRequest setBucketTaggingConfigurationRequest) {
        Assert.fail("unexpected");
    }

    public void deleteBucketTaggingConfiguration(String bucketName) {
        Assert.fail("unexpected");
    }

    public void deleteBucketTaggingConfiguration(DeleteBucketTaggingConfigurationRequest deleteBucketTaggingConfigurationRequest) {
        Assert.fail("unexpected");
    }

    public BucketNotificationConfiguration getBucketNotificationConfiguration(String bucketName) throws AmazonClientException, AmazonServiceException {
        Assert.fail("unexpected");
        return null;
    }

    public BucketNotificationConfiguration getBucketNotificationConfiguration(GetBucketNotificationConfigurationRequest getBucketNotificationConfigurationRequest) throws AmazonClientException, AmazonServiceException {
        Assert.fail("unexpected");
        return null;
    }

    public void setBucketNotificationConfiguration(SetBucketNotificationConfigurationRequest setBucketNotificationConfigurationRequest) throws AmazonClientException, AmazonServiceException {
        Assert.fail("unexpected");
    }

    public void setBucketNotificationConfiguration(String bucketName, BucketNotificationConfiguration bucketNotificationConfiguration) throws AmazonClientException, AmazonServiceException {
        Assert.fail("unexpected");
    }

    public BucketWebsiteConfiguration getBucketWebsiteConfiguration(String bucketName) throws AmazonClientException, AmazonServiceException {
        Assert.fail("unexpected");
        return null;
    }

    public BucketWebsiteConfiguration getBucketWebsiteConfiguration(GetBucketWebsiteConfigurationRequest getBucketWebsiteConfigurationRequest) throws AmazonClientException, AmazonServiceException {
        Assert.fail("unexpected");
        return null;
    }

    public void setBucketWebsiteConfiguration(String bucketName, BucketWebsiteConfiguration configuration) throws AmazonClientException, AmazonServiceException {
        Assert.fail("unexpected");
    }

    public void setBucketWebsiteConfiguration(SetBucketWebsiteConfigurationRequest setBucketWebsiteConfigurationRequest) throws AmazonClientException, AmazonServiceException {
        Assert.fail("unexpected");
    }

    public void deleteBucketWebsiteConfiguration(String bucketName) throws AmazonClientException, AmazonServiceException {
        Assert.fail("unexpected");
    }

    public void deleteBucketWebsiteConfiguration(DeleteBucketWebsiteConfigurationRequest deleteBucketWebsiteConfigurationRequest) throws AmazonClientException, AmazonServiceException {
        Assert.fail("unexpected");
    }

    public BucketPolicy getBucketPolicy(String bucketName) throws AmazonClientException, AmazonServiceException {
        Assert.fail("unexpected");
        return null;
    }

    public BucketPolicy getBucketPolicy(GetBucketPolicyRequest getBucketPolicyRequest) throws AmazonClientException, AmazonServiceException {
        Assert.fail("unexpected");
        return null;
    }

    public void setBucketPolicy(String bucketName, String policyText) throws AmazonClientException, AmazonServiceException {
        Assert.fail("unexpected");
    }

    public void setBucketPolicy(SetBucketPolicyRequest setBucketPolicyRequest) throws AmazonClientException, AmazonServiceException {
        Assert.fail("unexpected");
    }

    public void deleteBucketPolicy(String bucketName) throws AmazonClientException, AmazonServiceException {
        Assert.fail("unexpected");
    }

    public void deleteBucketPolicy(DeleteBucketPolicyRequest deleteBucketPolicyRequest) throws AmazonClientException, AmazonServiceException {
        Assert.fail("unexpected");
    }

    public URL generatePresignedUrl(String bucketName, String key, Date expiration) throws AmazonClientException {
        Assert.fail("unexpected");
        return null;
    }

    public URL generatePresignedUrl(String bucketName, String key, Date expiration, HttpMethod method) throws AmazonClientException {
        Assert.fail("unexpected");
        return null;
    }

    public URL generatePresignedUrl(GeneratePresignedUrlRequest generatePresignedUrlRequest) throws AmazonClientException {
        Assert.fail("unexpected");
        return null;
    }

    public InitiateMultipartUploadResult initiateMultipartUpload(InitiateMultipartUploadRequest request) throws AmazonClientException, AmazonServiceException {
        Assert.fail("unexpected");
        return null;
    }

    public UploadPartResult uploadPart(UploadPartRequest request) throws AmazonClientException, AmazonServiceException {
        Assert.fail("unexpected");
        return null;
    }

    public PartListing listParts(ListPartsRequest request) throws AmazonClientException, AmazonServiceException {
        Assert.fail("unexpected");
        return null;
    }

    public void abortMultipartUpload(AbortMultipartUploadRequest request) throws AmazonClientException, AmazonServiceException {
        Assert.fail("unexpected");
    }

    public CompleteMultipartUploadResult completeMultipartUpload(CompleteMultipartUploadRequest request) throws AmazonClientException, AmazonServiceException {
        Assert.fail("unexpected");
        return null;
    }

    public MultipartUploadListing listMultipartUploads(ListMultipartUploadsRequest request) throws AmazonClientException, AmazonServiceException {
        Assert.fail("unexpected");
        return null;
    }

    public S3ResponseMetadata getCachedResponseMetadata(AmazonWebServiceRequest request) {
        Assert.fail("unexpected");
        return null;
    }

    public void restoreObject(RestoreObjectRequest request) throws AmazonServiceException {
        Assert.fail("unexpected");
    }

    public void restoreObject(String bucketName, String key, int expirationInDays) throws AmazonServiceException {
        Assert.fail("unexpected");
    }

    public void enableRequesterPays(String bucketName) throws AmazonServiceException, AmazonClientException {
        Assert.fail("unexpected");
    }

    public void disableRequesterPays(String bucketName) throws AmazonServiceException, AmazonClientException {
        Assert.fail("unexpected");
    }

    public boolean isRequesterPaysEnabled(String bucketName) throws AmazonServiceException, AmazonClientException {
        Assert.fail("unexpected");
        return false;
    }

    public void setBucketReplicationConfiguration(String bucketName, BucketReplicationConfiguration configuration) throws AmazonServiceException, AmazonClientException {
        Assert.fail("unexpected");
    }

    public void setBucketReplicationConfiguration(SetBucketReplicationConfigurationRequest setBucketReplicationConfigurationRequest) throws AmazonServiceException, AmazonClientException {
        Assert.fail("unexpected");
    }

    public BucketReplicationConfiguration getBucketReplicationConfiguration(String bucketName) throws AmazonServiceException, AmazonClientException {
        Assert.fail("unexpected");
        return null;
    }

    public BucketReplicationConfiguration getBucketReplicationConfiguration(GetBucketReplicationConfigurationRequest getBucketReplicationConfigurationRequest) throws AmazonServiceException, AmazonClientException {
        Assert.fail("unexpected");
        return null;
    }

    public void deleteBucketReplicationConfiguration(String bucketName) throws AmazonServiceException, AmazonClientException {
        Assert.fail("unexpected");
    }

    public void deleteBucketReplicationConfiguration(DeleteBucketReplicationConfigurationRequest request) throws AmazonServiceException, AmazonClientException {
        Assert.fail("unexpected");
    }

    public boolean doesObjectExist(String bucketName, String objectName) throws AmazonServiceException, AmazonClientException {
        Assert.fail("unexpected");
        return false;
    }

    @Override
    public ListObjectsV2Result listObjectsV2(final String bucketName)
            throws SdkClientException, AmazonServiceException
    {
        return null;
    }

    @Override
    public ListObjectsV2Result listObjectsV2(final String bucketName, final String prefix)
            throws SdkClientException, AmazonServiceException
    {
        return null;
    }

    @Override
    public ListObjectsV2Result listObjectsV2(final ListObjectsV2Request listObjectsV2Request)
            throws SdkClientException, AmazonServiceException
    {
        return null;
    }

    @Override
    public String getObjectAsString(final String bucketName, final String key)
            throws AmazonServiceException, SdkClientException
    {
        return null;
    }

    @Override
    public GetObjectTaggingResult getObjectTagging(final GetObjectTaggingRequest getObjectTaggingRequest) {
        return null;
    }

    @Override
    public SetObjectTaggingResult setObjectTagging(final SetObjectTaggingRequest setObjectTaggingRequest) {
        return null;
    }

    @Override
    public DeleteObjectTaggingResult deleteObjectTagging(final DeleteObjectTaggingRequest deleteObjectTaggingRequest) {
        return null;
    }

    @Override
    public PutObjectResult putObject(final String bucketName, final String key, final String content)
            throws AmazonServiceException, SdkClientException
    {
        return null;
    }

    @Override
    public BucketAccelerateConfiguration getBucketAccelerateConfiguration(final String bucketName)
            throws AmazonServiceException, SdkClientException
    {
        return null;
    }

    @Override
    public BucketAccelerateConfiguration getBucketAccelerateConfiguration(final GetBucketAccelerateConfigurationRequest getBucketAccelerateConfigurationRequest)
            throws AmazonServiceException, SdkClientException
    {
        return null;
    }

    @Override
    public void setBucketAccelerateConfiguration(
            final String bucketName, final BucketAccelerateConfiguration accelerateConfiguration
    ) throws AmazonServiceException, SdkClientException
    {

    }

    @Override
    public void setBucketAccelerateConfiguration(final SetBucketAccelerateConfigurationRequest setBucketAccelerateConfigurationRequest)
            throws AmazonServiceException, SdkClientException
    {

    }

    @Override
    public DeleteBucketMetricsConfigurationResult deleteBucketMetricsConfiguration(
            final String bucketName,
            final String id
    )
            throws AmazonServiceException, SdkClientException
    {
        return null;
    }

    @Override
    public DeleteBucketMetricsConfigurationResult deleteBucketMetricsConfiguration(
            final DeleteBucketMetricsConfigurationRequest deleteBucketMetricsConfigurationRequest
    ) throws AmazonServiceException, SdkClientException
    {
        return null;
    }

    @Override
    public GetBucketMetricsConfigurationResult getBucketMetricsConfiguration(final String bucketName, final String id)
            throws AmazonServiceException, SdkClientException
    {
        return null;
    }

    @Override
    public GetBucketMetricsConfigurationResult getBucketMetricsConfiguration(final GetBucketMetricsConfigurationRequest getBucketMetricsConfigurationRequest)
            throws AmazonServiceException, SdkClientException
    {
        return null;
    }

    @Override
    public SetBucketMetricsConfigurationResult setBucketMetricsConfiguration(
            final String bucketName, final MetricsConfiguration metricsConfiguration
    ) throws AmazonServiceException, SdkClientException
    {
        return null;
    }

    @Override
    public SetBucketMetricsConfigurationResult setBucketMetricsConfiguration(final SetBucketMetricsConfigurationRequest setBucketMetricsConfigurationRequest)
            throws AmazonServiceException, SdkClientException
    {
        return null;
    }

    @Override
    public ListBucketMetricsConfigurationsResult listBucketMetricsConfigurations(final ListBucketMetricsConfigurationsRequest listBucketMetricsConfigurationsRequest)
            throws AmazonServiceException, SdkClientException
    {
        return null;
    }

    @Override
    public DeleteBucketAnalyticsConfigurationResult deleteBucketAnalyticsConfiguration(
            final String bucketName, final String id
    ) throws AmazonServiceException, SdkClientException
    {
        return null;
    }

    @Override
    public DeleteBucketAnalyticsConfigurationResult deleteBucketAnalyticsConfiguration(
            final DeleteBucketAnalyticsConfigurationRequest deleteBucketAnalyticsConfigurationRequest
    ) throws AmazonServiceException, SdkClientException
    {
        return null;
    }

    @Override
    public GetBucketAnalyticsConfigurationResult getBucketAnalyticsConfiguration(
            final String bucketName,
            final String id
    )
            throws AmazonServiceException, SdkClientException
    {
        return null;
    }

    @Override
    public GetBucketAnalyticsConfigurationResult getBucketAnalyticsConfiguration(final GetBucketAnalyticsConfigurationRequest getBucketAnalyticsConfigurationRequest)
            throws AmazonServiceException, SdkClientException
    {
        return null;
    }

    @Override
    public SetBucketAnalyticsConfigurationResult setBucketAnalyticsConfiguration(
            final String bucketName, final AnalyticsConfiguration analyticsConfiguration
    ) throws AmazonServiceException, SdkClientException
    {
        return null;
    }

    @Override
    public SetBucketAnalyticsConfigurationResult setBucketAnalyticsConfiguration(final
                                                                                     SetBucketAnalyticsConfigurationRequest setBucketAnalyticsConfigurationRequest)
            throws AmazonServiceException, SdkClientException
    {
        return null;
    }

    @Override
    public ListBucketAnalyticsConfigurationsResult listBucketAnalyticsConfigurations(
            final ListBucketAnalyticsConfigurationsRequest listBucketAnalyticsConfigurationsRequest
    ) throws AmazonServiceException, SdkClientException
    {
        return null;
    }

    @Override
    public DeleteBucketInventoryConfigurationResult deleteBucketInventoryConfiguration(
            final String bucketName, final String id
    ) throws AmazonServiceException, SdkClientException
    {
        return null;
    }

    @Override
    public DeleteBucketInventoryConfigurationResult deleteBucketInventoryConfiguration(
            final DeleteBucketInventoryConfigurationRequest deleteBucketInventoryConfigurationRequest
    ) throws AmazonServiceException, SdkClientException
    {
        return null;
    }

    @Override
    public GetBucketInventoryConfigurationResult getBucketInventoryConfiguration(
            final String bucketName,
            final String id
    )
            throws AmazonServiceException, SdkClientException
    {
        return null;
    }

    @Override
    public GetBucketInventoryConfigurationResult getBucketInventoryConfiguration(final GetBucketInventoryConfigurationRequest getBucketInventoryConfigurationRequest)
            throws AmazonServiceException, SdkClientException
    {
        return null;
    }

    @Override
    public SetBucketInventoryConfigurationResult setBucketInventoryConfiguration(
            final String bucketName, final InventoryConfiguration inventoryConfiguration
    ) throws AmazonServiceException, SdkClientException
    {
        return null;
    }

    @Override
    public SetBucketInventoryConfigurationResult setBucketInventoryConfiguration(final SetBucketInventoryConfigurationRequest setBucketInventoryConfigurationRequest)
            throws AmazonServiceException, SdkClientException
    {
        return null;
    }

    @Override
    public ListBucketInventoryConfigurationsResult listBucketInventoryConfigurations(
            final ListBucketInventoryConfigurationsRequest listBucketInventoryConfigurationsRequest
    ) throws AmazonServiceException, SdkClientException
    {
        return null;
    }

    @Override
    public void shutdown() {

    }

    @Override
    public Region getRegion() {
        return null;
    }

    @Override
    public String getRegionName() {
        return null;
    }

    @Override
    public URL getUrl(final String bucketName, final String key) {
        return null;
    }

    @Override
    public AmazonS3Waiters waiters() {
        return null;
    }
}
