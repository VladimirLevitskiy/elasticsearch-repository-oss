package org.elasticsearch.aliyun.oss.service;

import java.io.InputStream;

import com.aliyun.oss.ClientException;
import com.aliyun.oss.OSSException;
import com.aliyun.oss.model.AbortMultipartUploadRequest;
import com.aliyun.oss.model.CompleteMultipartUploadRequest;
import com.aliyun.oss.model.CompleteMultipartUploadResult;
import com.aliyun.oss.model.CopyObjectResult;
import com.aliyun.oss.model.DeleteObjectsRequest;
import com.aliyun.oss.model.DeleteObjectsResult;
import com.aliyun.oss.model.InitiateMultipartUploadRequest;
import com.aliyun.oss.model.InitiateMultipartUploadResult;
import com.aliyun.oss.model.ListObjectsRequest;
import com.aliyun.oss.model.OSSObject;
import com.aliyun.oss.model.ObjectListing;
import com.aliyun.oss.model.ObjectMetadata;
import com.aliyun.oss.model.PutObjectResult;
import com.aliyun.oss.model.UploadPartRequest;
import com.aliyun.oss.model.UploadPartResult;
import org.elasticsearch.aliyun.oss.service.exception.CreateStsOssClientException;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;

/**
 * OSS Service implementation for creating oss client
 * Created by yangkongshi on 2017/11/24.
 */
public class OssServiceImpl implements OssService {

    private OssStorageClient ossStorageClient;

    public OssServiceImpl(RepositoryMetadata metadata) throws CreateStsOssClientException {
        this.ossStorageClient = new OssStorageClient(metadata);
    }

    @Override
    public DeleteObjectsResult deleteObjects(DeleteObjectsRequest deleteObjectsRequest)
            throws OSSException, ClientException {
        return this.ossStorageClient.deleteObjects(deleteObjectsRequest);
    }

    @Override
    public boolean doesObjectExist(String bucketName, String key)
            throws OSSException, ClientException {
        return this.ossStorageClient.doesObjectExist(bucketName, key);
    }

    @Override
    public boolean doesBucketExist(String bucketName)
            throws OSSException, ClientException {
        return this.ossStorageClient.doesBucketExist(bucketName);
    }

    @Override
    public ObjectListing listObjects(ListObjectsRequest listObjectsRequest)
            throws OSSException, ClientException {
        return this.ossStorageClient.listObjects(listObjectsRequest);
    }

    @Override
    public OSSObject getObject(String bucketName, String key)
            throws OSSException, ClientException {
        return this.ossStorageClient.getObject(bucketName, key);
    }

    @Override
    public PutObjectResult putObject(String bucketName, String key, InputStream input,
                                     ObjectMetadata metadata) throws OSSException, ClientException {
        return this.ossStorageClient.putObject(bucketName, key, input, metadata);
    }

    @Override
    public void deleteObject(String bucketName, String key)
            throws OSSException, ClientException {
        this.ossStorageClient.deleteObject(bucketName, key);
    }

    @Override
    public CopyObjectResult copyObject(String sourceBucketName, String sourceKey,
                                       String destinationBucketName, String destinationKey) throws OSSException, ClientException {
        return this.ossStorageClient
                .copyObject(sourceBucketName, sourceKey, destinationBucketName, destinationKey);
    }

    @Override
    public void shutdown() {
        ossStorageClient.shutdown();
    }

    @Override
    public void refreshStsOssClient() throws CreateStsOssClientException {
        ossStorageClient.refreshStsOssClient();
    }

    @Override
    public boolean isUseStsOssClient() {
        return ossStorageClient.isStsOssClient();
    }

    @Override
    public InitiateMultipartUploadResult initiateMultipartUpload(InitiateMultipartUploadRequest request) {
        return ossStorageClient.initiateMultipartUpload(request);
    }

    @Override
    public UploadPartResult uploadPart(UploadPartRequest request) {
        return ossStorageClient.uploadPart(request);
    }

    @Override
    public boolean abortMultipartUpload(AbortMultipartUploadRequest request) {
        ossStorageClient.abortMultipartUpload(request);
        return true;
    }

    @Override
    public CompleteMultipartUploadResult completeMultipartUpload(CompleteMultipartUploadRequest request) {
        return ossStorageClient.completeMultipartUpload(request);
    }
}
