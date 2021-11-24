package org.elasticsearch.aliyun.oss.blobstore;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.aliyun.oss.ClientException;
import com.aliyun.oss.OSSException;
import com.aliyun.oss.model.AbortMultipartUploadRequest;
import com.aliyun.oss.model.CompleteMultipartUploadRequest;
import com.aliyun.oss.model.DeleteObjectsRequest;
import com.aliyun.oss.model.InitiateMultipartUploadRequest;
import com.aliyun.oss.model.ListObjectsRequest;
import com.aliyun.oss.model.OSSObjectSummary;
import com.aliyun.oss.model.ObjectListing;
import com.aliyun.oss.model.ObjectMetadata;
import com.aliyun.oss.model.PartETag;
import com.aliyun.oss.model.UploadPartRequest;
import com.aliyun.oss.model.UploadPartResult;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.aliyun.oss.service.OssService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobMetadata;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStore;
import org.elasticsearch.common.blobstore.BlobStoreException;
import org.elasticsearch.common.blobstore.DeleteResult;
import org.elasticsearch.common.blobstore.support.PlainBlobMetadata;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.repositories.blobstore.ChunkedBlobOutputStream;
import org.elasticsearch.utils.PermissionHelper;

/**
 * An oss blob store for managing oss client write and read blob directly
 * Created by yangkongshi on 2017/11/24.
 */
public class OssBlobStore implements BlobStore {
    private static final Logger logger = LogManager.getLogger(OssBlobStore.class);

    private static final int MAX_BULK_DELETES = 1000;

    private final OssService client;
    private final String bucket;
    private final BigArrays bigArrays;
    private final ByteSizeValue bufferSize;

    public OssBlobStore(String bucket, OssService client, BigArrays bigArrays, ByteSizeValue bufferSize) {
        this.client = client;
        this.bucket = bucket;
        this.bigArrays = bigArrays;
        this.bufferSize = bufferSize;
        if (!doesBucketExist(bucket)) {
            throw new BlobStoreException("bucket does not exist");
        }
    }

    public BigArrays getBigArrays() {
        return bigArrays;
    }

    public String getBucket() {
        return this.bucket;
    }

    @Override
    public BlobContainer blobContainer(BlobPath blobPath) {
        return new OssBlobContainer(blobPath, this);
    }

    public void delete(BlobPath blobPath) throws IOException {
        DeleteObjectsRequest deleteRequest = new DeleteObjectsRequest(bucket);
        Map<String, BlobMetadata> blobs = listBlobsByPrefix(blobPath.buildAsString(), null);
        List<String> toBeDeletedBlobs = new ArrayList<>();
        Iterator<String> blobNameIterator = blobs.keySet().iterator();
        while (blobNameIterator.hasNext()) {
            String blobName = blobNameIterator.next();
            toBeDeletedBlobs.add(blobPath.buildAsString() + blobName);
            if (toBeDeletedBlobs.size() > DeleteObjectsRequest.DELETE_OBJECTS_ONETIME_LIMIT / 2
                    || !blobNameIterator.hasNext()) {
                deleteRequest.setKeys(toBeDeletedBlobs);
                deleteObjects(deleteRequest);
                toBeDeletedBlobs.clear();
            }
        }
    }

    @Override
    public void close() {
        client.shutdown();
    }

    /**
     * Return true if the given bucket exists
     *
     * @param bucketName name of the bucket
     * @return true if the bucket exists, false otherwise
     */
    boolean doesBucketExist(String bucketName) {
        try {
            return doPrivilegedAndRefreshClient(() -> this.client.doesBucketExist(bucketName));
        } catch (IOException e) {
            throw new BlobStoreException("do privileged has failed", e);
        }
    }

    /**
     * List all blobs in the bucket which have a prefix
     *
     * @param prefix prefix of the blobs to list
     * @return a map of blob names and their metadata
     */
    Map<String, BlobMetadata> listBlobsByPrefix(String keyPath, String prefix) throws IOException {
        MapBuilder<String, BlobMetadata> blobsBuilder = MapBuilder.newMapBuilder();
        String actualPrefix = keyPath + (prefix == null ? "" : prefix);
        String nextMarker = null;
        ObjectListing blobs;
        do {
            blobs = listBlobs(actualPrefix, nextMarker);
            for (OSSObjectSummary summary : blobs.getObjectSummaries()) {
                String blobName = summary.getKey().substring(keyPath.length());
                blobsBuilder.put(blobName, new PlainBlobMetadata(blobName, summary.getSize()));
            }
            nextMarker = blobs.getNextMarker();
        } while (blobs.isTruncated());
        return blobsBuilder.immutableMap();
    }

    /**
     * list blob with privilege check
     *
     * @param actualPrefix actual prefix of the blobs to list
     * @param nextMarker   blobs next marker
     * @return {@link ObjectListing}
     */
    ObjectListing listBlobs(String actualPrefix, String nextMarker) throws IOException {
        return doPrivilegedAndRefreshClient(() -> this.client.listObjects(
                new ListObjectsRequest(bucket).withPrefix(actualPrefix).withMarker(nextMarker)
        ));
    }

    /**
     * Delete Objects
     *
     * @param deleteRequest {@link DeleteObjectsRequest}
     */
    void deleteObjects(DeleteObjectsRequest deleteRequest) throws IOException {
        doPrivilegedAndRefreshClient(() -> this.client.deleteObjects(deleteRequest));
    }

    /**
     * Returns true if the blob exists in the bucket
     *
     * @param blobName name of the blob
     * @return true if the blob exists, false otherwise
     */
    boolean blobExists(String blobName) throws OSSException, ClientException, IOException {
        return doPrivilegedAndRefreshClient(() -> this.client.doesObjectExist(bucket, blobName));
    }

    /**
     * Returns an {@link java.io.InputStream} for a given blob
     *
     * @param blobName name of the blob
     * @return an InputStream
     */
    InputStream readBlob(String blobName) throws OSSException, ClientException, IOException {
        return doPrivilegedAndRefreshClient(() -> this.client.getObject(bucket, blobName).getObjectContent());
    }

    public InputStream readBlob(String blobName, long position, long length) throws IOException {
        InputStream fullStream = doPrivilegedAndRefreshClient(() -> this.client.getObject(bucket, blobName).getObjectContent());
        fullStream.skip(position);
        return new LimitedSizeInputStream(fullStream, length);
    }

    /**
     * Writes a blob in the bucket.
     *
     * @param inputStream content of the blob to be written
     * @param blobSize    expected size of the blob to be written
     */
    void writeBlob(String blobName, InputStream inputStream, long blobSize)
            throws OSSException, ClientException, IOException {
        ObjectMetadata meta = new ObjectMetadata();
        meta.setContentLength(blobSize);
        doPrivilegedAndRefreshClient(() -> this.client.putObject(bucket, blobName, inputStream, meta));
    }

    /**
     * Deletes a blob in the bucket
     *
     * @param blobName name of the blob
     */
    void deleteBlob(String blobName) throws OSSException, ClientException, IOException {
        doPrivilegedAndRefreshClient(() -> {
            this.client.deleteObject(bucket, blobName);
            return null;
        });
    }

    public void move(String sourceBlobName, String targetBlobName)
            throws OSSException, ClientException, IOException {
        doPrivilegedAndRefreshClient(() -> {
            this.client.copyObject(bucket, sourceBlobName, bucket, targetBlobName);
            return null;
        });
        doPrivilegedAndRefreshClient(() -> {
            this.client.deleteObject(bucket, sourceBlobName);
            return null;
        });
    }

    /**
     * Executes a {@link PrivilegedExceptionAction} with privileges enabled.
     */
    <T> T doPrivilegedAndRefreshClient(PrivilegedExceptionAction<T> operation) throws IOException {
        refreshStsOssClient();
        return PermissionHelper.doPrivileged(operation);
    }

    private void refreshStsOssClient() throws IOException {
        if (this.client.isUseStsOssClient()) {
            this.client.refreshStsOssClient();//refresh token to avoid expired
        }
    }

    public long bufferSizeInBytes() {
        return bufferSize.getBytes();
    }

    public void writeBlob(String blobName, boolean failIfAlreadyExists, boolean atomic, CheckedConsumer<OutputStream, IOException> writer, OssBlobContainer ossBlobContainer) throws IOException {
        try (
                ChunkedBlobOutputStream<PartETag> out = new ChunkedBlobOutputStream<PartETag>(bigArrays, bufferSizeInBytes()) {

                    private final SetOnce<String> uploadId = new SetOnce<>();

                    @Override
                    protected void flushBuffer() throws IOException {
                        flushBuffer(false);
                    }

                    private void flushBuffer(boolean lastPart) throws IOException {
                        if (buffer.size() == 0) {
                            return;
                        }
                        if (flushedBytes == 0L) {
                            assert lastPart == false : "use single part upload if there's only a single part";
                            uploadId.set(doPrivilegedAndRefreshClient(() ->
                                    client.initiateMultipartUpload(initiateMultiPartUpload(blobName))
                                            .getUploadId())
                            );
                            if (Strings.isEmpty(uploadId.get())) {
                                throw new IOException("Failed to initialize multipart upload " + blobName);
                            }
                        }
                        assert lastPart == false || successful : "must only write last part if successful";
                        final UploadPartRequest uploadRequest = createPartUploadRequest(
                                buffer.bytes().streamInput(),
                                uploadId.get(),
                                parts.size() + 1,
                                blobName,
                                buffer.size()
                        );
                        final UploadPartResult uploadResponse =
                                doPrivilegedAndRefreshClient(() -> client.uploadPart(uploadRequest));
                        finishPart(uploadResponse.getPartETag());
                    }

                    @Override
                    protected void onCompletion() throws IOException {
                        if (flushedBytes == 0L) {
                            ossBlobContainer.writeBlob(blobName, buffer.bytes(), failIfAlreadyExists);
                        } else {
                            flushBuffer(true);
                            final CompleteMultipartUploadRequest completeRequest = new CompleteMultipartUploadRequest(
                                    bucket,
                                    blobName,
                                    uploadId.get(),
                                    parts
                            );
                            doPrivilegedAndRefreshClient(() -> client.completeMultipartUpload(completeRequest));
                        }
                    }

                    @Override
                    protected void onFailure() {
                        if (Strings.hasText(uploadId.get())) {
                            abortMultiPartUpload(uploadId.get(), blobName);
                        }
                    }
                }
        ) {
            writer.accept(out);
            out.markSuccess();
        }
    }

    private InitiateMultipartUploadRequest initiateMultiPartUpload(String blobName) {
        return new InitiateMultipartUploadRequest(bucket, blobName);
    }

    private void abortMultiPartUpload(String uploadId, String blobName) {
        final AbortMultipartUploadRequest abortRequest = new AbortMultipartUploadRequest(bucket, blobName, uploadId);
        try {
            doPrivilegedAndRefreshClient(() -> client.abortMultipartUpload(abortRequest));
        } catch (IOException ex) {
            logger.error(ex);
        }
    }

    private UploadPartRequest createPartUploadRequest(
            InputStream stream,
            String uploadId,
            int number,
            String blobName,
            long size
    ) {
        final UploadPartRequest uploadRequest = new UploadPartRequest();
        uploadRequest.setBucketName(bucket);
        uploadRequest.setKey(blobName);
        uploadRequest.setUploadId(uploadId);
        uploadRequest.setPartNumber(number);
        uploadRequest.setInputStream(stream);
        uploadRequest.setPartSize(size);
        return uploadRequest;
    }

    public DeleteResult delete(String keyPath, OssBlobContainer ossBlobContainer) throws IOException {
        final AtomicLong deletedBlobs = new AtomicLong();
        final AtomicLong deletedBytes = new AtomicLong();
        try {
            ObjectListing prevListing = null;
            while (true) {
                ObjectListing list;
                if (prevListing != null) {
                    final ListObjectsRequest listObjectsRequest = new ListObjectsRequest();
                    listObjectsRequest.setBucketName(prevListing.getBucketName());
                    listObjectsRequest.setPrefix(prevListing.getPrefix());
                    listObjectsRequest.setDelimiter(prevListing.getDelimiter());
                    listObjectsRequest.setMarker(prevListing.getNextMarker());
                    listObjectsRequest.setMaxKeys(prevListing.getMaxKeys());
                    list = doPrivilegedAndRefreshClient(() -> client.listObjects(listObjectsRequest));
                } else {
                    final ListObjectsRequest listObjectsRequest = new ListObjectsRequest();
                    listObjectsRequest.setBucketName(bucket);
                    listObjectsRequest.setPrefix(keyPath);
                    list = doPrivilegedAndRefreshClient(() -> client.listObjects(listObjectsRequest));
                }
                final Iterator<OSSObjectSummary> objectSummaryIterator = list.getObjectSummaries().iterator();
                final Iterator<String> blobNameIterator = new Iterator<String>() {
                    @Override
                    public boolean hasNext() {
                        return objectSummaryIterator.hasNext();
                    }

                    @Override
                    public String next() {
                        final OSSObjectSummary summary = objectSummaryIterator.next();
                        deletedBlobs.incrementAndGet();
                        deletedBytes.addAndGet(summary.getSize());
                        return summary.getKey();
                    }
                };
                if (list.isTruncated()) {
                    doDeleteBlobs(blobNameIterator, false, ossBlobContainer);
                    prevListing = list;
                } else {
                    doDeleteBlobs(Iterators.concat(blobNameIterator, Collections.singletonList(keyPath).iterator()), false, ossBlobContainer);
                    break;
                }
            }
        } catch (final Exception e) {
            throw new IOException("Exception when deleting blob container [" + keyPath + "]", e);
        }
        return new DeleteResult(deletedBlobs.get(), deletedBytes.get());
    }

    private void doDeleteBlobs(Iterator<String> blobNames, boolean relative, OssBlobContainer ossBlobContainer) throws IOException {
        if (!blobNames.hasNext()) {
            return;
        }
        final Iterator<String> outstanding;
        if (relative) {
            outstanding = new Iterator<String>() {
                @Override
                public boolean hasNext() {
                    return blobNames.hasNext();
                }

                @Override
                public String next() {
                    return ossBlobContainer.buildKey(blobNames.next());
                }
            };
        } else {
            outstanding = blobNames;
        }

        final List<String> partition = new ArrayList<>();
        try {
            final AtomicReference<Exception> aex = new AtomicReference<>();
            doPrivilegedAndRefreshClient(() -> {
                outstanding.forEachRemaining(key -> {
                    partition.add(key);
                    if (partition.size() == MAX_BULK_DELETES) {
                        deletePartition(partition, aex);
                        partition.clear();
                    }
                });
                if (partition.isEmpty() == false) {
                    deletePartition(partition, aex);
                }
                return true;
            });
            if (aex.get() != null) {
                throw aex.get();
            }
        } catch (Exception e) {
            throw new IOException("Failed to delete blobs " + partition.stream().limit(10).collect(Collectors.toList()), e);
        }
    }

    private void deletePartition(List<String> partition, AtomicReference<Exception> aex) {
        try {
            client.deleteObjects(bulkDelete(bucket, partition));
        } catch (Exception e) {
            aex.set(ExceptionsHelper.useOrSuppress(aex.get(), e));
        }
    }

    private static DeleteObjectsRequest bulkDelete(String bucket, List<String> blobs) {
        return new DeleteObjectsRequest(bucket)
                .withKeys(blobs)
                .withQuiet(true);
    }

    public Map<String, BlobContainer> children(BlobPath path, String keyPath) throws IOException {
        try {
            return executeListing(generateListObjectsRequest(keyPath)).stream().flatMap(listing -> {
                        assert listing.getObjectSummaries().stream().noneMatch(s -> {
                            for (String commonPrefix : listing.getCommonPrefixes()) {
                                if (s.getKey().substring(keyPath.length()).startsWith(commonPrefix)) {
                                    return true;
                                }
                            }
                            return false;
                        }) : "Response contained children for listed common prefixes.";
                        return listing.getCommonPrefixes().stream();
                    })
                    .map(prefix -> prefix.substring(keyPath.length()))
                    .filter(name -> name.isEmpty() == false)
                    // Stripping the trailing slash off of the common prefix
                    .map(name -> name.substring(0, name.length() - 1))
                    .collect(Collectors.toMap(Function.identity(), name -> blobContainer(path.add(name))));
        } catch (final Exception e) {
            throw new IOException("Exception when listing children of [" + path.buildAsString() + ']', e);
        }
    }

    private ListObjectsRequest generateListObjectsRequest(String keyPath) {
        ListObjectsRequest request = new ListObjectsRequest();
        request.setBucketName(bucket);
        request.setPrefix(keyPath);
        request.setDelimiter("/");
        return request;
    }

    private List<ObjectListing> executeListing(ListObjectsRequest listObjectsRequest) throws IOException {
        final List<ObjectListing> results = new ArrayList<>();
        ObjectListing prevListing = null;
        while (true) {
            ObjectListing list;
            if (prevListing != null) {
                listObjectsRequest.setBucketName(prevListing.getBucketName());
                listObjectsRequest.setPrefix(prevListing.getPrefix());
                listObjectsRequest.setDelimiter(prevListing.getDelimiter());
                listObjectsRequest.setMarker(prevListing.getNextMarker());
                listObjectsRequest.setMaxKeys(prevListing.getMaxKeys());
                list = doPrivilegedAndRefreshClient(() -> client.listObjects(listObjectsRequest));
            } else {
                list = doPrivilegedAndRefreshClient(() -> client.listObjects(listObjectsRequest));
            }
            results.add(list);
            if (list.isTruncated()) {
                prevListing = list;
            } else {
                break;
            }
        }
        return results;
    }

    static class LimitedSizeInputStream extends InputStream {

        private final InputStream original;
        private final long maxSize;
        private long total;

        public LimitedSizeInputStream(InputStream original, long maxSize) {
            this.original = original;
            this.maxSize = maxSize;
        }

        @Override
        public int read() throws IOException {
            int bytesRead = original.read();
            if (bytesRead >= 0 && incrementCounter(1) < 0) {
                return -1;
            }
            return bytesRead;
        }

        @Override
        public int read(byte b[]) throws IOException {
            return read(b, 0, b.length);
        }

        @Override
        public int read(byte b[], int off, int len) throws IOException {
            int bytesRead = original.read(b, off, len);
            if (bytesRead >= 0) {
                long bytesLeft = incrementCounter(bytesRead);
                if (bytesLeft < 0) {
                    bytesRead = bytesRead + Math.toIntExact(bytesLeft);
                }
            }
            return bytesRead;
        }

        private long incrementCounter(int size) {
            total += size;
            return maxSize - total;
        }
    }
}
