package org.elasticsearch.aliyun.oss.blobstore;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.NoSuchFileException;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;

import com.aliyun.oss.ClientException;
import com.aliyun.oss.OSSException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobMetadata;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStoreException;
import org.elasticsearch.common.blobstore.DeleteResult;
import org.elasticsearch.common.blobstore.support.AbstractBlobContainer;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.core.CheckedConsumer;

/**
 * A class for managing a oss repository of blob entries, where each blob entry is just a named group of bytes
 * Created by yangkongshi on 2017/11/24.
 */
public class OssBlobContainer extends AbstractBlobContainer {
    private static final Logger logger = LogManager.getLogger(OssBlobContainer.class);
    protected final OssBlobStore blobStore;
    protected final String keyPath;

    public OssBlobContainer(BlobPath path, OssBlobStore blobStore) {
        super(path);
        this.keyPath = path.buildAsString();
        this.blobStore = blobStore;
    }

    /**
     * Tests whether a blob with the given blob name exists in the container.
     *
     * @param blobName The name of the blob whose existence is to be determined.
     * @return {@code true} if a blob exists in the BlobContainer with the given name, and {@code false} otherwise.
     */
    @Override
    public boolean blobExists(String blobName) {
        logger.trace("blobExists({})", blobName);
        boolean result = false;
        try {
            result = blobStore.blobExists(buildKey(blobName));
        } catch (OSSException | ClientException | IOException e) {
            logger.warn("can not access [{}] : {}", blobName, e.getMessage());
            throw new BlobStoreException("Failed to check if blob [" + blobName + "] exists", e);
        }
        logger.trace("blobExists({}) result={}", blobName, result);
        return result;
    }

    /**
     * Creates a new {@link InputStream} for the given blob name.
     *
     * @param blobName The name of the blob to get an {@link InputStream} for.
     * @return The {@code InputStream} to read the blob.
     * @throws NoSuchFileException if the blob does not exist
     * @throws IOException         if the blob can not be read.
     */
    @Override
    public InputStream readBlob(String blobName) throws IOException {
        logger.trace("readBlob({})", blobName);
        if (!blobExists(blobName)) {
            throw new NoSuchFileException("[" + blobName + "] blob not found");
        }
        return blobStore.readBlob(buildKey(blobName));
    }

    @Override
    public InputStream readBlob(String blobName, long position, long length) throws IOException {
        logger.trace("readBlob({},{},{})", blobName, position, length);
        if (!blobExists(blobName)) {
            throw new NoSuchFileException("[" + blobName + "] blob not found");
        }
        if (position < 0L) {
            throw new IllegalArgumentException("position must be non-negative");
        }
        if (length < 0) {
            throw new IllegalArgumentException("length must be non-negative");
        }
        if (length == 0) {
            return new ByteArrayInputStream(new byte[0]);
        } else {
            return blobStore.readBlob(buildKey(blobName), position, length);
        }
    }

    @Override
    public long readBlobPreferredLength() {
        logger.trace("readBlobPreferredLength()");
        return super.readBlobPreferredLength();
    }

    /**
     * Reads blob content from the input stream and writes it to the container in a new blob with the given name.
     * This method assumes the container does not already contain a blob of the same blobName.  If a blob by the
     * same name already exists, the operation will fail and an {@link IOException} will be thrown.
     *
     * @param blobName            The name of the blob to write the contents of the input stream to.
     * @param inputStream         The input stream from which to retrieve the bytes to write to the blob.
     * @param blobSize            The size of the blob to be written, in bytes.  It is implementation dependent whether
     *                            this value is used in writing the blob to the repository.
     * @param failIfAlreadyExists whether to throw a FileAlreadyExistsException if the given blob already exists
     * @throws FileAlreadyExistsException if failIfAlreadyExists is true and a blob by the same name already exists
     * @throws IOException                if the input stream could not be read, or the target blob could not be written to.
     */
    @Override
    public void writeBlob(String blobName, InputStream inputStream, long blobSize, boolean failIfAlreadyExists) throws IOException {
        logger.trace("writeBlob({},{},{},{})", blobName, inputStream, blobSize, failIfAlreadyExists);
        if (blobExists(blobName)) {
            if (failIfAlreadyExists) {
                throw new FileAlreadyExistsException(
                        "blob [" + blobName + "] already exists, cannot overwrite");
            } else {
                deleteBlobsIgnoringIfNotExists(Collections.singletonList(blobName).listIterator());
            }
        }
        blobStore.writeBlob(buildKey(blobName), inputStream, blobSize);
    }

    @Override
    public void writeBlob(String blobName, boolean failIfAlreadyExists, boolean atomic, CheckedConsumer<OutputStream, IOException> writer)
            throws IOException {
        logger.trace("writeBlob({},{},{},{})", blobName, failIfAlreadyExists, atomic, writer);
        blobStore.writeBlob(blobName, failIfAlreadyExists, atomic, writer, this, buildKey(blobName));
    }

    @Override
    public void writeBlobAtomic(String blobName, BytesReference bytes, boolean failIfAlreadyExists) throws IOException {
        logger.trace("writeBlobAtomic({}, {}, {})", blobName, bytes, failIfAlreadyExists);
        writeBlob(blobName, bytes, failIfAlreadyExists);
    }

    /**
     * Deletes a blob with giving name, if the blob exists.  If the blob does not exist, this method throws an
     * IOException.
     *
     * @param blobName The name of the blob to delete.
     * @throws NoSuchFileException if the blob does not exist
     * @throws IOException         if the blob exists but could not be deleted.
     */
    public void deleteBlob(String blobName) throws IOException {
        logger.trace("deleteBlob({})", blobName);
        if (!blobExists(blobName)) {
            throw new NoSuchFileException("Blob [" + blobName + "] does not exist");
        }
        try {
            blobStore.deleteBlob(buildKey(blobName));
        } catch (OSSException | ClientException e) {
            logger.warn("can not access [{}] : {}", blobName,
                    e.getMessage());
            throw new IOException(e);
        }
    }

    @Override
    public DeleteResult delete() throws IOException {
        logger.trace("delete()");
        DeleteResult result = blobStore.delete(keyPath, this);
        logger.trace("delete() blobsDeleted={} bytesDeleted={}",
                result == null ? "null" : result.blobsDeleted(),
                result == null ? "null" : result.bytesDeleted());
        return result;
    }

    @Override
    public void deleteBlobsIgnoringIfNotExists(Iterator<String> blobNames) throws IOException {
        logger.trace("deleteBlobsIgnoringIfNotExists(iterator)");
        for (Iterator<String> it = blobNames; it.hasNext(); ) {
            String blobName = it.next();
            try {
                logger.trace("deleteBlobsIgnoringIfNotExists(current={})", blobName);
                deleteBlob(blobName);
            } catch (IOException ex) {
                logger.warn(ex);
            }
        }
    }

    /**
     * Lists all blobs in the container.
     *
     * @return A map of all the blobs in the container.  The keys in the map are the names of the blobs and
     * the values are {@link BlobMetadata}, containing basic information about each blob.
     * @throws IOException if there were any failures in reading from the blob container.
     */
    @Override
    public Map<String, BlobMetadata> listBlobs() throws IOException {
        logger.trace("listBlobs()");
        Map<String, BlobMetadata> result = listBlobsByPrefix(null);
        logger.trace("listBlobs() result={}", result == null ? "null" : result.keySet());
        return result;
    }

    @Override
    public Map<String, BlobContainer> children() throws IOException {
        logger.trace("children()");
        Map<String, BlobContainer> result = blobStore.children(path(), keyPath);
        logger.trace("children() result={}", result == null ? "null" : result.keySet());
        return result;
    }

    /**
     * Lists all blobs in the container.
     *
     * @return A map of all the blobs in the container.  The keys in the map are the names of the blobs and
     * the values are {@link BlobMetadata}, containing basic information about each blob.
     * @throws IOException if there were any failures in reading from the blob container.
     */
    @Override
    public Map<String, BlobMetadata> listBlobsByPrefix(String blobNamePrefix)
            throws IOException {
        logger.trace("listBlobsByPrefix({})", blobNamePrefix);
        Map<String, BlobMetadata> result = null;
        try {
            result = blobStore.listBlobsByPrefix(keyPath, blobNamePrefix);
        } catch (IOException e) {
            logger.warn("can not access [{}] : {}", blobNamePrefix, e.getMessage());
            throw new IOException(e);
        }
        logger.trace("listBlobsByPrefix({} result={}", blobNamePrefix, result == null ? "null" : result.keySet());
        return result;
    }

    protected String buildKey(String blobName) {
        return keyPath + (blobName == null ? "" : blobName);
    }
}
