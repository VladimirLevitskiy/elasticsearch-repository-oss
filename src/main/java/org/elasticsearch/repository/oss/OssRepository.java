package org.elasticsearch.repository.oss;

import java.io.File;

import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.aliyun.oss.blobstore.OssBlobContainer;
import org.elasticsearch.aliyun.oss.blobstore.OssBlobStore;
import org.elasticsearch.aliyun.oss.service.OssClientSettings;
import org.elasticsearch.aliyun.oss.service.OssService;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStore;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.env.Environment;
import org.elasticsearch.indices.recovery.RecoverySettings;
import org.elasticsearch.repositories.RepositoryException;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;

/**
 * An oss repository working for snapshot and restore.
 * Implementations are responsible for reading and writing both metadata and shard data to and from
 * a repository backend.
 * Created by yangkongshi on 2017/11/24.
 */
public class OssRepository extends BlobStoreRepository {

    private static final Logger logger = LogManager.getLogger(OssBlobContainer.class);

    public static final String TYPE = "oss";
    private final BlobPath basePath;
    private final ByteSizeValue bufferSize;
    private final ByteSizeValue chunkSize;
    private final String bucket;
    private final OssService ossService;
    private final Environment env;


    public OssRepository(RepositoryMetadata metadata, Environment env,
                         NamedXContentRegistry namedXContentRegistry, OssService ossService, ClusterService clusterService, BigArrays bigArrays, RecoverySettings recoverySettings) {
        super(metadata, getSetting(OssClientSettings.COMPRESS, metadata), namedXContentRegistry, clusterService, bigArrays, recoverySettings);
        this.env = env;
        this.ossService = ossService;
        String ecsRamRole = OssClientSettings.ECS_RAM_ROLE.get(metadata.settings()).toString();
        if (StringUtils.isNotEmpty(ecsRamRole)) {
            this.bucket = getSetting(OssClientSettings.AUTO_SNAPSHOT_BUCKET, metadata).toString();
        } else {
            this.bucket = getSetting(OssClientSettings.BUCKET, metadata);
        }
        String basePath = OssClientSettings.BASE_PATH.get(metadata.settings());
        if (Strings.hasLength(basePath)) {
            BlobPath path = BlobPath.EMPTY;
            for (String elem : basePath.split(File.separator)) {
                path = path.add(elem);
            }
            this.basePath = path;
        } else {
            this.basePath = BlobPath.EMPTY;
        }
        this.chunkSize = getSetting(OssClientSettings.CHUNK_SIZE, metadata);
        this.bufferSize = getSetting(OssClientSettings.BUFFER_SIZE, metadata);
        logger.info("Using base_path [{}], chunk_size [{}], compress [{}]",
                basePath, chunkSize, getSetting(OssClientSettings.COMPRESS, metadata));
    }

    @Override
    protected BlobStore createBlobStore() throws Exception {
        return new OssBlobStore(bucket, ossService, bigArrays, bufferSize);
    }

    @Override
    public BlobPath basePath() {
        return basePath;
    }


    @Override
    protected ByteSizeValue chunkSize() {
        return chunkSize;
    }

    public static <T> T getSetting(Setting<T> setting, RepositoryMetadata metadata) {
        T value = setting.get(metadata.settings());
        if (value == null) {
            throw new RepositoryException(metadata.name(),
                    "Setting [" + setting.getKey() + "] is not defined for repository");
        }
        if ((value instanceof String) && (Strings.hasText((String) value)) == false) {
            throw new RepositoryException(metadata.name(),
                    "Setting [" + setting.getKey() + "] is empty for repository");
        }
        return value;
    }

    /**
     * mainly for test
     **/
    public OssService getOssService() {
        return ossService;
    }
}
