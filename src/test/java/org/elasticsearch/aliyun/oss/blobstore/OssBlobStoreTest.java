//package org.elasticsearch.aliyun.oss.blobstore;
//
//import java.io.IOException;
//import java.util.Locale;
//
//import org.elasticsearch.common.blobstore.BlobStore;
//import org.elasticsearch.repositories.blobstore.ESBlobStoreRepositoryIntegTestCase;
//import org.elasticsearch.repository.oss.OssRepository;
//
///**
// * Created by yangkongshi on 2017/11/28.
// */
//public class OssBlobStoreTest extends ESBlobStoreRepositoryIntegTestCase {
//
//    @Override
//    protected String repositoryType() {
//        return OssRepository.TYPE;
//    }
//
//    @Override
//    protected BlobStore newBlobStore() {
//        String bucket = randomAlphaOfLength(randomIntBetween(1, 10)).toLowerCase(Locale.ROOT);
//        MockOssService client = new MockOssService();
//        return new OssBlobStore(bucket, client);
//    }
//}
