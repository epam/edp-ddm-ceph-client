package com.epam.digital.data.platform.integration.ceph.service.impl;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.DeleteObjectsRequest.KeyVersion;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.epam.digital.data.platform.integration.ceph.service.BaseCephService;
import com.epam.digital.data.platform.integration.ceph.service.S3ObjectCephService;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Service
public class S3ObjectCephServiceImpl extends BaseCephService implements S3ObjectCephService {

  private final String cephBucketName;
  private final AmazonS3 cephAmazonS3;

  @Autowired
  public S3ObjectCephServiceImpl(@Value("${ceph.bucket}") String cephBucketName,
      AmazonS3 cephAmazonS3) {
    this.cephBucketName = cephBucketName;
    this.cephAmazonS3 = cephAmazonS3;
  }

  @Override
  public ObjectMetadata put(String key, String contentType, Map<String, String> userMetadata,
      InputStream fileInputStream) {
    assertBucketExists(cephAmazonS3, cephBucketName);
    return execute(() -> {
      var objectMetadata = new ObjectMetadata();
      objectMetadata.setContentType(contentType);
      objectMetadata.setUserMetadata(userMetadata);
      cephAmazonS3.putObject(cephBucketName, key, fileInputStream, objectMetadata);
      return cephAmazonS3.getObjectMetadata(cephBucketName, key);
    });
  }

  @Override
  public Optional<S3Object> get(String key) {
    assertBucketExists(cephAmazonS3, cephBucketName);
    var doesContentExist = execute(() -> cephAmazonS3.doesObjectExist(cephBucketName, key));
    if (!doesContentExist) {
      return Optional.empty();
    }
    return execute(() -> Optional.of(cephAmazonS3.getObject(cephBucketName, key)));
  }

  @Override
  public Optional<List<ObjectMetadata>> getMetadata(List<String> keys) {
    assertBucketExists(cephAmazonS3, cephBucketName);
    var doesContentExist = execute(
        () -> keys.stream().allMatch(k -> cephAmazonS3.doesObjectExist(cephBucketName, k)));
    if (!doesContentExist) {
      return Optional.empty();
    }
    return execute(() -> Optional.of(keys.stream()
        .map(k -> cephAmazonS3.getObjectMetadata(cephBucketName, k)).collect(Collectors.toList())));
  }

  @Override
  public void delete(List<String> keys) {
    assertBucketExists(cephAmazonS3, cephBucketName);
    executeRunnable(() -> {
      var keyVersions = keys.stream().map(KeyVersion::new).collect(Collectors.toList());
      var deleteObjectsRequest = new DeleteObjectsRequest(cephBucketName).withKeys(keyVersions);
      cephAmazonS3.deleteObjects(deleteObjectsRequest);
    });
  }

  @Override
  public Boolean exist(List<String> keys) {
    assertBucketExists(cephAmazonS3, cephBucketName);
    return execute(
        () -> keys.stream().allMatch(k -> cephAmazonS3.doesObjectExist(cephBucketName, k)));
  }

  @Override
  public List<String> getKeys(String prefix) {
    assertBucketExists(cephAmazonS3, cephBucketName);
    return execute(
        () -> cephAmazonS3.listObjects(cephBucketName, prefix).getObjectSummaries().stream()
            .map(S3ObjectSummary::getKey).collect(Collectors.toList())
    );
  }
}
