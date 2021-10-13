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
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Slf4j
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
    log.info("Putting file with key {} to ceph bucket {}", key, cephBucketName);
    assertBucketExists(cephAmazonS3, cephBucketName);
    var result = execute(() -> {
      var objectMetadata = new ObjectMetadata();
      objectMetadata.setContentType(contentType);
      objectMetadata.setUserMetadata(userMetadata);
      cephAmazonS3.putObject(cephBucketName, key, fileInputStream, objectMetadata);
      return cephAmazonS3.getObjectMetadata(cephBucketName, key);
    });
    log.info("File {} was put to ceph bucket {}", key, cephBucketName);
    return result;
  }

  @Override
  public Optional<S3Object> get(String key) {
    log.info("Getting file with key {} from ceph bucket {}", key, cephBucketName);
    assertBucketExists(cephAmazonS3, cephBucketName);
    var doesContentExist = execute(() -> cephAmazonS3.doesObjectExist(cephBucketName, key));
    if (!doesContentExist) {
      log.info("File {} wasn't found in ceph bucket {}", key, cephBucketName);
      return Optional.empty();
    }
    var result = execute(() -> Optional.of(cephAmazonS3.getObject(cephBucketName, key)));
    log.info("File {} was found in ceph bucket {}", key, cephBucketName);
    return result;
  }

  @Override
  public Optional<List<ObjectMetadata>> getMetadata(List<String> keys) {
    log.info("Getting file metadata for keys {} from ceph bucket {}", keys, cephBucketName);
    assertBucketExists(cephAmazonS3, cephBucketName);
    var doesContentExist = execute(
        () -> keys.stream().allMatch(k -> cephAmazonS3.doesObjectExist(cephBucketName, k)));
    if (!doesContentExist) {
      log.info("One of the files {} wasn't found in ceph bucket {}", keys, cephBucketName);
      return Optional.empty();
    }
    var result = execute(() -> Optional.of(keys.stream()
        .map(k -> cephAmazonS3.getObjectMetadata(cephBucketName, k)).collect(Collectors.toList())));
    log.info("Files metadata {} was found in ceph bucket {}", keys, cephBucketName);
    return result;
  }

  @Override
  public void delete(List<String> keys) {
    log.info("Deleting files with keys {} from ceph bucket {}", keys, cephBucketName);
    assertBucketExists(cephAmazonS3, cephBucketName);
    executeRunnable(() -> {
      var keyVersions = keys.stream().map(KeyVersion::new).collect(Collectors.toList());
      var deleteObjectsRequest = new DeleteObjectsRequest(cephBucketName).withKeys(keyVersions);
      cephAmazonS3.deleteObjects(deleteObjectsRequest);
    });
    log.info("Files {} was deleted from ceph bucket {}", keys, cephBucketName);
  }

  @Override
  public Boolean exist(List<String> keys) {
    log.info("Checking if all files with keys {} exist in ceph bucket {}", keys, cephBucketName);
    assertBucketExists(cephAmazonS3, cephBucketName);
    var result = execute(
        () -> keys.stream().allMatch(k -> cephAmazonS3.doesObjectExist(cephBucketName, k)));
    log.info("All files {} existing in ceph bucket {} - {}", keys, cephBucketName, result);
    return result;
  }

  @Override
  public List<String> getKeys(String prefix) {
    log.info("Getting all ceph keys with prefix {} from ceph bucket {}", prefix, cephBucketName);
    assertBucketExists(cephAmazonS3, cephBucketName);
    var result = execute(
        () -> cephAmazonS3.listObjects(cephBucketName, prefix).getObjectSummaries().stream()
            .map(S3ObjectSummary::getKey).collect(Collectors.toList())
    );
    log.info("Found {} keys for prefix {} in ceph bucket {}", result.size(), prefix,
        cephBucketName);
    return result;
  }
}
