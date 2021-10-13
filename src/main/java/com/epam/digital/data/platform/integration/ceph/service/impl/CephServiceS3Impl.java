package com.epam.digital.data.platform.integration.ceph.service.impl;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.util.IOUtils;
import com.epam.digital.data.platform.integration.ceph.dto.CephObject;
import com.epam.digital.data.platform.integration.ceph.exception.CephCommunicationException;
import com.epam.digital.data.platform.integration.ceph.service.BaseCephService;
import com.epam.digital.data.platform.integration.ceph.service.CephService;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Optional;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class CephServiceS3Impl extends BaseCephService implements CephService {

  private final AmazonS3 cephAmazonS3;

  @Override
  public void putContent(String cephBucketName, String key, String content) {
    log.info("Putting content with key {} to ceph bucket {}", key, cephBucketName);
    assertBucketExists(cephAmazonS3, cephBucketName);
    execute(() -> cephAmazonS3.putObject(cephBucketName, key, content));
    log.info("Content {} was put to ceph bucket {}", key, cephBucketName);
  }

  @Override
  public void putObject(String cephBucketName, String key, CephObject cephObject) {
    log.info("Putting object with key {} to ceph bucket {}", key, cephBucketName);
    assertBucketExists(cephAmazonS3, cephBucketName);
    execute(
        () -> {
          var objectMetadata = new ObjectMetadata();
          objectMetadata.setUserMetadata(cephObject.getMetadata());
          return cephAmazonS3.putObject(
              new PutObjectRequest(
                  cephBucketName,
                  key,
                  new ByteArrayInputStream(cephObject.getContent()),
                  objectMetadata));
        });
    log.info("Object {} was put to ceph bucket {}", key, cephBucketName);
  }

  @Override
  public Optional<String> getContent(String cephBucketName, String key) {
    log.info("Getting content with key {} from ceph bucket {}", key, cephBucketName);
    assertBucketExists(cephAmazonS3, cephBucketName);
    var doesContentExist = execute(() -> cephAmazonS3.doesObjectExist(cephBucketName, key));
    if (Boolean.FALSE.equals(doesContentExist)) {
      log.warn("Content {} wasn't found in ceph bucket {}", key, cephBucketName);
      return Optional.empty();
    }
    var result = execute(() -> Optional.of(cephAmazonS3.getObjectAsString(cephBucketName, key)));
    log.info("Content {} was found in ceph bucket {}", key, cephBucketName);
    return result;
  }

  @Override
  public Optional<CephObject> getObject(String cephBucketName, String key) {
    log.info("Getting object with key {} from ceph bucket {}", key, cephBucketName);
    assertBucketExists(cephAmazonS3, cephBucketName);
    var doesContentExist = execute(() -> cephAmazonS3.doesObjectExist(cephBucketName, key));
    if (Boolean.FALSE.equals(doesContentExist)) {
      log.info("Object {} wasn't found in ceph bucket {}", key, cephBucketName);
      return Optional.empty();
    }
    var result = execute(
        () -> {
          try (var s3Object = cephAmazonS3.getObject(cephBucketName, key);
              var contentInputStream = s3Object.getObjectContent()) {
            var content = IOUtils.toByteArray(contentInputStream);
            return Optional.of(
                new CephObject(content, s3Object.getObjectMetadata().getUserMetadata()));
          } catch (IOException exception) {
            throw new CephCommunicationException(exception.getMessage(), exception);
          }
        });
    log.info("Object {} was found in ceph bucket {}", key, cephBucketName);
    return result;
  }

  @Override
  public void deleteObject(String cephBucketName, String key) {
    log.info("Deleting object with key {} from ceph bucket {}", key, cephBucketName);
    assertBucketExists(cephAmazonS3, cephBucketName);
    executeRunnable(() -> cephAmazonS3.deleteObject(cephBucketName, key));
    log.info("Content {} was deleted from ceph bucket {}", key, cephBucketName);
  }

  @Override
  public boolean doesObjectExist(String cephBucketName, String key) {
    log.info("Checking if object with key {} exists in ceph bucket {}", key, cephBucketName);
    assertBucketExists(cephAmazonS3, cephBucketName);
    var result = execute(() -> cephAmazonS3.doesObjectExist(cephBucketName, key));
    log.info("Object {} existing in ceph bucket {} - {}", key, cephBucketName, result);
    return result;
  }
}
