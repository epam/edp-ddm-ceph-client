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
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class CephServiceS3Impl extends BaseCephService implements CephService {

  private final AmazonS3 cephAmazonS3;

  @Override
  public void putContent(String cephBucketName, String key, String content) {
    assertBucketExists(cephAmazonS3, cephBucketName);
    execute(() -> cephAmazonS3.putObject(cephBucketName, key, content));
  }

  @Override
  public void putObject(String cephBucketName, String key, CephObject cephObject) {
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
  }

  @Override
  public Optional<String> getContent(String cephBucketName, String key) {
    assertBucketExists(cephAmazonS3, cephBucketName);
    var doesContentExist = execute(() -> cephAmazonS3.doesObjectExist(cephBucketName, key));
    if (Boolean.FALSE.equals(doesContentExist)) {
      return Optional.empty();
    }
    return execute(() -> Optional.of(cephAmazonS3.getObjectAsString(cephBucketName, key)));
  }

  @Override
  public Optional<CephObject> getObject(String cephBucketName, String key) {
    assertBucketExists(cephAmazonS3, cephBucketName);
    var doesContentExist = execute(() -> cephAmazonS3.doesObjectExist(cephBucketName, key));
    if (Boolean.FALSE.equals(doesContentExist)) {
      return Optional.empty();
    }
    return execute(
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
  }

  @Override
  public void deleteObject(String cephBucketName, String key) {
    assertBucketExists(cephAmazonS3, cephBucketName);
    executeRunnable(() -> cephAmazonS3.deleteObject(cephBucketName, key));
  }
}
