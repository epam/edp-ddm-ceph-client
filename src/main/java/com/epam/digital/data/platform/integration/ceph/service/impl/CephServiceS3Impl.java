/*
 * Copyright 2021 EPAM Systems.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.epam.digital.data.platform.integration.ceph.service.impl;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.DeleteObjectsRequest.KeyVersion;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.epam.digital.data.platform.integration.ceph.exception.CephCommunicationException;
import com.epam.digital.data.platform.integration.ceph.exception.MisconfigurationException;
import com.epam.digital.data.platform.integration.ceph.model.CephObject;
import com.epam.digital.data.platform.integration.ceph.model.CephObjectMetadata;
import com.epam.digital.data.platform.integration.ceph.service.CephService;
import java.io.InputStream;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CephServiceS3Impl implements CephService {

  private final AmazonS3 cephAmazonS3;

  @Builder
  public CephServiceS3Impl(String cephEndpoint, String cephAccessKey, String cephSecretKey) {
    var credentials = new AWSStaticCredentialsProvider(
        new BasicAWSCredentials(cephAccessKey, cephSecretKey));
    var clientConfig = new ClientConfiguration();
    clientConfig.setProtocol(Protocol.HTTP);
    cephAmazonS3 = AmazonS3ClientBuilder.standard()
        .withCredentials(credentials)
        .withClientConfiguration(clientConfig)
        .withEndpointConfiguration(new EndpointConfiguration(cephEndpoint, null))
        .withPathStyleAccessEnabled(true)
        .build();
  }

  public CephServiceS3Impl(AmazonS3 amazonS3) {
    this.cephAmazonS3 = amazonS3;
  }

  @Override
  public Optional<CephObject> get(String cephBucketName, String key) {
    log.info("Getting file with key {} from ceph bucket {}", key, cephBucketName);
    assertBucketExists(cephAmazonS3, cephBucketName);
    var doesContentExist = execute(() -> cephAmazonS3.doesObjectExist(cephBucketName, key));
    if (!doesContentExist) {
      log.info("File {} wasn't found in ceph bucket {}", key, cephBucketName);
      return Optional.empty();
    }
    var result = execute(() -> Optional.of(cephAmazonS3.getObject(cephBucketName, key)));
    log.info("File {} was found in ceph bucket {}", key, cephBucketName);
    return result.map(this::tpCephObject);
  }

  @Override
  public Optional<String> getAsString(String cephBucketName, String key) {
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
  public void put(String cephBucketName, String key, String content) {
    log.info("Putting content with key {} to ceph bucket {}", key, cephBucketName);
    assertBucketExists(cephAmazonS3, cephBucketName);
    execute(() -> cephAmazonS3.putObject(cephBucketName, key, content));
    log.info("Content {} was put to ceph bucket {}", key, cephBucketName);
  }

  @Override
  public CephObjectMetadata put(String cephBucketName, String key, String contentType,
      Map<String, String> userMetadata, InputStream content) {
    log.info("Putting file with key {} to ceph bucket {}", key, cephBucketName);
    assertBucketExists(cephAmazonS3, cephBucketName);
    var result = execute(() -> {
      var objectMetadata = new ObjectMetadata();
      objectMetadata.setContentType(contentType);
      objectMetadata.setUserMetadata(userMetadata);
      cephAmazonS3.putObject(cephBucketName, key, content, objectMetadata);
      return cephAmazonS3.getObjectMetadata(cephBucketName, key);
    });
    log.info("File {} was put to ceph bucket {}", key, cephBucketName);
    return toCephObjectMetadata(result);
  }


  @Override
  public void delete(String cephBucketName, Set<String> keys) {
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
  public Boolean exist(String cephBucketName, Set<String> keys) {
    log.info("Checking if all files with keys {} exist in ceph bucket {}", keys, cephBucketName);
    assertBucketExists(cephAmazonS3, cephBucketName);
    var result = execute(
        () -> keys.stream().allMatch(k -> cephAmazonS3.doesObjectExist(cephBucketName, k)));
    log.info("All files {} existing in ceph bucket {} - {}", keys, cephBucketName, result);
    return result;
  }

  @Override
  public Boolean exist(String cephBucketName, String key) {
    log.info("Checking if object with key {} exists in ceph bucket {}", key, cephBucketName);
    assertBucketExists(cephAmazonS3, cephBucketName);
    var result = execute(() -> cephAmazonS3.doesObjectExist(cephBucketName, key));
    log.info("Object {} existing in ceph bucket {} - {}", key, cephBucketName, result);
    return result;
  }

  @Override
  public Set<String> getKeys(String cephBucketName, String prefix) {
    log.info("Getting all ceph keys with prefix {} from ceph bucket {}", prefix, cephBucketName);
    assertBucketExists(cephAmazonS3, cephBucketName);
    var result = execute(
        () -> cephAmazonS3.listObjects(cephBucketName, prefix).getObjectSummaries().stream()
            .map(S3ObjectSummary::getKey).collect(Collectors.toSet())
    );
    log.info("Found {} keys for prefix {} in ceph bucket {}", result.size(), prefix,
        cephBucketName);
    return result;
  }

  @Override
  public Set<String> getKeys(String cephBucketName) {
    log.info("Getting all ceph keys from bucket {}", cephBucketName);
    assertBucketExists(cephAmazonS3, cephBucketName);
    var result = execute(
        () -> cephAmazonS3.listObjects(cephBucketName).getObjectSummaries().stream()
            .map(S3ObjectSummary::getKey).collect(Collectors.toSet()));
    log.info("Found {} keys from bucket {}", result.size(), cephBucketName);
    return result;
  }

  @Override
  public List<CephObjectMetadata> getMetadata(String cephBucketName, Set<String> keys) {
    log.info("Getting file metadata for keys {} from ceph bucket {}", keys, cephBucketName);
    assertBucketExists(cephAmazonS3, cephBucketName);
    var doesContentExist = execute(
        () -> keys.stream().allMatch(k -> cephAmazonS3.doesObjectExist(cephBucketName, k)));
    if (!doesContentExist) {
      log.info("One of the files {} wasn't found in ceph bucket {}", keys, cephBucketName);
      return Collections.emptyList();
    }
    var result = execute(() -> keys.stream()
        .map(k -> cephAmazonS3.getObjectMetadata(cephBucketName, k)).collect(Collectors.toList()));

    log.info("Files metadata {} was found in ceph bucket {}", keys, cephBucketName);
    return toCephObjectMetadataList(result);
  }

  private void assertBucketExists(AmazonS3 cephAmazonS3, String cephBucketName) {
    log.debug("Checking if bucket {} exists", cephBucketName);
    var buckets = execute(cephAmazonS3::listBuckets);
    buckets.stream()
        .filter(bucket -> bucket.getName().equals(cephBucketName))
        .findFirst()
        .orElseThrow(() -> new MisconfigurationException(
            String.format("Bucket %s hasn't found", cephBucketName)));
  }

  private <T> T execute(Supplier<T> supplier) {
    try {
      return supplier.get();
    } catch (RuntimeException exception) {
      throw new CephCommunicationException(exception.getMessage(), exception);
    }
  }

  private void executeRunnable(Runnable runnable) {
    try {
      runnable.run();
    } catch (RuntimeException exception) {
      throw new CephCommunicationException(exception.getMessage(), exception);
    }
  }

  private List<CephObjectMetadata> toCephObjectMetadataList(
      List<ObjectMetadata> objectMetadataList) {
    return objectMetadataList.stream().map(this::toCephObjectMetadata).collect(Collectors.toList());
  }

  private CephObjectMetadata toCephObjectMetadata(ObjectMetadata objectMetadata) {
    return CephObjectMetadata.builder()
        .contentType(objectMetadata.getContentType())
        .userMetadata(objectMetadata.getUserMetadata())
        .contentLength(objectMetadata.getContentLength())
        .build();
  }

  private CephObject tpCephObject(S3Object s3Object) {
    return CephObject.builder()
        .metadata(toCephObjectMetadata(s3Object.getObjectMetadata()))
        .content(s3Object.getObjectContent())
        .build();
  }
}
