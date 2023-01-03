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

package com.epam.digital.data.platform.integration.ceph.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.epam.digital.data.platform.integration.ceph.exception.CephCommunicationException;
import com.epam.digital.data.platform.integration.ceph.exception.MisconfigurationException;
import com.epam.digital.data.platform.integration.ceph.service.impl.CephServiceS3Impl;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import lombok.SneakyThrows;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class CephServiceS3ImplTest {

  @Mock
  private AmazonS3 amazonS3;
  @Mock
  private ObjectListing objectListing;

  private CephServiceS3Impl cephServiceS3;

  @BeforeEach
  public void before() {
    cephServiceS3 = new CephServiceS3Impl(amazonS3);
  }

  @Test
  void readContentFromNonExistingBucket() {
    var bucketName = "bucket";
    var contentKey = "key";

    when(amazonS3.listBuckets()).thenReturn(Collections.emptyList());

    assertThrows(MisconfigurationException.class,
        () -> cephServiceS3.getAsString(bucketName, contentKey));
  }

  @Test
  void readContentWithCommunicationIssues() {
    var bucketName = "bucket";
    var contentKey = "key";

    when(amazonS3.listBuckets()).thenThrow(new RuntimeException());

    assertThrows(CephCommunicationException.class,
        () -> cephServiceS3.getAsString(bucketName, contentKey));
  }

  @Test
  void readContentAsString() {
    var bucketName = "bucket";
    var contentKey = "key";
    var content = "key";

    when(amazonS3.listBuckets()).thenReturn(Collections.singletonList(new Bucket(bucketName)));
    when(amazonS3.doesObjectExist(bucketName, contentKey)).thenReturn(true);
    when(amazonS3.getObjectAsString(bucketName, contentKey)).thenReturn(content);

    var result = cephServiceS3.getAsString(bucketName, contentKey);

    assertThat(result).isPresent();
    assertSame(content, result.get());
  }

  @Test
  @SneakyThrows
  void readObject() {
    var bucketName = "bucket";
    var contentKey = "key";
    var content = "key".getBytes();
    var s3Object = new S3Object();
    s3Object.setObjectContent(new ByteArrayInputStream(content));
    var metadata = new ObjectMetadata();
    var userMetadata = Map.of("name", "value");
    metadata.setUserMetadata(userMetadata);
    s3Object.setObjectMetadata(metadata);

    when(amazonS3.listBuckets()).thenReturn(Collections.singletonList(new Bucket(bucketName)));
    when(amazonS3.doesObjectExist(bucketName, contentKey)).thenReturn(true);
    when(amazonS3.getObject(bucketName, contentKey)).thenReturn(s3Object);

    var result = cephServiceS3.get(bucketName, contentKey).get();

    assertThat(result.getContent().readAllBytes()).isEqualTo(content);
    assertThat(result.getMetadata().getUserMetadata()).isEqualTo(userMetadata);
  }

  @Test
  void readContentWithMetadataDoesNotExist() {
    var bucketName = "bucket";
    var contentKey = "key";

    when(amazonS3.listBuckets()).thenReturn(Collections.singletonList(new Bucket(bucketName)));
    when(amazonS3.doesObjectExist(bucketName, contentKey)).thenReturn(false);

    var result = cephServiceS3.get(bucketName, contentKey);

    assertThat(result).isEmpty();
  }

  @Test
  void putStringContent() {
    var bucketName = "bucket";
    var contentKey = "key";
    var content = "key";

    when(amazonS3.listBuckets()).thenReturn(Collections.singletonList(new Bucket(bucketName)));

    cephServiceS3.put(bucketName, contentKey, content);

    verify(amazonS3).putObject(bucketName, contentKey, content);
  }

  @Test
  void putObjectContent() throws IOException {
    var bucketName = "bucket";
    var contentKey = "key";
    var content = new ByteArrayInputStream("content".getBytes());
    var contentType = "application/png";
    var contentLength = 1000L;
    var userMetadata = Map.of("name", "value");
    var objectMetadata = new ObjectMetadata();
    objectMetadata.setContentType(contentType);
    objectMetadata.setContentLength(contentLength);
    objectMetadata.setUserMetadata(userMetadata);

    when(amazonS3.listBuckets()).thenReturn(Collections.singletonList(new Bucket(bucketName)));
    when(amazonS3.getObjectMetadata(bucketName, contentKey)).thenReturn(objectMetadata);

    var result = cephServiceS3.put(bucketName, contentKey, contentType, Map.of("name", "value"),
        content);

    assertThat(result.getContentType()).isEqualTo(contentType);
    assertThat(result.getContentLength()).isEqualTo(contentLength);
    assertThat(result.getUserMetadata()).isEqualTo(userMetadata);
    var objectMetadataArgCaptor = ArgumentCaptor.forClass(ObjectMetadata.class);
    verify(amazonS3).putObject(eq(bucketName), eq(contentKey), eq(content),
        objectMetadataArgCaptor.capture());
    var objectMetadataValue = objectMetadataArgCaptor.getValue();
    assertThat(objectMetadataValue.getUserMetadata()).isEqualTo(userMetadata);
    assertThat(objectMetadataValue.getContentType()).isEqualTo(contentType);
    assertThat(objectMetadataValue.getContentLength()).isNotNull();
  }

  @Test
  void putObjectContentWithContentLength() {
    var bucketName = "bucket";
    var contentKey = "key";
    var content = new ByteArrayInputStream("content".getBytes());
    var contentType = "application/png";
    var userMetadata = Map.of("name", "value");

    when(amazonS3.listBuckets()).thenReturn(Collections.singletonList(new Bucket(bucketName)));
    when(amazonS3.getObjectMetadata(bucketName, contentKey)).thenReturn(new ObjectMetadata());

    cephServiceS3.put(bucketName, contentKey, contentType, 999L,
        Map.of("name", "value"), content);

    var objectMetadataArgCaptor = ArgumentCaptor.forClass(ObjectMetadata.class);
    verify(amazonS3).putObject(eq(bucketName), eq(contentKey), eq(content),
        objectMetadataArgCaptor.capture());
    var objectMetadataValue = objectMetadataArgCaptor.getValue();
    assertThat(objectMetadataValue.getUserMetadata()).isEqualTo(userMetadata);
    assertThat(objectMetadataValue.getContentType()).isEqualTo(contentType);
    assertThat(objectMetadataValue.getContentLength()).isEqualTo(999L);
  }

  @Test
  void deleteContent() {
    var bucketName = "bucket";
    var contentKey = "key";

    when(amazonS3.listBuckets()).thenReturn(Collections.singletonList(new Bucket(bucketName)));

    cephServiceS3.delete(bucketName, Set.of(contentKey));

    ArgumentCaptor<DeleteObjectsRequest> captor = ArgumentCaptor.forClass(
        DeleteObjectsRequest.class);
    verify(amazonS3, times(1)).deleteObjects(captor.capture());
    var deleteObjectsRequests = captor.getValue();
    assertThat(deleteObjectsRequests.getKeys().get(0).getKey()).isEqualTo(contentKey);
  }

  @Test
  void deleteContentWithCommunicationIssues() {
    var bucketName = "bucket";
    var contentKey = "key";

    when(amazonS3.listBuckets()).thenReturn(Collections.singletonList(new Bucket(bucketName)));
    doThrow(new RuntimeException()).when(amazonS3).deleteObjects(any());

    assertThrows(CephCommunicationException.class,
        () -> cephServiceS3.delete(bucketName, Set.of(contentKey)));
  }

  @Test
  void readContentDoesNotExist() {
    var bucketName = "bucket";
    var contentKey = "key";

    when(amazonS3.listBuckets()).thenReturn(Collections.singletonList(new Bucket(bucketName)));
    when(amazonS3.doesObjectExist(bucketName, contentKey)).thenReturn(false);

    assertThat(cephServiceS3.getAsString(bucketName, contentKey)).isEmpty();
  }

  @Test
  void doesObjectExist() {
    var bucketName = "bucket";
    var contentKey = "key";

    when(amazonS3.listBuckets()).thenReturn(Collections.singletonList(new Bucket(bucketName)));
    when(amazonS3.doesObjectExist(bucketName, contentKey)).thenReturn(true);

    assertThat(cephServiceS3.exist(bucketName, contentKey)).isTrue();
  }

  @Test
  void testGetKeys() {
    var prefix = "test/files";
    var s3ObjectSummary = new S3ObjectSummary();
    var bucketName = "bucket";
    s3ObjectSummary.setKey("test/files/filename.png");
    when(amazonS3.listBuckets()).thenReturn(Collections.singletonList(new Bucket(bucketName)));
    when(amazonS3.listObjects(bucketName, prefix)).thenReturn(objectListing);
    when(objectListing.getObjectSummaries()).thenReturn(List.of(s3ObjectSummary));

    var keys = cephServiceS3.getKeys(bucketName, prefix);

    assertThat(keys.size()).isEqualTo(1);
    assertThat(keys.iterator().next()).isEqualTo(s3ObjectSummary.getKey());
  }

  @Test
  void testExistKeys() {
    var bucketName = "bucket";
    var contentKey = "key";
    lenient().when(amazonS3.listBuckets())
        .thenReturn(Collections.singletonList(new Bucket(bucketName)));
    lenient().when(amazonS3.doesObjectExist(bucketName, contentKey)).thenReturn(true);
    lenient().when(amazonS3.doesObjectExist(bucketName, "notExist")).thenReturn(false);

    assertThat(cephServiceS3.exist(bucketName, Set.of(contentKey, "notExist"))).isFalse();
  }

  @Test
  void testGetMetadata() {
    var contentKey = "key";
    var bucketName = "bucket";
    var userMetadata = Map.of(
        "id", contentKey,
        "checksum", "sha256hex",
        "filename", "filename.png"
    );
    var testObjectMetadata = new ObjectMetadata();
    testObjectMetadata.setContentLength(111L);
    testObjectMetadata.setContentType("image/png");
    testObjectMetadata.setUserMetadata(userMetadata);

    when(amazonS3.listBuckets()).thenReturn(Collections.singletonList(new Bucket(bucketName)));
    when(amazonS3.getObjectMetadata(bucketName, contentKey)).thenReturn(testObjectMetadata);
    when(amazonS3.doesObjectExist(bucketName, contentKey)).thenReturn(true);

    var metadata = cephServiceS3.getMetadata(bucketName, Set.of(contentKey));
    assertThat(metadata.size()).isOne();
    var objectMetadata = metadata.get(0);
    assertThat(objectMetadata.getContentLength()).isEqualTo(111L);
    assertThat(objectMetadata.getContentType()).isEqualTo("image/png");
    assertThat(objectMetadata.getUserMetadata().get("id")).isEqualTo(contentKey);
    assertThat(objectMetadata.getUserMetadata().get("checksum")).isEqualTo("sha256hex");
    assertThat(objectMetadata.getUserMetadata().get("filename")).isEqualTo("filename.png");
  }

  @Test
  void testGetMetadataByPrefix() {
    var contentKey = "key";
    var bucketName = "bucket";
    var userMetadata = Map.of(
        "fieldName", "documents",
        "formKey", "document_upload_form"
    );
    var testObjectMetadata = new ObjectMetadata();
    testObjectMetadata.setUserMetadata(userMetadata);

    when(amazonS3.listBuckets()).thenReturn(Collections.singletonList(new Bucket(bucketName)));
    when(amazonS3.getObjectMetadata(bucketName, contentKey)).thenReturn(testObjectMetadata);
    when(amazonS3.doesObjectExist(bucketName, contentKey)).thenReturn(true);

    var metadata = cephServiceS3.getMetadata(bucketName, Set.of(contentKey));
    assertThat(metadata.size()).isOne();
    var objectMetadata = metadata.get(0);
    assertThat(objectMetadata.getUserMetadata().get("fieldName")).isEqualTo("documents");
    assertThat(objectMetadata.getUserMetadata().get("formKey")).isEqualTo("document_upload_form");
  }

  @Test
  void testGetKeysInBucket() {
    var s3ObjectSummary = new S3ObjectSummary();
    var bucketName = "bucket";
    s3ObjectSummary.setKey("process/1d7fb67b-0125-11ed-8911-0a580a803423/task/Activity_1");
    when(amazonS3.listBuckets()).thenReturn(Collections.singletonList(new Bucket(bucketName)));
    when(amazonS3.listObjects(bucketName)).thenReturn(objectListing);
    when(objectListing.getObjectSummaries()).thenReturn(List.of(s3ObjectSummary));

    var keys = cephServiceS3.getKeys(bucketName);

    assertThat(keys.size()).isEqualTo(1);
    assertThat(keys.iterator().next()).isEqualTo(s3ObjectSummary.getKey());
  }
  
  @Test
  void shouldSetUserMetadataToNewObjectMetadata() {
    var contentKey = "key";
    var bucketName = "bucket";

    var newUserMetadata = Map.of(
        "id", contentKey,
        "checksum", "sha256hex",
        "filename", "filename.png"
    );
    var testObjectMetadata = new ObjectMetadata();
    testObjectMetadata.setUserMetadata(newUserMetadata);

    when(amazonS3.listBuckets()).thenReturn(Collections.singletonList(new Bucket(bucketName)));
    when(amazonS3.getObjectMetadata(bucketName, contentKey)).thenReturn(testObjectMetadata);
    
    cephServiceS3.setUserMetadata(bucketName, contentKey, newUserMetadata);

    var requestCaptor = ArgumentCaptor.forClass(CopyObjectRequest.class);
    verify(amazonS3).copyObject(requestCaptor.capture());
    var objectMetadata = requestCaptor.getValue().getNewObjectMetadata().getUserMetadata();
    assertThat(objectMetadata.get("id")).isEqualTo(contentKey);
    assertThat(objectMetadata.get("checksum")).isEqualTo("sha256hex");
    assertThat(objectMetadata.get("filename")).isEqualTo("filename.png");
  }
}