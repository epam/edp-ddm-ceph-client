package com.epam.digital.data.platform.integration.ceph.service.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.util.IOUtils;
import com.epam.digital.data.platform.integration.ceph.dto.CephObject;
import com.epam.digital.data.platform.integration.ceph.exception.CephCommunicationException;
import com.epam.digital.data.platform.integration.ceph.exception.MisconfigurationException;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class CephServiceS3ImplTest {

  @Mock
  private AmazonS3 amazonS3;
  @InjectMocks
  private CephServiceS3Impl cephServiceS3;

  @Test
  public void readContentFromNonExistingBucket() {
    var bucketName = "bucket";
    var contentKey = "key";

    when(amazonS3.listBuckets()).thenReturn(Collections.emptyList());

    assertThrows(MisconfigurationException.class,
        () -> cephServiceS3.getContent(bucketName, contentKey));
  }

  @Test
  public void readContentWithCommunicationIssues() {
    var bucketName = "bucket";
    var contentKey = "key";

    when(amazonS3.listBuckets()).thenThrow(new RuntimeException());

    assertThrows(CephCommunicationException.class,
        () -> cephServiceS3.getContent(bucketName, contentKey));
  }

  @Test
  public void readContent() {
    var bucketName = "bucket";
    var contentKey = "key";
    var content = "key";

    when(amazonS3.listBuckets()).thenReturn(Collections.singletonList(new Bucket(bucketName)));
    when(amazonS3.doesObjectExist(bucketName, contentKey)).thenReturn(true);
    when(amazonS3.getObjectAsString(bucketName, contentKey)).thenReturn(content);

    var result = cephServiceS3.getContent(bucketName, contentKey);

    assertThat(result).isPresent();
    assertSame(content, result.get());
  }

  @Test
  public void readObject() {
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

    var result = cephServiceS3.getObject(bucketName, contentKey).get();

    assertThat(result.getContent()).isEqualTo(content);
    assertThat(result.getMetadata()).isEqualTo(userMetadata);
  }

  @Test
  public void readContentWithMetadataDoesNotExist() {
    var bucketName = "bucket";
    var contentKey = "key";

    when(amazonS3.listBuckets()).thenReturn(Collections.singletonList(new Bucket(bucketName)));
    when(amazonS3.doesObjectExist(bucketName, contentKey)).thenReturn(false);

    var result = cephServiceS3.getObject(bucketName, contentKey);

    assertThat(result).isEmpty();
  }

  @Test
  public void putContent() {
    var bucketName = "bucket";
    var contentKey = "key";
    var content = "key";

    when(amazonS3.listBuckets()).thenReturn(Collections.singletonList(new Bucket(bucketName)));

    cephServiceS3.putContent(bucketName, contentKey, content);

    verify(amazonS3).putObject(bucketName, contentKey, content);
  }

  @Test
  public void putByteArrayContent() throws IOException {
    var bucketName = "bucket";
    var contentKey = "key";
    var content = "content".getBytes();
    var userMetadata = Map.of("name", "value");

    when(amazonS3.listBuckets()).thenReturn(Collections.singletonList(new Bucket(bucketName)));

    cephServiceS3.putObject(bucketName, contentKey, new CephObject(content, Map.of("name", "value")));

    var putRequestCaptor = ArgumentCaptor.forClass(PutObjectRequest.class);
    verify(amazonS3).putObject(putRequestCaptor.capture());

    var putRequest = putRequestCaptor.getValue();
    assertThat(putRequest.getBucketName()).isEqualTo(bucketName);
    assertThat(putRequest.getKey()).isEqualTo(contentKey);
    assertThat(IOUtils.toByteArray(putRequest.getInputStream())).isEqualTo(content);
    assertThat(putRequest.getMetadata().getUserMetadata()).isEqualTo(userMetadata);
  }

  @Test
  public void deleteContent() {
    var bucketName = "bucket";
    var contentKey = "key";

    when(amazonS3.listBuckets()).thenReturn(Collections.singletonList(new Bucket(bucketName)));

    cephServiceS3.deleteObject(bucketName, contentKey);

    verify(amazonS3).deleteObject(bucketName, contentKey);
  }

  @Test
  public void deleteContentWithCommunicationIssues() {
    var bucketName = "bucket";
    var contentKey = "key";

    when(amazonS3.listBuckets()).thenReturn(Collections.singletonList(new Bucket(bucketName)));
    doThrow(new RuntimeException()).when(amazonS3).deleteObject(bucketName, contentKey);

    assertThrows(CephCommunicationException.class,
        () -> cephServiceS3.deleteObject(bucketName, contentKey));
  }

  @Test
  public void readContentDoesNotExist() {
    var bucketName = "bucket";
    var contentKey = "key";

    when(amazonS3.listBuckets()).thenReturn(Collections.singletonList(new Bucket(bucketName)));
    when(amazonS3.doesObjectExist(bucketName, contentKey)).thenReturn(false);

    assertThat(cephServiceS3.getContent(bucketName, contentKey)).isEmpty();
  }

  @Test
  public void doesObjectExist() {
    var bucketName = "bucket";
    var contentKey = "key";

    when(amazonS3.listBuckets()).thenReturn(Collections.singletonList(new Bucket(bucketName)));
    when(amazonS3.doesObjectExist(bucketName, contentKey)).thenReturn(true);

    assertThat(cephServiceS3.doesObjectExist(bucketName, contentKey)).isTrue();
  }
}
