package com.epam.digital.data.platform.integration.ceph.service.impl;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.epam.digital.data.platform.integration.ceph.UserMetadataHeaders;
import com.epam.digital.data.platform.integration.ceph.service.S3ObjectCephService;
import java.io.ByteArrayInputStream;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class S3ObjectCephServiceImplTest {

  @Mock
  private AmazonS3 amazonS3;
  private ObjectListing objectListing;

  private S3ObjectCephService service;

  private final String contentKey = "key";
  private final String bucketName = "bucket";

  @Before
  public void init() {
    service = new S3ObjectCephServiceImpl(bucketName, amazonS3);
    objectListing = Mockito.mock(ObjectListing.class);
  }

  @Test
  public void testGetObject() {
    var contentType = "application/pdf";
    var contentLength = 1000L;
    var testObjectMetadata = new ObjectMetadata();
    testObjectMetadata.setContentType(contentType);
    testObjectMetadata.setContentLength(contentLength);
    var testObject = new S3Object();
    testObject.setObjectMetadata(testObjectMetadata);

    when(amazonS3.listBuckets()).thenReturn(Collections.singletonList(new Bucket(bucketName)));
    when(amazonS3.getObject(bucketName, contentKey)).thenReturn(testObject);
    when(amazonS3.doesObjectExist(bucketName, contentKey)).thenReturn(true);

    S3Object returnedObject = service.get(contentKey).get();

    assertThat(returnedObject.getObjectMetadata().getContentLength()).isEqualTo(contentLength);
    assertThat(returnedObject.getObjectMetadata().getContentType()).isEqualTo(contentType);
  }

  @Test
  public void testPutObject() {
    var data = new byte[] {1};
    var filename = "test.pdf";
    var contentType = "application/pdf";
    var userMetadata = Map.of(UserMetadataHeaders.FILENAME, filename);
    var testObjectMetadata = new ObjectMetadata();
    testObjectMetadata.setContentType(contentType);
    testObjectMetadata.setUserMetadata(userMetadata);

    when(amazonS3.listBuckets()).thenReturn(Collections.singletonList(new Bucket(bucketName)));
    when(amazonS3.getObjectMetadata(bucketName, contentKey)).thenReturn(testObjectMetadata);

    ObjectMetadata savedObjectMetadata = service
        .put(contentKey, contentType, userMetadata, new ByteArrayInputStream(data));

    verify(amazonS3).putObject(eq(bucketName), eq(contentKey), any(), any());
    assertThat(savedObjectMetadata.getContentType()).isEqualTo(contentType);
    assertThat(savedObjectMetadata.getUserMetaDataOf(UserMetadataHeaders.FILENAME))
        .isEqualTo(filename);
  }

  @Test
  public void testGetMetadata() {
    var userMetadata = Map.of(
        UserMetadataHeaders.ID, contentKey,
        UserMetadataHeaders.CHECKSUM, "sha256hex",
        UserMetadataHeaders.FILENAME, "filename.png"
    );
    var testObjectMetadata = new ObjectMetadata();
    testObjectMetadata.setContentLength(111L);
    testObjectMetadata.setContentType("image/png");
    testObjectMetadata.setUserMetadata(userMetadata);

    when(amazonS3.listBuckets()).thenReturn(Collections.singletonList(new Bucket(bucketName)));
    when(amazonS3.getObjectMetadata(bucketName, contentKey)).thenReturn(testObjectMetadata);
    when(amazonS3.doesObjectExist(bucketName, contentKey)).thenReturn(true);

    List<ObjectMetadata> metadata = service.getMetadata(List.of(contentKey)).get();
    assertThat(metadata.size()).isOne();
    ObjectMetadata objectMetadata = metadata.get(0);
    assertThat(objectMetadata.getContentLength()).isEqualTo(111L);
    assertThat(objectMetadata.getContentType()).isEqualTo("image/png");
    assertThat(objectMetadata.getUserMetaDataOf(UserMetadataHeaders.ID)).isEqualTo(contentKey);
    assertThat(objectMetadata.getUserMetaDataOf(UserMetadataHeaders.CHECKSUM))
        .isEqualTo("sha256hex");
    assertThat(objectMetadata.getUserMetaDataOf(UserMetadataHeaders.FILENAME))
        .isEqualTo("filename.png");
  }

  @Test
  public void testDeleteObject() {
    when(amazonS3.listBuckets()).thenReturn(Collections.singletonList(new Bucket(bucketName)));

    service.delete(List.of(contentKey));
    ArgumentCaptor<DeleteObjectsRequest> captor = ArgumentCaptor
        .forClass(DeleteObjectsRequest.class);
    verify(amazonS3, times(1))
        .deleteObjects(captor.capture());
    DeleteObjectsRequest deleteObjectsRequests = captor.getValue();
    assertThat(deleteObjectsRequests.getKeys().get(0).getKey()).isEqualTo(contentKey);
  }

  @Test
  public void testExistKeys() {
    when(amazonS3.listBuckets()).thenReturn(Collections.singletonList(new Bucket(bucketName)));
    when(amazonS3.doesObjectExist(bucketName, contentKey)).thenReturn(true);
    when(amazonS3.doesObjectExist(bucketName, "notExist")).thenReturn(false);

    assertThat(service.exist(List.of(contentKey, "notExist"))).isFalse();
  }

  @Test
  public void testGetKeys() {
    var prefix = "test/files";
    var s3ObjectSummary = new S3ObjectSummary();
    s3ObjectSummary.setKey("test/files/filename.png");
    when(amazonS3.listBuckets()).thenReturn(Collections.singletonList(new Bucket(bucketName)));
    when(amazonS3.listObjects(bucketName, prefix)).thenReturn(objectListing);
    when(objectListing.getObjectSummaries()).thenReturn(List.of(s3ObjectSummary));

    var keys = service.getKeys(prefix);

    assertThat(keys.size()).isEqualTo(1);
    assertThat(keys.get(0)).isEqualTo(s3ObjectSummary.getKey());
  }
}
