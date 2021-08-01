package com.epam.digital.data.platform.integration.ceph.service.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertThrows;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.Bucket;
import com.epam.digital.data.platform.integration.ceph.dto.FormDataDto;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Collections;
import java.util.LinkedHashMap;
import org.assertj.core.util.Lists;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class FormDataCephServiceImplTest {

  @Mock
  private AmazonS3 amazonS3;

  private FormDataCephServiceImpl cephServiceS3;

  private final String contentKey = "key";
  private final String bucketName = "bucket";
  private final String testFormDataContent = "{\"data\":{\"count\":10,\"name\":\"testName\",\"accreditationEndDate\":\"2021-02-25\",\"researches\":[\"addedResearchId\"]},\"x-access-token\":\"token\"}";

  @Before
  public void init() {
    cephServiceS3 = new FormDataCephServiceImpl(bucketName, new CephServiceS3Impl(amazonS3),
        new ObjectMapper());
  }

  @Test
  public void readFormData() {
    FormDataDto expectedFormData = new FormDataDto();
    expectedFormData.setAccessToken("token");
    LinkedHashMap<String, Object> data = new LinkedHashMap<>();
    data.put("count", 10);
    data.put("name", "testName");
    data.put("accreditationEndDate", "2021-02-25");
    data.put("researches", Lists.newArrayList("addedResearchId"));
    expectedFormData.setData(data);

    when(amazonS3.listBuckets()).thenReturn(Collections.singletonList(new Bucket(bucketName)));
    when(amazonS3.doesObjectExist(bucketName, contentKey)).thenReturn(true);
    when(amazonS3.getObjectAsString(bucketName, contentKey)).thenReturn(testFormDataContent);

    var result = cephServiceS3.getFormData(contentKey);

    assertThat(result).isPresent();
    assertEquals(expectedFormData, result.get());
  }

  @Test
  public void putFormData() {
    FormDataDto formData = new FormDataDto();
    formData.setAccessToken("token");
    LinkedHashMap<String, Object> data = new LinkedHashMap<>();
    data.put("count", 10);
    data.put("name", "testName");
    data.put("accreditationEndDate", "2021-02-25");
    data.put("researches", Lists.newArrayList("addedResearchId"));
    formData.setData(data);

    when(amazonS3.listBuckets()).thenReturn(Collections.singletonList(new Bucket(bucketName)));

    cephServiceS3.putFormData(contentKey, formData);

    verify(amazonS3).putObject(bucketName, contentKey, testFormDataContent);
  }

  @Test
  public void shouldNotDeserialize() {
    when(amazonS3.listBuckets()).thenReturn(Collections.singletonList(new Bucket(bucketName)));
    when(amazonS3.doesObjectExist(bucketName, contentKey)).thenReturn(true);
    when(amazonS3.getObjectAsString(bucketName, contentKey)).thenReturn("not valid json");

    assertThrows(IllegalArgumentException.class, () -> cephServiceS3.getFormData(contentKey));
  }

  @Test
  public void formDataDoesNotExist() {
    when(amazonS3.listBuckets()).thenReturn(Collections.singletonList(new Bucket(bucketName)));
    when(amazonS3.doesObjectExist(bucketName, contentKey)).thenReturn(false);

    assertThat(cephServiceS3.getFormData(contentKey)).isEmpty();
  }
}