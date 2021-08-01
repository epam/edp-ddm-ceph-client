package com.epam.digital.data.platform.integration.ceph.service.impl;

import com.epam.digital.data.platform.integration.ceph.dto.FormDataDto;
import com.epam.digital.data.platform.integration.ceph.service.CephService;
import com.epam.digital.data.platform.integration.ceph.service.FormDataCephService;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Optional;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Service
public class FormDataCephServiceImpl implements FormDataCephService {

  private final String cephBucketName;
  private final CephService cephService;
  private final ObjectMapper objectMapper;

  @Autowired
  public FormDataCephServiceImpl(@Value("${ceph.bucket}") String cephBucketName,
      CephService cephService, ObjectMapper objectMapper) {
    this.cephBucketName = cephBucketName;
    this.cephService = cephService;
    this.objectMapper = objectMapper;
  }

  @Override
  public Optional<FormDataDto> getFormData(String key) {
    return cephService.getContent(cephBucketName, key).map(this::deserializeFormData);
  }

  @Override
  public void putFormData(String key, FormDataDto content) {
    cephService.putContent(cephBucketName, key, serializeFormData(content));
  }

  private FormDataDto deserializeFormData(String formData) {
    try {
      return objectMapper.readValue(formData, FormDataDto.class);
    } catch (JsonProcessingException e) {
      e.clearLocation();
      throw new IllegalArgumentException("Couldn't deserialize form data", e);
    }
  }

  private String serializeFormData(FormDataDto formData) {
    try {
      return objectMapper.writeValueAsString(formData);
    } catch (JsonProcessingException e) {
      e.clearLocation();
      throw new IllegalArgumentException("Couldn't serialize form data", e);
    }
  }
}
