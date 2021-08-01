package com.epam.digital.data.platform.integration.ceph.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.Serializable;
import java.util.LinkedHashMap;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class FormDataDto implements Serializable {

  private LinkedHashMap<String, Object> data;
  @JsonProperty("x-access-token")
  @JsonInclude(Include.NON_NULL)
  private String accessToken;
  @JsonInclude(Include.NON_NULL)
  private String signature;
}

