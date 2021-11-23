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

package com.epam.digital.data.platform.integration.ceph.dto;

import java.util.Arrays;
import java.util.Map;
import java.util.Objects;

public class CephObject {
  private byte[] content;
  private Map<String, String> metadata;

  public CephObject(byte[] content, Map<String, String> metadata) {
    this.content = content;
    this.metadata = metadata;
  }

  public byte[] getContent() {
    return content;
  }

  public void setContent(byte[] content) {
    this.content = content;
  }

  public Map<String, String> getMetadata() {
    return metadata;
  }

  public void setMetadata(Map<String, String> metadata) {
    this.metadata = metadata;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    CephObject that = (CephObject) o;
    return Arrays.equals(content, that.content) && Objects.equals(metadata, that.metadata);
  }

  @Override
  public int hashCode() {
    int result = Objects.hash(metadata);
    result = 31 * result + Arrays.hashCode(content);
    return result;
  }

  @Override
  public String toString() {
    return "CephObject{" + "content=" + Arrays.toString(content) + ", metadata=" + metadata + '}';
  }
}