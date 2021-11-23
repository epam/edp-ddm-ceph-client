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

import com.epam.digital.data.platform.integration.ceph.dto.FormDataDto;
import com.epam.digital.data.platform.integration.ceph.exception.CephCommunicationException;
import com.epam.digital.data.platform.integration.ceph.exception.MisconfigurationException;
import java.util.Optional;
import org.springframework.cloud.sleuth.annotation.NewSpan;

/**
 * Ceph client class that is used for managing content as {@link FormDataDto}.
 * <p>
 * Class is meant to use fixed ceph bucket name
 */
public interface FormDataCephService {

  /**
   * Retrieve formData by document id
   *
   * @param key document id
   * @return {@link FormDataDto} content representation (optional)
   * @throws MisconfigurationException if configured ceph bucket not exist
   * @throws CephCommunicationException if faced any 4xx or 5xx error from Ceph
   * @throws IllegalArgumentException  if stored content couldn't be parsed to {@link FormDataDto}
   */
  @NewSpan
  Optional<FormDataDto> getFormData(String key);

  /**
   * Put formData to ceph
   *
   * @param key     document id
   * @param content {@link FormDataDto} content representation
   * @throws MisconfigurationException if configured ceph bucket not exist
   * @throws CephCommunicationException if faced any 4xx or 5xx error from Ceph
   */
  @NewSpan
  void putFormData(String key, FormDataDto content);
}
