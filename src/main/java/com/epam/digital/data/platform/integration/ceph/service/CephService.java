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

import com.epam.digital.data.platform.integration.ceph.exception.CephCommunicationException;
import com.epam.digital.data.platform.integration.ceph.exception.MisconfigurationException;
import com.epam.digital.data.platform.integration.ceph.model.CephObject;
import com.epam.digital.data.platform.integration.ceph.model.CephObjectMetadata;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.springframework.cloud.sleuth.annotation.NewSpan;

public interface CephService {

  /**
   * Retrieve ceph content by ceph bucket name and document id.
   *
   * @param cephBucketName ceph bucket name
   * @param key            document id
   * @return ceph content and metadata
   * @throws MisconfigurationException  if ceph bucket not exist
   * @throws CephCommunicationException if faced any 4xx or 5xx error from Ceph
   */
  @NewSpan("getObject")
  Optional<CephObject> get(String cephBucketName, String key);

  /**
   * Retrieve content as string by ceph bucket name and document id.
   *
   * @param cephBucketName ceph bucket name
   * @param key            document id
   * @return the document string representation (optional)
   * @throws MisconfigurationException  if ceph bucket not exist
   * @throws CephCommunicationException if faced any 4xx or 5xx error from Ceph
   */
  @NewSpan
  Optional<String> getAsString(String cephBucketName, String key);

  /**
   * Put string content to ceph bucket
   *
   * @param cephBucketName ceph bucket name
   * @param key            document id
   * @param content        the content to put itself
   * @throws MisconfigurationException  if ceph bucket not exist
   * @throws CephCommunicationException if faced any 4xx or 5xx error from Ceph
   */
  @NewSpan("putContentAsString")
  void put(String cephBucketName, String key, String content);

  /**
   * Put file object to ceph storage.
   *
   * @param key             object id.
   * @param contentType     object content type.
   * @param userMetadata    additional user metadata.
   * @param fileInputStream file input stream.
   * @return metadata of the saved object.
   * @throws MisconfigurationException  if ceph bucket not exist
   * @throws CephCommunicationException if faced any 4xx or 5xx error from Ceph
   */
  @NewSpan("putObject")
  CephObjectMetadata put(String cephBucketName, String key, String contentType,
      Map<String, String> userMetadata, InputStream fileInputStream);

  /**
   * Put file object to ceph storage.
   *
   * @param key             object id.
   * @param contentType     object content type.
   * @param userMetadata    additional user metadata.
   * @param inputStream     input stream.
   * @return metadata of the saved object.
   * @throws MisconfigurationException  if ceph bucket not exist
   * @throws CephCommunicationException if faced any 4xx or 5xx error from Ceph
   */
  @NewSpan("putObject")
  CephObjectMetadata put(String cephBucketName, String key, String contentType, long contentLength,
      Map<String, String> userMetadata, InputStream inputStream);

  
  /**
   * Delete objects by keys.
   *
   * @param keys objects keys.
   * @throws MisconfigurationException  if ceph bucket not exist
   * @throws CephCommunicationException if faced any 4xx or 5xx error from Ceph
   */
  @NewSpan
  void delete(String cephBucketName, Set<String> keys);

  /**
   * Check keys existence.
   *
   * @param keys specified keys.
   * @return true if all keys exist in storage.
   * @throws MisconfigurationException  if ceph bucket not exist
   * @throws CephCommunicationException if faced any 4xx or 5xx error from Ceph
   */
  @NewSpan("checkKeysExistence")
  Boolean exist(String cephBucketName, Set<String> keys);

  /**
   * Check if object exists in bucket
   *
   * @param cephBucketName ceph bucket name
   * @param key            document id
   * @throws MisconfigurationException  if ceph bucket not exist
   * @throws CephCommunicationException if faced any 4xx or 5xx error from Ceph
   */
  @NewSpan("checkKeyExistence")
  Boolean exist(String cephBucketName, String key);

  /**
   * Get list of keys by prefix
   *
   * @param prefix used to search keys beginning with the specified prefix
   * @return set of keys
   * @throws MisconfigurationException  if ceph bucket not exist
   * @throws CephCommunicationException if faced any 4xx or 5xx error from Ceph               .
   */
  @NewSpan
  Set<String> getKeys(String cephBucketName, String prefix);

  /**
   * Get list of all keys in ceph storage
   *
   * @param cephBucketName ceph bucket name
   * @return set of keys
   * @throws MisconfigurationException  if ceph bucket not exist
   * @throws CephCommunicationException if faced any 4xx or 5xx error from Ceph
   */
  Set<String> getKeys(String cephBucketName);

  /**
   * Get objects metadata by keys.
   *
   * @param keys object ids.
   * @return list of objects metadata.
   * @throws MisconfigurationException  if ceph bucket not exist
   * @throws CephCommunicationException if faced any 4xx or 5xx error from Ceph
   */
  @NewSpan("getObjectsMetadata")
  List<CephObjectMetadata> getMetadata(String cephBucketName, Set<String> keys);

  /**
   * Get objects metadata by key prefix.
   *
   * @param keyPrefix specified key prefix
   * @return list of objects metadata.
   */
  @NewSpan("getObjectsMetadata")
  List<CephObjectMetadata> getMetadata(String cephBucketName, String keyPrefix);


  /**
   * Set user metadata by key.
   *
   * @param key             object id.
   * @param userMetadata    new user metadata.
   * @return set user metadata to current object.
   */
  @NewSpan("setUserMetadata")
  CephObjectMetadata setUserMetadata(
      String cephBucketName, String key, Map<String, String> userMetadata);
}
