package com.epam.digital.data.platform.integration.ceph.service;

import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.epam.digital.data.platform.integration.ceph.exception.CephCommunicationException;
import com.epam.digital.data.platform.integration.ceph.exception.MisconfigurationException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.springframework.cloud.sleuth.annotation.NewSpan;

/**
 * Ceph client that is used for managing content as file objects.
 */
public interface S3ObjectCephService {

  /**
   * Put file object to ceph storage.
   *
   * @param key             object id.
   * @param contentType     object content type.
   * @param userMetadata    additional user metadata.
   * @param fileInputStream file input stream.
   * @return metadata of the saved object.
   * @throws MisconfigurationException if ceph bucket not exist
   * @throws CephCommunicationException if faced any 4xx or 5xx error from Ceph
   */
  @NewSpan
  ObjectMetadata put(String key, String contentType, Map<String, String> userMetadata,
      InputStream fileInputStream);

  /**
   * Get file object from ceph storage.
   *
   * @param key document id.
   * @return an object stored in ceph.
   * @throws MisconfigurationException if ceph bucket not exist
   * @throws CephCommunicationException if faced any 4xx or 5xx error from Ceph
   */
  @NewSpan
  Optional<S3Object> get(String key);

  /**
   * Get objects metadata by keys.
   *
   * @param keys object ids.
   * @return list of objects metadata.
   * @throws MisconfigurationException if ceph bucket not exist
   * @throws CephCommunicationException if faced any 4xx or 5xx error from Ceph
   */
  @NewSpan
  Optional<List<ObjectMetadata>> getMetadata(List<String> keys);

  /**
   * Delete objects by keys.
   *
   * @param keys objects keys.
   * @throws MisconfigurationException if ceph bucket not exist
   * @throws CephCommunicationException if faced any 4xx or 5xx error from Ceph
   */
  @NewSpan
  void delete(List<String> keys);

  /**
   * Check keys existence.
   *
   * @param keys specified keys.
   * @return true if all keys exist in storage.
   */
  @NewSpan
  Boolean exist(List<String> keys);

  /**
   * Get list of keys by prefix
   *
   * @param prefix used to search keys beginning with the specified prefix.
   * @return list of keys
   */
  @NewSpan
  List<String> getKeys(String prefix);
}
