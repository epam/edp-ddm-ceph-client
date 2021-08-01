package com.epam.digital.data.platform.integration.ceph.service;

import com.epam.digital.data.platform.integration.ceph.dto.CephObject;
import com.epam.digital.data.platform.integration.ceph.exception.CephCommunicationException;
import com.epam.digital.data.platform.integration.ceph.exception.MisconfigurationException;
import java.util.Optional;
import org.springframework.cloud.sleuth.annotation.NewSpan;

/**
 * Ceph client class that is used for managing content
 */
public interface CephService {

  /**
   * Retrieve content by ceph bucket name and document id.
   *
   * @param cephBucketName ceph bucket name
   * @param key            document id
   * @return the document string representation (optional)
   * @throws MisconfigurationException if ceph bucket not exist
   * @throws CephCommunicationException if faced any 4xx or 5xx error from Ceph
   */
  @NewSpan
  Optional<String> getContent(String cephBucketName, String key);

  /**
   * Retrieve ceph content by ceph bucket name and document id.
   *
   * @param cephBucketName ceph bucket name
   * @param key            document id
   * @return ceph content and some custom additional info
   * @throws MisconfigurationException if ceph bucket not exist
   * @throws CephCommunicationException if faced any 4xx or 5xx error from Ceph
   */
  @NewSpan
  Optional<CephObject> getObject(String cephBucketName, String key);

  /**
   * Put string content to ceph bucket
   *
   * @param cephBucketName ceph bucket name
   * @param key            document id
   * @param content        the content to put itself
   * @throws MisconfigurationException if ceph bucket not exist
   * @throws CephCommunicationException if faced any 4xx or 5xx error from Ceph
   */
  @NewSpan
  void putContent(String cephBucketName, String key, String content);

  /**
   * Put byte[] content to ceph bucket
   *
   * @param cephBucketName ceph bucket name
   * @param key            document id
   * @param cephObject     the content to put itself
   * @throws MisconfigurationException if ceph bucket not exist
   * @throws CephCommunicationException if faced any 4xx or 5xx error from Ceph
   */
  @NewSpan
  void putObject(String cephBucketName, String key, CephObject cephObject);

  /**
   * Delete content from ceph bucket
   *
   * @param cephBucketName ceph bucket name
   * @param key            document id
   * @throws MisconfigurationException if ceph bucket not exist
   * @throws CephCommunicationException if faced any 4xx or 5xx error from Ceph
   */
  @NewSpan
  void deleteObject(String cephBucketName, String key);
}
