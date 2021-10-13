package com.epam.digital.data.platform.integration.ceph.service;

import com.amazonaws.services.s3.AmazonS3;
import com.epam.digital.data.platform.integration.ceph.exception.CephCommunicationException;
import com.epam.digital.data.platform.integration.ceph.exception.MisconfigurationException;
import java.util.function.Supplier;
import lombok.extern.slf4j.Slf4j;

/**
 * Contains general logic for all ceph services.
 */
@Slf4j
public abstract class BaseCephService {

  protected void assertBucketExists(AmazonS3 cephAmazonS3, String cephBucketName) {
    log.debug("Checking if bucket {} exists", cephBucketName);
    var buckets = execute(cephAmazonS3::listBuckets);
    buckets.stream()
        .filter(bucket -> bucket.getName().equals(cephBucketName))
        .findFirst()
        .orElseThrow(() -> new MisconfigurationException(
            String.format("Bucket %s hasn't found", cephBucketName)));
  }

  protected <T> T execute(Supplier<T> supplier) {
    try {
      return supplier.get();
    } catch (RuntimeException exception) {
      throw new CephCommunicationException(exception.getMessage(), exception);
    }
  }

  protected void executeRunnable(Runnable runnable) {
    try {
      runnable.run();
    } catch (RuntimeException exception) {
      throw new CephCommunicationException(exception.getMessage(), exception);
    }
  }
}
