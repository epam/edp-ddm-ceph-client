package com.epam.digital.data.platform.integration.ceph.exception;

public class MisconfigurationException extends RuntimeException {

  public MisconfigurationException(String message) {
    super(message);
  }
}
