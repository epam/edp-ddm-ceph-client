package com.epam.digital.data.platform.integration.ceph.exception;

public class CephCommunicationException extends RuntimeException {

  public CephCommunicationException(String message, Throwable cause) {
    super(message, cause);
  }
}
