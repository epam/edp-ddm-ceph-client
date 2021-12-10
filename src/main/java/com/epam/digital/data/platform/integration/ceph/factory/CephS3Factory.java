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

package com.epam.digital.data.platform.integration.ceph.factory;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.epam.digital.data.platform.integration.ceph.config.S3ConfigProperties;
import com.epam.digital.data.platform.integration.ceph.service.CephService;
import com.epam.digital.data.platform.integration.ceph.service.impl.CephServiceS3Impl;

public class CephS3Factory {

  private final S3ConfigProperties s3ConfigProperties;

  public CephS3Factory(S3ConfigProperties s3ConfigProperties) {
    this.s3ConfigProperties = s3ConfigProperties;
  }

  public CephService createCephService(
      String cephEndpoint, String cephAccessKey, String cephSecretKey) {
    return new CephServiceS3Impl(s3Client(cephEndpoint, cephAccessKey, cephSecretKey));
  }

  private AmazonS3 s3Client(String cephEndpoint, String cephAccessKey, String cephSecretKey) {
    var clientOptions = s3ConfigProperties.getOptions();
    return AmazonS3ClientBuilder.standard()
        .withCredentials(
            new AWSStaticCredentialsProvider(new BasicAWSCredentials(cephAccessKey, cephSecretKey)))
        .withClientConfiguration(s3ConfigProperties.getClient())
        .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(cephEndpoint, null))
        .withPathStyleAccessEnabled(clientOptions.isPathStyleAccess())
        .withChunkedEncodingDisabled(clientOptions.isChunkedEncodingDisabled())
        .withAccelerateModeEnabled(clientOptions.isAccelerateModeEnabled())
        .withPayloadSigningEnabled(clientOptions.isPayloadSigningEnabled())
        .withDualstackEnabled(clientOptions.isDualstackEnabled())
        .withForceGlobalBucketAccessEnabled(clientOptions.isForceGlobalBucketAccessEnabled())
        .build();
  }
}
