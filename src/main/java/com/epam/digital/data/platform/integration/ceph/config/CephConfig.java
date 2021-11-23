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

package com.epam.digital.data.platform.integration.ceph.config;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.epam.digital.data.platform.integration.ceph.service.impl.CephServiceS3Impl;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

@Configuration
@ComponentScan(basePackageClasses = CephServiceS3Impl.class)
public class CephConfig {

  @Bean
  public AmazonS3 cephAmazonS3(
      @Value("${ceph.http-endpoint}") String cephHttpEndpoint,
      @Value("${ceph.access-key}") String cephAccessKey,
      @Value("${ceph.secret-key}") String cephSecretKey) {

    var credentials = new AWSStaticCredentialsProvider(
        new BasicAWSCredentials(cephAccessKey, cephSecretKey));

    var clientConfig = new ClientConfiguration();
    clientConfig.setProtocol(Protocol.HTTP);

    return AmazonS3ClientBuilder.standard()
        .withCredentials(credentials)
        .withClientConfiguration(clientConfig)
        .withEndpointConfiguration(new EndpointConfiguration(cephHttpEndpoint, null))
        .withPathStyleAccessEnabled(true)
        .build();
  }

}


