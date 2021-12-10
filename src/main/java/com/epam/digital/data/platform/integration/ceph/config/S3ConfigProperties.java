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
import com.amazonaws.services.s3.S3ClientOptions;

public class S3ConfigProperties {

  private S3ClientOptions options = S3ClientOptions.builder().build();
  private ClientConfiguration client = new ClientConfiguration();

  public S3ClientOptions getOptions() {
    return options;
  }

  public void setOptions(S3ClientOptions options) {
    this.options = options;
  }

  public ClientConfiguration getClient() {
    return client;
  }

  public void setClient(ClientConfiguration client) {
    this.client = client;
  }
}
