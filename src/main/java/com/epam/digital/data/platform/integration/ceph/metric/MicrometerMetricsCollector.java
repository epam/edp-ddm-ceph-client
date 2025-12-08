/*
 * Copyright 2025 EPAM Systems.
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

package com.epam.digital.data.platform.integration.ceph.metric;

import com.amazonaws.Request;
import com.amazonaws.Response;
import com.amazonaws.metrics.RequestMetricCollector;
import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.model.DeleteObjectRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.util.TimingInfo;
import com.epam.digital.data.platform.integration.ceph.metric.model.OperationInfo;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class MicrometerMetricsCollector extends RequestMetricCollector {

  private final MeterRegistry registry;
  private final Map<String, AtomicLong> gauges = new ConcurrentHashMap<>();

  @Override
  public void collectMetrics(Request<?> request, Response<?> response) {

    if (request == null) return;
    TimingInfo timingInfo = request.getAWSRequestMetrics() != null ? request.getAWSRequestMetrics().getTimingInfo() : null;
    if (timingInfo == null) return;

    // Connection pool gauges
    setGauge("aws_sdk_pool_available", timingInfo.getCounter("HttpClientPoolAvailableCount"));
    setGauge("aws_sdk_pool_leased", timingInfo.getCounter("HttpClientPoolLeasedCount"));
    setGauge("aws_sdk_pool_pending", timingInfo.getCounter("HttpClientPoolPendingCount"));

    OperationInfo operationInfo = extractOperationInfo(request);
    if (operationInfo == null) return;

    // Errors
    incrementCounter("aws_sdk_exception_count", operationInfo, timingInfo.getCounter("Exception"));

    // Latency
    recordLatency("ClientExecuteTime", timingInfo, operationInfo);
  }

  private void recordLatency(String metricName, TimingInfo ti, OperationInfo info) {
    Number value = extractLatency(ti, metricName);
    if (value == null) return;

    Timer.builder("aws_sdk_latency." + metricName.toLowerCase())
        .tag("bucket", info.getSourceBucket())
        .tag("operation", info.getOperation())
        .register(registry)
        .record(value.longValue(), TimeUnit.MILLISECONDS);
  }

  private void incrementCounter(String name, OperationInfo info, Number value) {
    if (value == null) return;

    Counter.builder(name)
        .tag("bucket", info.getSourceBucket())
        .tag("operation", info.getOperation())
        .register(registry)
        .increment(value.doubleValue());
  }

  private void setGauge(String name, Number value) {
    if (value == null) return;

    gauges.computeIfAbsent(name, k -> {
      AtomicLong gaugeValue = new AtomicLong(value.longValue());
      Gauge.builder(name, gaugeValue, AtomicLong::get)
          .register(registry);
      return gaugeValue;
    }).set(value.longValue());
  }

  private Number extractLatency(TimingInfo ti, String metricName) {
    TimingInfo sub = ti.getSubMeasurement(metricName);
    return sub.getTimeTakenMillisIfKnown();
  }

  private OperationInfo extractOperationInfo(Request<?> request) {
    Object original = request.getOriginalRequest();
    if (original == null) return null;

    String operation = original.getClass().getSimpleName();

    if (original instanceof PutObjectRequest) {
      return new OperationInfo(((PutObjectRequest) original).getBucketName(), operation);
    } else if (original instanceof GetObjectRequest) {
      return new OperationInfo(((GetObjectRequest) original).getBucketName(), operation);
    } else if (original instanceof ListObjectsRequest) {
      return new OperationInfo(((ListObjectsRequest) original).getBucketName(), operation);
    } else if (original instanceof DeleteObjectRequest) {
      return new OperationInfo(((DeleteObjectRequest) original).getBucketName(), operation);
    } else if (original instanceof CopyObjectRequest) {
      return new OperationInfo(((CopyObjectRequest) original).getSourceBucketName(), operation);
    }
    return null;
  }
}
