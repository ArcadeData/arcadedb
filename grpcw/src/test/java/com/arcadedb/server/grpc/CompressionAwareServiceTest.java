/*
 * Copyright © 2021-present Arcade Data Ltd (info@arcadedata.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * SPDX-FileCopyrightText: 2021-present Arcade Data Ltd (info@arcadedata.com)
 * SPDX-License-Identifier: Apache-2.0
 */
package com.arcadedb.server.grpc;

import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class CompressionAwareServiceTest {

  @Test
  void setResponseCompressionWithServerCallStreamObserver() {
    @SuppressWarnings("unchecked")
    ServerCallStreamObserver<String> mockObserver = mock(ServerCallStreamObserver.class);

    CompressionAwareService.setResponseCompression(mockObserver, "gzip");

    verify(mockObserver).setCompression("gzip");
  }

  @Test
  void setResponseCompressionWithRegularStreamObserver() {
    @SuppressWarnings("unchecked")
    StreamObserver<String> mockObserver = mock(StreamObserver.class);

    // Should not throw even though it can't set compression
    CompressionAwareService.setResponseCompression(mockObserver, "gzip");

    // No exception means success
    assertThat(true).isTrue();
  }

  @Test
  void isCurrentRequestCompressedReturnsFalseWithoutContext() {
    // Without context set, should return false
    assertThat(CompressionAwareService.isCurrentRequestCompressed()).isFalse();
  }

  @Test
  void getCurrentRequestEncodingReturnsIdentityWithoutContext() {
    // Without context set, should return "identity"
    assertThat(CompressionAwareService.getCurrentRequestEncoding()).isEqualTo("identity");
  }

  @Test
  void compressionStatsRecordsCompressedRequest() {
    CompressionAwareService.CompressionStats stats = new CompressionAwareService.CompressionStats();

    stats.recordRequest(true);
    stats.recordRequest(true);
    stats.recordRequest(false);

    String statsString = stats.getStats();
    assertThat(statsString).contains("Requests:");
    assertThat(statsString).contains("2/3"); // 2 compressed out of 3
  }

  @Test
  void compressionStatsRecordsResponses() {
    CompressionAwareService.CompressionStats stats = new CompressionAwareService.CompressionStats();

    stats.recordResponse(true);
    stats.recordResponse(false);
    stats.recordResponse(false);
    stats.recordResponse(false);

    String statsString = stats.getStats();
    assertThat(statsString).contains("Responses:");
    assertThat(statsString).contains("1/4"); // 1 compressed out of 4
  }

  @Test
  void compressionStatsHandlesZeroRequests() {
    CompressionAwareService.CompressionStats stats = new CompressionAwareService.CompressionStats();

    String statsString = stats.getStats();

    // Should not throw with zero counts
    assertThat(statsString).matches(".*0(.|,)0%.*");
  }

  @Test
  void compressionStatsCalculatesCorrectPercentage() {
    CompressionAwareService.CompressionStats stats = new CompressionAwareService.CompressionStats();

    // 50% compression rate
    stats.recordRequest(true);
    stats.recordRequest(false);

    String statsString = stats.getStats();
    assertThat(statsString).matches(".*50(.|,)0%.*");
  }
}
