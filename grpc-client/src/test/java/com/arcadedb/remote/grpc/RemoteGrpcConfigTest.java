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
package com.arcadedb.remote.grpc;

import com.arcadedb.server.grpc.ProjectionSettings.ProjectionEncoding;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for RemoteGrpcConfig.
 * Tests gRPC configuration record class.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class RemoteGrpcConfigTest {

  @Test
  void shouldCreateConfigWithProjectionsEnabled() {
    final RemoteGrpcConfig config = new RemoteGrpcConfig(true, ProjectionEncoding.PROJECTION_AS_JSON, 1024);

    assertThat(config.includeProjections()).isTrue();
    assertThat(config.projectionEncoding()).isEqualTo(ProjectionEncoding.PROJECTION_AS_JSON);
    assertThat(config.softLimitBytes()).isEqualTo(1024);
  }

  @Test
  void shouldCreateConfigWithProjectionsDisabled() {
    final RemoteGrpcConfig config = new RemoteGrpcConfig(false, ProjectionEncoding.PROJECTION_AS_MAP, 2048);

    assertThat(config.includeProjections()).isFalse();
    assertThat(config.projectionEncoding()).isEqualTo(ProjectionEncoding.PROJECTION_AS_MAP);
    assertThat(config.softLimitBytes()).isEqualTo(2048);
  }

  @Test
  void shouldCreateConfigWithLinkEncoding() {
    final RemoteGrpcConfig config = new RemoteGrpcConfig(true, ProjectionEncoding.PROJECTION_AS_LINK, 4096);

    assertThat(config.includeProjections()).isTrue();
    assertThat(config.projectionEncoding()).isEqualTo(ProjectionEncoding.PROJECTION_AS_LINK);
    assertThat(config.softLimitBytes()).isEqualTo(4096);
  }

  @Test
  void shouldCreateConfigWithMapEncoding() {
    final RemoteGrpcConfig config = new RemoteGrpcConfig(true, ProjectionEncoding.PROJECTION_AS_MAP, 8192);

    assertThat(config.projectionEncoding()).isEqualTo(ProjectionEncoding.PROJECTION_AS_MAP);
    assertThat(config.softLimitBytes()).isEqualTo(8192);
  }

  @Test
  void shouldCreateConfigWithJsonEncoding() {
    final RemoteGrpcConfig config = new RemoteGrpcConfig(true, ProjectionEncoding.PROJECTION_AS_JSON, 16384);

    assertThat(config.projectionEncoding()).isEqualTo(ProjectionEncoding.PROJECTION_AS_JSON);
    assertThat(config.softLimitBytes()).isEqualTo(16384);
  }

  @Test
  void shouldSupportEqualityComparison() {
    final RemoteGrpcConfig config1 = new RemoteGrpcConfig(true, ProjectionEncoding.PROJECTION_AS_JSON, 1024);
    final RemoteGrpcConfig config2 = new RemoteGrpcConfig(true, ProjectionEncoding.PROJECTION_AS_JSON, 1024);
    final RemoteGrpcConfig config3 = new RemoteGrpcConfig(false, ProjectionEncoding.PROJECTION_AS_JSON, 1024);

    assertThat(config1).isEqualTo(config2);
    assertThat(config1).isNotEqualTo(config3);
  }

  @Test
  void shouldSupportHashCode() {
    final RemoteGrpcConfig config1 = new RemoteGrpcConfig(true, ProjectionEncoding.PROJECTION_AS_JSON, 1024);
    final RemoteGrpcConfig config2 = new RemoteGrpcConfig(true, ProjectionEncoding.PROJECTION_AS_JSON, 1024);

    assertThat(config1.hashCode()).isEqualTo(config2.hashCode());
  }

  @Test
  void shouldSupportToString() {
    final RemoteGrpcConfig config = new RemoteGrpcConfig(true, ProjectionEncoding.PROJECTION_AS_JSON, 1024);

    final String toString = config.toString();
    assertThat(toString).contains("true");
    assertThat(toString).contains("1024");
  }

  @Test
  void shouldHandleDifferentEncodingTypes() {
    final RemoteGrpcConfig jsonConfig = new RemoteGrpcConfig(true, ProjectionEncoding.PROJECTION_AS_JSON, 1024);
    final RemoteGrpcConfig mapConfig = new RemoteGrpcConfig(true, ProjectionEncoding.PROJECTION_AS_MAP, 1024);
    final RemoteGrpcConfig linkConfig = new RemoteGrpcConfig(true, ProjectionEncoding.PROJECTION_AS_LINK, 1024);

    assertThat(jsonConfig.projectionEncoding()).isNotEqualTo(mapConfig.projectionEncoding());
    assertThat(mapConfig.projectionEncoding()).isNotEqualTo(linkConfig.projectionEncoding());
    assertThat(jsonConfig).isNotEqualTo(mapConfig);
    assertThat(mapConfig).isNotEqualTo(linkConfig);
  }

  @Test
  void shouldCreateConfigWithZeroSoftLimit() {
    final RemoteGrpcConfig config = new RemoteGrpcConfig(false, ProjectionEncoding.PROJECTION_AS_LINK, 0);

    assertThat(config.softLimitBytes()).isEqualTo(0);
  }

  @Test
  void shouldCreateConfigWithLargeSoftLimit() {
    final RemoteGrpcConfig config = new RemoteGrpcConfig(true, ProjectionEncoding.PROJECTION_AS_JSON, 1024 * 1024);

    assertThat(config.softLimitBytes()).isEqualTo(1024 * 1024);
  }
}
