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
package com.arcadedb;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9316: nineteen of the twenty {@code arcadedb.grpc.*} keys the gRPC plugin reads were bare strings, reachable from -D
 * only. The server configuration file dropped them with a warning and the environment-variable loop never asked for them, so
 * {@code arcadedb.grpc.tls.enabled=true} written in either left a plaintext endpoint listening. They are declared settings now.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9316GrpcSettingsDeclaredTest {
  private static final List<String> KEYS = List.of("enabled", "port", "host", "mode", "xds.port", "tls.enabled", "tls.cert",
      "tls.key", "maxMessageSize", "maxMetadataSize", "maxConcurrentTransactions", "maxConcurrentTransactionsPerPrincipal",
      "reflection.enabled", "health.enabled", "compression.enabled", "compression.force", "compression.type", "tx.maxIdleMs",
      "tx.maxAgeMs", "tx.reaperPeriodMs");

  @Test
  void everyGrpcKeyTheServerPluginReadsIsADeclaredSetting() {
    for (final String key : KEYS)
      assertThat(GlobalConfiguration.findByKey("arcadedb.grpc." + key)).as("arcadedb.grpc." + key).isNotNull();
  }

  @Test
  void theServerConfigurationFileReachesTlsAndTheOtherGrpcKeys() {
    final ContextConfiguration cfg = new ContextConfiguration();
    cfg.fromJSON("{\"configuration\":{\"grpc.port\":50099,\"grpc.tls.enabled\":true,\"grpc.tls.cert\":\"/etc/ssl/server.crt\","
        + "\"grpc.tls.key\":\"/etc/ssl/server.key\",\"grpc.reflection.enabled\":false,\"grpc.tx.maxIdleMs\":1234}}");

    assertThat(cfg.getValueAsBoolean(GlobalConfiguration.GRPC_TLS_ENABLED)).isTrue();
    assertThat(cfg.getValueAsString(GlobalConfiguration.GRPC_TLS_CERT)).isEqualTo("/etc/ssl/server.crt");
    assertThat(cfg.getValueAsString(GlobalConfiguration.GRPC_TLS_KEY)).isEqualTo("/etc/ssl/server.key");
    assertThat(cfg.getValueAsBoolean(GlobalConfiguration.GRPC_REFLECTION_ENABLED)).isFalse();
    assertThat(cfg.getValueAsLong(GlobalConfiguration.GRPC_TX_MAX_IDLE_MS)).isEqualTo(1234L);
    assertThat(cfg.getValueAsInteger(GlobalConfiguration.GRPC_PORT)).isEqualTo(50099);
  }

  @Test
  void theDefaultsAreTheOnesThePluginAlwaysUsed() {
    assertThat((Boolean) GlobalConfiguration.GRPC_ENABLED.getDefValue()).isTrue();
    assertThat((Boolean) GlobalConfiguration.GRPC_TLS_ENABLED.getDefValue()).isFalse();
    assertThat(GlobalConfiguration.GRPC_HOST.getDefValue()).isEqualTo("0.0.0.0");
    assertThat(GlobalConfiguration.GRPC_MODE.getDefValue()).isEqualTo("standard");
    assertThat(GlobalConfiguration.GRPC_XDS_PORT.getDefValue()).isEqualTo(50052);
    assertThat(GlobalConfiguration.GRPC_MAX_MESSAGE_SIZE.getDefValue()).isEqualTo(100);
    assertThat(GlobalConfiguration.GRPC_MAX_METADATA_SIZE.getDefValue()).isEqualTo(16);
    assertThat(GlobalConfiguration.GRPC_MAX_CONCURRENT_TRANSACTIONS.getDefValue()).isEqualTo(1000);
    assertThat(GlobalConfiguration.GRPC_MAX_CONCURRENT_TRANSACTIONS_PER_PRINCIPAL.getDefValue()).isEqualTo(100);
    assertThat(GlobalConfiguration.GRPC_COMPRESSION_TYPE.getDefValue()).isEqualTo("gzip");
    assertThat(GlobalConfiguration.GRPC_TX_MAX_IDLE_MS.getDefValue()).isEqualTo(300_000L);
    assertThat(GlobalConfiguration.GRPC_TX_MAX_AGE_MS.getDefValue()).isEqualTo(0L);
    assertThat(GlobalConfiguration.GRPC_TX_REAPER_PERIOD_MS.getDefValue()).isEqualTo(30_000L);
  }
}
