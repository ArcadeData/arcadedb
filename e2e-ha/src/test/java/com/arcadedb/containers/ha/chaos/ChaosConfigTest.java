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

package com.arcadedb.containers.ha.chaos;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.entry;

class ChaosConfigTest {

  static Properties props(final String... keyValues) {
    final Properties properties = new Properties();
    for (int i = 0; i < keyValues.length; i += 2)
      properties.setProperty(keyValues[i], keyValues[i + 1]);
    return properties;
  }

  @Test
  void defaults() {
    final ChaosConfig config = ChaosConfig.fromProperties(props());
    assertThat(config.nodes()).isEqualTo(3);
    assertThat(config.writers()).isEqualTo(4);
    assertThat(config.duration()).isEqualTo(Duration.ofMinutes(20));
    assertThat(config.maxSteps()).isZero();
    assertThat(config.faultWeights()).containsOnlyKeys(ChaosConfig.ALL_FAULTS.toArray(new String[0]));
    assertThat(config.faultWeights().values()).containsOnly(1);
    assertThat(config.holdMin()).isEqualTo(Duration.ofSeconds(10));
    assertThat(config.holdMax()).isEqualTo(Duration.ofSeconds(60));
    assertThat(config.calmMin()).isEqualTo(Duration.ofSeconds(10));
    assertThat(config.calmMax()).isEqualTo(Duration.ofSeconds(30));
    assertThat(config.convergenceTimeout()).isEqualTo(Duration.ofMinutes(2));
    assertThat(config.electionTimeout()).isEqualTo(Duration.ofSeconds(60));
    assertThat(config.availabilityGrace()).isEqualTo(Duration.ofSeconds(20));
  }

  @Test
  void explicitSeedAndNodes() {
    final ChaosConfig config = ChaosConfig.fromProperties(props("chaos.seed", "42", "chaos.nodes", "5"));
    assertThat(config.seed()).isEqualTo(42L);
    assertThat(config.nodes()).isEqualTo(5);
  }

  @Test
  void missingSeedIsRandom() {
    assertThat(ChaosConfig.fromProperties(props()).seed()).isNotEqualTo(ChaosConfig.fromProperties(props()).seed());
  }

  @Test
  void weightedFaults() {
    final ChaosConfig config = ChaosConfig.fromProperties(props("chaos.faults", " kill:3, pause "));
    assertThat(config.faultWeights()).containsExactly(entry("kill", 3), entry("pause", 1));
    assertThat(config.faultsSpec()).isEqualTo("kill:3,pause:1");
  }

  @Test
  void rejectsInvalidValues() {
    assertThatThrownBy(() -> ChaosConfig.fromProperties(props("chaos.nodes", "4")))
        .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("chaos.nodes");
    assertThatThrownBy(() -> ChaosConfig.fromProperties(props("chaos.faults", "nuke")))
        .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("nuke");
    assertThatThrownBy(() -> ChaosConfig.fromProperties(props("chaos.faults", "kill:0")))
        .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("weight");
    assertThatThrownBy(() -> ChaosConfig.fromProperties(props("chaos.holdMin", "PT30S", "chaos.holdMax", "PT10S")))
        .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("chaos.hold");
    assertThatThrownBy(() -> ChaosConfig.fromProperties(props("chaos.writers", "0")))
        .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("chaos.writers");
  }

  @Test
  void replayCommandCarriesTheDecisions() {
    final ChaosConfig config = ChaosConfig.fromProperties(props("chaos.seed", "42", "chaos.faults", "kill:3,pause"));
    assertThat(config.replayCommand())
        .contains("-Dit.test=HaChaosIT")
        .contains("-Dfailsafe.excludedGroups=")
        .contains("-Dchaos.seed=42")
        .contains("-Dchaos.faults=kill:3,pause:1")
        .contains("-Dchaos.holdMin=PT10S");
  }
}
