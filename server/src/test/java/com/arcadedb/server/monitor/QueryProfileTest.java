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
package com.arcadedb.server.monitor;

import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class QueryProfileTest {

  @AfterEach
  void cleanThreadLocal() {
    QueryProfile.popCurrent();
  }

  @Test
  void accumulatesTimeAcrossMultipleCalls() {
    final QueryProfile profile = new QueryProfile();
    profile.addDeserializationNanos(10);
    profile.addDeserializationNanos(5);
    profile.addEngineNanos(100);
    profile.addSerializationNanos(25);

    assertThat(profile.getDeserializationNanos()).isEqualTo(15);
    assertThat(profile.getEngineNanos()).isEqualTo(100);
    assertThat(profile.getSerializationNanos()).isEqualTo(25);
  }

  @Test
  void overheadExcludesEngineTime() {
    final QueryProfile profile = new QueryProfile();
    profile.addDeserializationNanos(10);
    profile.addEngineNanos(1000);
    profile.addSerializationNanos(20);

    assertThat(profile.getOverheadNanos()).isEqualTo(30);
    assertThat(profile.getTotalNanos()).isEqualTo(1030);
  }

  @Test
  void emptyProfileReportsZeros() {
    final QueryProfile profile = new QueryProfile();
    final JSONObject json = profile.toJSON();

    assertThat(json.getLong("deserializationNanos")).isZero();
    assertThat(json.getLong("engineNanos")).isZero();
    assertThat(json.getLong("serializationNanos")).isZero();
    assertThat(json.getLong("overheadNanos")).isZero();
    assertThat(json.getLong("totalNanos")).isZero();
    assertThat(json.getDouble("deserializationMs")).isZero();
    assertThat(json.getDouble("engineMs")).isZero();
    assertThat(json.getDouble("serializationMs")).isZero();
    assertThat(json.getDouble("overheadMs")).isZero();
    assertThat(json.getDouble("totalMs")).isZero();
  }

  @Test
  void toJsonExposesBothNanosAndMilliseconds() {
    final QueryProfile profile = new QueryProfile();
    // 1.5ms of deserialization, 2ms of engine, 0.5ms of serialization
    profile.addDeserializationNanos(1_500_000L);
    profile.addEngineNanos(2_000_000L);
    profile.addSerializationNanos(500_000L);

    final JSONObject json = profile.toJSON();

    assertThat(json.getLong("deserializationNanos")).isEqualTo(1_500_000L);
    assertThat(json.getLong("engineNanos")).isEqualTo(2_000_000L);
    assertThat(json.getLong("serializationNanos")).isEqualTo(500_000L);
    assertThat(json.getLong("overheadNanos")).isEqualTo(2_000_000L);
    assertThat(json.getLong("totalNanos")).isEqualTo(4_000_000L);

    assertThat(json.getDouble("deserializationMs")).isEqualTo(1.5);
    assertThat(json.getDouble("engineMs")).isEqualTo(2.0);
    assertThat(json.getDouble("serializationMs")).isEqualTo(0.5);
    assertThat(json.getDouble("overheadMs")).isEqualTo(2.0);
    assertThat(json.getDouble("totalMs")).isEqualTo(4.0);
  }

  @Test
  void threadLocalPushAndPopExposesCurrentProfile() {
    assertThat(QueryProfile.current()).isNull();

    final QueryProfile profile = new QueryProfile();
    QueryProfile.pushCurrent(profile);
    try {
      assertThat(QueryProfile.current()).isSameAs(profile);
    } finally {
      QueryProfile.popCurrent();
    }

    assertThat(QueryProfile.current()).isNull();
  }
}
