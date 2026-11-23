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
package com.arcadedb.remote;

import com.arcadedb.database.RID;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Base64;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #12187: the parameter types JSON cannot carry go on the wire as typed markers the server decodes, instead of the
 * text, double and epoch millis they used to become.
 */
class Issue12187EncodeTypedParametersTest {

  @Test
  void eachTypeBecomesItsMarker() {
    final Map<String, Object> in = new LinkedHashMap<>();
    in.put("rid", new RID(12, 3));
    in.put("decimal", new BigDecimal("0.10"));
    in.put("bytes", new byte[] { 1, 2, -1 });
    in.put("date", new Date(1700000000123L));
    in.put("localDateTime", LocalDateTime.of(2024, 2, 29, 13, 14, 15, 123456789));
    in.put("instant", Instant.ofEpochMilli(1700000000123L));
    in.put("zoned", ZonedDateTime.of(2024, 1, 1, 10, 0, 0, 0, ZoneId.of("Europe/Rome")));

    final Map<String, Object> out = RemoteHttpComponent.encodeParameters(in);

    assertThat(out.get("rid")).isEqualTo(Map.of("$rid", "#12:3"));
    assertThat(out.get("decimal")).isEqualTo(Map.of("$decimal", "0.10"));
    assertThat(out.get("bytes")).isEqualTo(Map.of("$bytes", Base64.getEncoder().encodeToString(new byte[] { 1, 2, -1 })));
    assertThat(out.get("date")).isEqualTo(Map.of("$datetime", 1700000000123L));
    assertThat(out.get("localDateTime")).isEqualTo(Map.of("$localDateTime", "2024-02-29T13:14:15.123456789"));
    assertThat(out.get("instant")).isEqualTo(Map.of("$instant", "2023-11-14T22:13:20.123Z"));
    assertThat(out.get("zoned")).isEqualTo(Map.of("$zonedDateTime", "2024-01-01T10:00:00+01:00[Europe/Rome]"));
  }

  @Test
  void markersNestedInCollectionsAndMaps() {
    final Map<String, Object> in = new LinkedHashMap<>();
    in.put("links", List.of(new RID(1, 0), new RID(1, 1)));
    in.put("prices", Map.of("eur", new BigDecimal("19.90")));

    final Map<String, Object> out = RemoteHttpComponent.encodeParameters(in);

    assertThat(out.get("links")).isEqualTo(List.of(Map.of("$rid", "#1:0"), Map.of("$rid", "#1:1")));
    assertThat(out.get("prices")).isEqualTo(Map.of("eur", Map.of("$decimal", "19.90")));
  }

  @Test
  void plainJsonValuesAreLeftAsTheyAre() {
    final Map<String, Object> in = new LinkedHashMap<>();
    in.put("s", "#1:0");
    in.put("n", 5L);
    in.put("d", 1.5);
    in.put("b", true);
    in.put("l", List.of("a", 1));
    assertThat(RemoteHttpComponent.encodeParameters(in)).isSameAs(in);
  }
}
