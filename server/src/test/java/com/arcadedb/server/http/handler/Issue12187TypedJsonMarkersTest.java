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
package com.arcadedb.server.http.handler;

import com.arcadedb.database.RID;
import com.arcadedb.server.http.RequestRefusedBeforeExecutionException;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Issue #12187: the typed markers the Java remote client sends for a link, a decimal and the date-time types. */
class Issue12187TypedJsonMarkersTest {

  @Test
  @SuppressWarnings("unchecked")
  void markersDecodeIntoTheirJavaType() {
    final Map<String, Object> in = new LinkedHashMap<>();
    in.put("rid", Map.of("$rid", "#12:3"));
    in.put("decimal", Map.of("$decimal", "0.10"));
    in.put("datetime", Map.of("$datetime", 1700000000123L));
    in.put("localDateTime", Map.of("$localDateTime", "2024-02-29T13:14:15.123456789"));
    in.put("instant", Map.of("$instant", "2023-11-14T22:13:20.123Z"));
    in.put("zoned", Map.of("$zonedDateTime", "2024-01-01T10:00:00+01:00[Europe/Rome]"));
    in.put("nested", List.of(Map.of("$rid", "#1:0"), Map.of("k", Map.of("$decimal", "1.230"))));

    final Map<String, Object> out = AbstractQueryHandler.decodeTypedJsonMarkers(in);

    assertThat(out.get("rid")).isEqualTo(new RID(12, 3));
    assertThat(out.get("decimal")).isEqualTo(new BigDecimal("0.10"));
    assertThat(((BigDecimal) out.get("decimal")).scale()).isEqualTo(2);
    assertThat(out.get("datetime")).isEqualTo(new Date(1700000000123L));
    assertThat(out.get("localDateTime")).isEqualTo(LocalDateTime.of(2024, 2, 29, 13, 14, 15, 123456789));
    assertThat(out.get("instant")).isEqualTo(Instant.ofEpochMilli(1700000000123L));
    assertThat(out.get("zoned")).isEqualTo(ZonedDateTime.of(2024, 1, 1, 10, 0, 0, 0, ZoneId.of("Europe/Rome")));
    final List<Object> nested = (List<Object>) out.get("nested");
    assertThat(nested.get(0)).isEqualTo(new RID(1, 0));
    assertThat(((Map<String, Object>) nested.get(1)).get("k")).isEqualTo(new BigDecimal("1.230"));
  }

  @Test
  void malformedMarkersAreRefused() {
    for (final Map<String, Object> marker : List.<Map<String, Object>>of(
        Map.of("$rid", "not a rid"), Map.of("$rid", 12),
        Map.of("$decimal", "1.2.3"), Map.of("$decimal", 1.5),
        Map.of("$datetime", "yesterday"), Map.of("$datetime", 1.5),
        Map.of("$localDateTime", "2024-13-01T00:00"), Map.of("$instant", 5), Map.of("$zonedDateTime", "2024-01-01")))
      assertThatThrownBy(() -> AbstractQueryHandler.decodeTypedJsonMarkers(new LinkedHashMap<>(Map.of("p", marker))))
          .as(marker.toString())
          .isInstanceOf(RequestRefusedBeforeExecutionException.class);
  }
}
