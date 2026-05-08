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
package com.arcadedb.utility;

import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class DateUtilsTest {

  /**
   * Regression for issue #4142: {@link DateUtils#dateTimeToTimestamp(Object, ChronoUnit)} is
   * the conversion landing path used by the GraphBatch edge serializer. Until #4142 it called
   * {@link LocalDateTime#parse(CharSequence)} directly, which rejects ISO-8601 strings ending
   * with {@code Z} or any {@code ±HH:mm} offset - the default JSON datetime serialization in
   * most languages.
   */
  @Test
  void parsesIsoDateTimeWithZSuffix() {
    final long expected = LocalDateTime.parse("2026-05-08T18:37:54").toEpochSecond(java.time.ZoneOffset.UTC);
    assertThat(DateUtils.dateTimeToTimestamp("2026-05-08T18:37:54Z", ChronoUnit.SECONDS)).isEqualTo(expected);
  }

  @Test
  void parsesIsoDateTimeWithMillisAndZSuffix() {
    final long expected = LocalDateTime.parse("2026-05-08T18:37:54").toEpochSecond(java.time.ZoneOffset.UTC) * 1_000L;
    assertThat(DateUtils.dateTimeToTimestamp("2026-05-08T18:37:54.000Z", ChronoUnit.MILLIS)).isEqualTo(expected);
  }

  @Test
  void parsesIsoDateTimeWithExplicitOffset() {
    final long expected = LocalDateTime.parse("2026-05-08T18:37:54").toEpochSecond(java.time.ZoneOffset.UTC);
    assertThat(DateUtils.dateTimeToTimestamp("2026-05-08T18:37:54+00:00", ChronoUnit.SECONDS)).isEqualTo(expected);
  }

  @Test
  void parsesIsoDateTimeWithoutZone() {
    final long expected = LocalDateTime.parse("2026-05-08T18:37:54").toEpochSecond(java.time.ZoneOffset.UTC);
    assertThat(DateUtils.dateTimeToTimestamp("2026-05-08T18:37:54", ChronoUnit.SECONDS)).isEqualTo(expected);
  }

  @Test
  void zoneSuffixYieldsSameTimestampAsLocalDateTime() {
    // Both inputs must collapse to the same UTC epoch value: the offset is a wall-clock hint
    // that the engine drops at ingest time so vertex and edge paths agree on storage.
    final Long withZ = DateUtils.dateTimeToTimestamp("2026-05-08T18:37:54Z", ChronoUnit.MILLIS);
    final Long withoutZone = DateUtils.dateTimeToTimestamp("2026-05-08T18:37:54", ChronoUnit.MILLIS);
    assertThat(withZ).isEqualTo(withoutZone);
  }
}
