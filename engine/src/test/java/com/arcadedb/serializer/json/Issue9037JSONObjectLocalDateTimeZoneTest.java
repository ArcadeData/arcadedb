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
package com.arcadedb.serializer.json;

import com.arcadedb.utility.DateUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;

import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.List;
import java.util.TimeZone;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for #9037: a {@link LocalDate} is a calendar day, so its JSON number must not depend on the JVM default time zone.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9037JSONObjectLocalDateTimeZoneTest {

  private static final long UTC_MIDNIGHT = LocalDate.of(2024, 1, 2).atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli();

  @Test
  @ResourceLock(Resources.TIME_ZONE)
  void localDateIsTheSameNumberInEveryZone() {
    final TimeZone previous = TimeZone.getDefault();
    try {
      for (final String zone : new String[] { "UTC", "Asia/Seoul", "America/Los_Angeles" }) {
        TimeZone.setDefault(TimeZone.getTimeZone(zone));
        final JSONObject direct = new JSONObject().put("day", (Object) LocalDate.of(2024, 1, 2));
        assertThat(direct.getLong("day")).as(zone).isEqualTo(UTC_MIDNIGHT);
        assertThat(DateUtils.millisToLocalDate(direct.getLong("day"))).as(zone).isEqualTo(LocalDate.of(2024, 1, 2));

        final JSONObject nested = new JSONObject().put("days", (Object) List.of(LocalDate.of(2024, 1, 2)));
        assertThat(nested.getJSONArray("days").getLong(0)).as(zone).isEqualTo(UTC_MIDNIGHT);
      }
    } finally {
      TimeZone.setDefault(previous);
    }
  }
}
