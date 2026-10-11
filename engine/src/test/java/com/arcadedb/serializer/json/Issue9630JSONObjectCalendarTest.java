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

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for #9630: {@code arcadedb.dateTimeImplementation=java.util.Calendar} and
 * {@code arcadedb.dateImplementation=java.util.Calendar} are supported settings, so a {@link Calendar} is a value a record
 * can carry. {@link JSONObject} wrote one as its {@code toString()} - a 600-character dump of the calendar's fields - where
 * a {@link Date} holding the same instant was written as its epoch milliseconds.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9630JSONObjectCalendarTest {

  private static final long MILLIS = Instant.parse("2024-03-15T14:30:45.123Z").toEpochMilli();

  private static Calendar calendar(final String zone) {
    final Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone(zone));
    calendar.setTimeInMillis(MILLIS);
    return calendar;
  }

  @Test
  void calendarIsWrittenAsItsEpochMillisLikeADate() {
    for (final String zone : new String[] { "UTC", "Asia/Tokyo", "America/New_York" }) {
      final JSONObject json = new JSONObject().put("ts", (Object) calendar(zone)).put("date", (Object) new Date(MILLIS));
      assertThat(json.getLong("ts")).as(zone).isEqualTo(MILLIS);
      assertThat(json.getLong("ts")).as(zone).isEqualTo(json.getLong("date"));
    }
  }

  @Test
  void calendarInsideACollectionOrMapIsWrittenAsItsEpochMillis() {
    final JSONObject json = new JSONObject().put("list", (Object) List.of(calendar("Asia/Tokyo")))
        .put("map", (Object) Map.of("ts", calendar("America/New_York")));
    assertThat(json.getJSONArray("list").getLong(0)).isEqualTo(MILLIS);
    assertThat(json.getJSONObject("map").getLong("ts")).isEqualTo(MILLIS);
  }

  /**
   * With a date format set, a Calendar is written as the Date it holds would be, whatever zone the calendar carries: the
   * same instant must not become two different strings depending on which of the two configured classes holds it.
   */
  @Test
  void calendarFollowsTheDateFormatExactlyAsADate() {
    for (final String zone : new String[] { "UTC", "Asia/Tokyo", "America/New_York" }) {
      final JSONObject json = new JSONObject().setDateFormat("yyyy-MM-dd HH:mm:ss.SSS").put("ts", (Object) calendar(zone))
          .put("date", (Object) new Date(MILLIS));
      assertThat(json.getString("ts")).as(zone).isEqualTo(json.getString("date"));
      assertThat(json.getString("ts")).as(zone).matches("2024-03-1[45] \\d\\d:\\d\\d:45\\.123");
    }
  }
}
