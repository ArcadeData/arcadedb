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
package com.arcadedb.server.support;

import com.arcadedb.serializer.json.JSONObject;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeParseException;
import java.util.Map;

/**
 * The window of the log to collect: a preset ({@code 10m, 30m, 1h, 12h, 24h, 1w}, counted back from now) or a custom
 * {@code from}-{@code to}. Both ends are instants; an ISO-8601 value without an offset is read in the time zone of the log
 * (the JVM time zone of the server), which is the zone the log timestamps are written in.
 */
public record SupportLogWindow(Instant from, Instant to) {
  private static final Map<String, Duration> PRESETS = Map.of("10m", Duration.ofMinutes(10), "30m", Duration.ofMinutes(30), "1h",
      Duration.ofHours(1), "12h", Duration.ofHours(12), "24h", Duration.ofHours(24), "1w", Duration.ofDays(7));

  public static SupportLogWindow preset(final String preset, final Instant now) {
    final Duration duration = PRESETS.get(preset);
    if (duration == null)
      throw new IllegalArgumentException("Unknown log window preset '" + preset + "': use 10m, 30m, 1h, 12h, 24h or 1w");
    return new SupportLogWindow(now.minus(duration), now);
  }

  /**
   * @param window {@code {"preset":"1h"}} or {@code {"from":"...","to":"..."}}
   * @param zone   the zone of ISO values with no offset
   */
  public static SupportLogWindow parse(final JSONObject window, final Instant now, final ZoneId zone) {
    if (window == null)
      throw new IllegalArgumentException("The log window is required when the logs are included");
    if (window.has("preset"))
      return preset(window.getString("preset", ""), now);
    if (!window.has("from") || !window.has("to"))
      throw new IllegalArgumentException("The log window is a preset or a 'from' and a 'to'");
    final Instant from = parseInstant(window.getString("from", ""), zone);
    final Instant to = parseInstant(window.getString("to", ""), zone);
    if (!to.isAfter(from))
      throw new IllegalArgumentException("The end of the log window must be after its start");
    return new SupportLogWindow(from, to);
  }

  static Instant parseInstant(final String text, final ZoneId zone) {
    final String value = text == null ? "" : text.trim();
    try {
      return Instant.parse(value);
    } catch (final DateTimeParseException ignored) {
      // try the next form
    }
    try {
      return OffsetDateTime.parse(value).toInstant();
    } catch (final DateTimeParseException ignored) {
      // try the next form
    }
    try {
      return LocalDateTime.parse(value).atZone(zone).toInstant();
    } catch (final DateTimeParseException e) {
      throw new IllegalArgumentException("'" + value + "' is not an ISO-8601 date and time");
    }
  }
}
