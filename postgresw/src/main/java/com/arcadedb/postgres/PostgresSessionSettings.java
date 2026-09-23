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
package com.arcadedb.postgres;

import com.arcadedb.log.LogManager;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.logging.Level;
import java.util.regex.Pattern;

/**
 * The run-time parameters of ONE Postgres wire connection: what its {@code SET} commands and its startup packet
 * established, and what {@code SHOW} answers (issue #8217). A PostgreSQL {@code SET} is session-scoped, so nothing
 * here reaches the database, the schema or any other connection.
 * <p>
 * {@code SHOW} answers the value this connection set, else the server default. A few parameters describe what this
 * server actually does rather than a preference the client can change, and {@code SHOW} always answers the truth
 * for those:
 * <ul>
 *   <li>{@code server_version}, {@code server_encoding}, {@code integer_datetimes} are read-only in PostgreSQL too,
 *   and a {@code SET} of them is refused with {@code 55P02} as PostgreSQL refuses it;</li>
 *   <li>{@code client_encoding} is accepted - drivers send it routinely - but every text value leaves this server in
 *   UTF-8, so {@code SHOW} answers {@code UTF8};</li>
 *   <li>{@code DateStyle} keeps the field order the client asks for, but its output style is always {@code ISO}: that
 *   is the only format {@code PostgresType} writes a date or timestamp in.</li>
 * </ul>
 * Not thread-safe: a connection is served by one thread.
 */
final class PostgresSessionSettings {
  static final String DATESTYLE = "datestyle";

  private static final String  DEFAULT_DATE_ORDER  = "MDY";
  private static final Pattern DATESTYLE_SEPARATOR = Pattern.compile("[,\\s]+");

  private final Map<String, String> values    = new HashMap<>();
  private       String              dateOrder = DEFAULT_DATE_ORDER;

  /**
   * Thrown when a {@code SET} is refused, carrying the SQLSTATE PostgreSQL answers the same refusal with.
   */
  static final class SettingException extends RuntimeException {
    final String sqlState;

    SettingException(final String message, final String sqlState) {
      super(message);
      this.sqlState = sqlState;
    }
  }

  /**
   * Applies a {@code SET <name> = <value>}. {@code DEFAULT} restores the server default.
   *
   * @throws SettingException if PostgreSQL would refuse the same {@code SET}; nothing changes then
   */
  void set(final String name, final String value) {
    final String key = name.toLowerCase(Locale.ENGLISH);
    switch (key) {
    case "server_version", "server_encoding", "integer_datetimes" ->
        throw new SettingException("parameter \"" + key + "\" cannot be changed", "55P02"); // cant_change_runtime_param
    case "client_encoding" -> {
      // Accepted, as drivers send it routinely, but not stored: show() answers UTF8, what the server really sends.
    }
    case DATESTYLE -> dateOrder = "DEFAULT".equalsIgnoreCase(value) ? DEFAULT_DATE_ORDER : parseDateOrder(value);
    default -> {
      if ("DEFAULT".equalsIgnoreCase(value))
        values.remove(key);
      else
        values.put(key, value);
    }
    }
  }

  /**
   * Records a parameter the client sent in its startup packet. Unlike {@link #set}, a value that would be refused is
   * logged and ignored: failing the whole connection for a startup preference would be harsher than PostgreSQL, which
   * reports a bad one as a warning for most parameters.
   */
  void setFromStartup(final String name, final String value) {
    try {
      set(name, value);
    } catch (final SettingException e) {
      LogManager.instance().log(this, Level.INFO, "Ignoring startup parameter %s='%s': %s", name, value, e.getMessage());
    }
  }

  /**
   * The value {@code SHOW <name>} answers: what this connection set, else the server default, else an empty string.
   */
  String show(final String name) {
    final String key = name.toLowerCase(Locale.ENGLISH);
    return switch (key) {
      // Server facts: nothing a client sets changes them.
      case "server_version" -> PostgresNetworkExecutor.PG_SERVER_VERSION;
      case "server_encoding", "client_encoding" -> "UTF8";
      case "integer_datetimes" -> "on";
      case DATESTYLE -> "ISO, " + dateOrder;
      default -> {
        final String value = values.get(key);
        if (value != null)
          yield value;
        yield switch (key) {
          case "standard_conforming_strings" -> "on";
          case "timezone" -> "UTC";
          default -> "";
        };
      }
    };
  }

  /**
   * The field order a {@code DateStyle} value selects - the current one when the value names only an output style, as
   * in PostgreSQL. An output style other than {@code ISO} is accepted but not applied, since dates only ever travel as
   * ISO on this server.
   */
  private String parseDateOrder(final String value) {
    String order = dateOrder;
    for (final String token : DATESTYLE_SEPARATOR.split(value.trim())) {
      switch (token.toUpperCase(Locale.ENGLISH)) {
      case "ISO" -> {
      }
      case "SQL", "POSTGRES", "GERMAN" ->
          LogManager.instance().log(this, Level.INFO, "DateStyle output '%s' not supported, dates are sent as ISO", token);
      case "MDY", "US", "NONEURO", "NONEUROPEAN" -> order = "MDY";
      case "DMY", "EURO", "EUROPEAN" -> order = "DMY";
      case "YMD" -> order = "YMD";
      default -> throw new SettingException("invalid value for parameter \"DateStyle\": \"" + value + "\"", "22023"); // invalid_parameter_value
      }
    }
    return order;
  }
}
