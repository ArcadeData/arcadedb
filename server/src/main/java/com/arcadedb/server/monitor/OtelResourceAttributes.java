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

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * The OpenTelemetry resource attributes (most importantly {@code service.name}) the optional tracing and OTLP metrics
 * plugins report, resolved in one place so the two can never disagree (issue #7295).
 * <p>
 * Precedence follows the OpenTelemetry SDK environment-variable specification, with ArcadeDB's own setting as the
 * fallback where the SDK would report {@code unknown_service}:
 * <ol>
 *   <li>{@code OTEL_SERVICE_NAME}</li>
 *   <li>a {@code service.name} entry in {@code OTEL_RESOURCE_ATTRIBUTES}</li>
 *   <li>{@link GlobalConfiguration#SERVER_METRICS_SERVICE_NAME} ({@code arcadedb} by default)</li>
 * </ol>
 * Every other {@code OTEL_RESOURCE_ATTRIBUTES} entry is reported as well. The tracing plugin used to build its tracer
 * provider from the bare SDK default resource, which reads neither variable, so every span said
 * {@code unknown_service:java}; the OTLP metrics plugin read the variables but let {@code OTEL_RESOURCE_ATTRIBUTES} win
 * over {@code OTEL_SERVICE_NAME} and fell back to {@code unknown_service}.
 */
public final class OtelResourceAttributes {
  public static final String SERVICE_NAME            = "service.name";
  static final        String ENV_SERVICE_NAME        = "OTEL_SERVICE_NAME";
  static final        String ENV_RESOURCE_ATTRIBUTES = "OTEL_RESOURCE_ATTRIBUTES";

  private OtelResourceAttributes() {
  }

  /**
   * Resolves the attributes from the process environment and {@code configuration}.
   */
  public static Map<String, String> resolve(final ContextConfiguration configuration) {
    return resolve(configuration, System.getenv());
  }

  /**
   * Resolves the attributes from {@code environment} (the process environment in production, a plain map in tests) and
   * {@code configuration}. The returned map always carries a non-blank {@code service.name}.
   */
  public static Map<String, String> resolve(final ContextConfiguration configuration, final Map<String, String> environment) {
    final Map<String, String> attributes = parseResourceAttributes(environment.get(ENV_RESOURCE_ATTRIBUTES));

    final String envServiceName = environment.get(ENV_SERVICE_NAME);
    if (envServiceName != null && !envServiceName.isBlank())
      attributes.put(SERVICE_NAME, envServiceName.trim());
    else if (attributes.getOrDefault(SERVICE_NAME, "").isBlank())
      attributes.put(SERVICE_NAME, configuredServiceName(configuration));

    return attributes;
  }

  private static String configuredServiceName(final ContextConfiguration configuration) {
    final String configured = configuration != null ?
        configuration.getValueAsString(GlobalConfiguration.SERVER_METRICS_SERVICE_NAME) :
        null;
    if (configured == null || configured.isBlank())
      return (String) GlobalConfiguration.SERVER_METRICS_SERVICE_NAME.getDefValue();
    return configured.trim();
  }

  /**
   * Parses the {@code key1=value1,key2=value2} format of {@code OTEL_RESOURCE_ATTRIBUTES}. Values are percent-decoded
   * as the specification requires; an entry with no {@code =} or an empty key is skipped rather than failing the
   * plugin, and a value whose percent-encoding is malformed is kept as written.
   */
  static Map<String, String> parseResourceAttributes(final String raw) {
    final Map<String, String> attributes = new LinkedHashMap<>();
    if (raw == null || raw.isBlank())
      return attributes;

    for (final String entry : raw.split(",")) {
      final int eq = entry.indexOf('=');
      if (eq <= 0)
        continue;
      final String key = entry.substring(0, eq).trim();
      if (key.isEmpty())
        continue;
      attributes.put(key, percentDecode(entry.substring(eq + 1).trim()));
    }
    return attributes;
  }

  private static String percentDecode(final String value) {
    if (value.indexOf('%') < 0)
      return value;
    try {
      // URLDecoder is form decoding, which also turns '+' into a space; the specification asks for percent-decoding
      // only, so a literal '+' is protected first.
      return URLDecoder.decode(value.replace("+", "%2B"), StandardCharsets.UTF_8);
    } catch (final IllegalArgumentException e) {
      return value;
    }
  }
}
