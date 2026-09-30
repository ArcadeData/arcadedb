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
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #7295: the {@code service.name} the tracing and OTLP metrics plugins report is resolved from the standard
 * OpenTelemetry environment variables first and falls back to ArcadeDB's own setting ({@code arcadedb} by default)
 * rather than to {@code unknown_service}.
 */
class Issue7295OtelResourceAttributesTest {

  @Test
  void defaultsToArcadedbWhenNothingNamesAService() {
    final Map<String, String> attributes = OtelResourceAttributes.resolve(new ContextConfiguration(), Map.of());

    assertThat(attributes).containsExactly(Map.entry("service.name", "arcadedb"));
  }

  @Test
  void configuredServiceNameIsUsedWhenTheEnvironmentNamesNone() {
    final ContextConfiguration configuration = new ContextConfiguration();
    configuration.setValue(GlobalConfiguration.SERVER_METRICS_SERVICE_NAME, "orders-graph");

    assertThat(OtelResourceAttributes.resolve(configuration, Map.of()).get("service.name")).isEqualTo("orders-graph");
  }

  @Test
  void blankConfiguredServiceNameFallsBackToTheDefault() {
    final ContextConfiguration configuration = new ContextConfiguration();
    configuration.setValue(GlobalConfiguration.SERVER_METRICS_SERVICE_NAME, "  ");

    assertThat(OtelResourceAttributes.resolve(configuration, Map.of()).get("service.name")).isEqualTo("arcadedb");
  }

  @Test
  void otelServiceNameWinsOverEverything() {
    final ContextConfiguration configuration = new ContextConfiguration();
    configuration.setValue(GlobalConfiguration.SERVER_METRICS_SERVICE_NAME, "from-setting");

    final Map<String, String> attributes = OtelResourceAttributes.resolve(configuration,
        Map.of("OTEL_SERVICE_NAME", "from-env", "OTEL_RESOURCE_ATTRIBUTES", "service.name=from-attributes,deployment.environment=prod"));

    // OTEL_SERVICE_NAME takes precedence over a service.name in OTEL_RESOURCE_ATTRIBUTES, as the OpenTelemetry
    // specification requires; the other attributes are still reported.
    assertThat(attributes.get("service.name")).isEqualTo("from-env");
    assertThat(attributes.get("deployment.environment")).isEqualTo("prod");
  }

  @Test
  void resourceAttributesServiceNameWinsOverTheSetting() {
    final ContextConfiguration configuration = new ContextConfiguration();
    configuration.setValue(GlobalConfiguration.SERVER_METRICS_SERVICE_NAME, "from-setting");

    final Map<String, String> attributes = OtelResourceAttributes.resolve(configuration,
        Map.of("OTEL_RESOURCE_ATTRIBUTES", " service.name = from-attributes , service.namespace=graph "));

    assertThat(attributes.get("service.name")).isEqualTo("from-attributes");
    assertThat(attributes.get("service.namespace")).isEqualTo("graph");
  }

  @Test
  void blankOtelServiceNameIsIgnored() {
    assertThat(OtelResourceAttributes.resolve(new ContextConfiguration(), Map.of("OTEL_SERVICE_NAME", " ")).get("service.name"))
        .isEqualTo("arcadedb");
  }

  @Test
  void resourceAttributeValuesArePercentDecodedAndMalformedEntriesSkipped() {
    final Map<String, String> attributes = OtelResourceAttributes.parseResourceAttributes(
        "team=graph%20db,city=Montr%C3%A9al,plus=a+b,broken=%zz,notutf8=a%FFb,noequals,=novalue,,k8s.pod.name=arcadedb-0");

    assertThat(attributes).containsExactly(
        Map.entry("team", "graph db"),
        Map.entry("city", "Montréal"),
        Map.entry("plus", "a+b"),
        Map.entry("broken", "%zz"),
        Map.entry("notutf8", "a%FFb"),
        Map.entry("k8s.pod.name", "arcadedb-0"));
  }
}
