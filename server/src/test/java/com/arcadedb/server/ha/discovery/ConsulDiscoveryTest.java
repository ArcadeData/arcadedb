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
package com.arcadedb.server.ha.discovery;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for ConsulDiscovery implementation.
 * Note: These tests focus on initialization and validation.
 * Integration tests with actual Consul would require a Consul agent.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class ConsulDiscoveryTest {

  @Test
  void testConstructorWithValidParameters() {
    // When: Creating discovery with valid parameters
    ConsulDiscovery discovery = new ConsulDiscovery("localhost", 8500, "arcadedb");

    // Then: Discovery is created successfully
    assertThat(discovery).isNotNull();
    assertThat(discovery.getName()).isEqualTo("consul");
  }

  @Test
  void testConstructorWithCustomSettings() {
    // When: Creating discovery with custom datacenter and health settings
    ConsulDiscovery discovery = new ConsulDiscovery(
        "consul.service.consul", 8500, "arcadedb", "dc1", false);

    // Then: Discovery is created successfully
    assertThat(discovery).isNotNull();
    assertThat(discovery.toString()).contains("dc1");
    assertThat(discovery.toString()).contains("onlyHealthy=false");
  }

  @Test
  void testNullConsulAddressThrowsException() {
    // When/Then: Creating discovery with null Consul address
    assertThatThrownBy(() ->
        new ConsulDiscovery(null, 8500, "arcadedb"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Consul address cannot be null or empty");
  }

  @Test
  void testEmptyConsulAddressThrowsException() {
    // When/Then: Creating discovery with empty Consul address
    assertThatThrownBy(() ->
        new ConsulDiscovery("", 8500, "arcadedb"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Consul address cannot be null or empty");
  }

  @Test
  void testWhitespaceConsulAddressThrowsException() {
    // When/Then: Creating discovery with whitespace-only Consul address
    assertThatThrownBy(() ->
        new ConsulDiscovery("   ", 8500, "arcadedb"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Consul address cannot be null or empty");
  }

  @Test
  void testInvalidConsulPortThrowsException() {
    // When/Then: Creating discovery with invalid port (0)
    assertThatThrownBy(() ->
        new ConsulDiscovery("localhost", 0, "arcadedb"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Consul port must be between 1 and 65535");
  }

  @Test
  void testNegativeConsulPortThrowsException() {
    // When/Then: Creating discovery with negative port
    assertThatThrownBy(() ->
        new ConsulDiscovery("localhost", -1, "arcadedb"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Consul port must be between 1 and 65535");
  }

  @Test
  void testConsulPortTooHighThrowsException() {
    // When/Then: Creating discovery with port > 65535
    assertThatThrownBy(() ->
        new ConsulDiscovery("localhost", 70000, "arcadedb"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Consul port must be between 1 and 65535");
  }

  @Test
  void testNullServiceNameThrowsException() {
    // When/Then: Creating discovery with null service name
    assertThatThrownBy(() ->
        new ConsulDiscovery("localhost", 8500, null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Service name cannot be null or empty");
  }

  @Test
  void testEmptyServiceNameThrowsException() {
    // When/Then: Creating discovery with empty service name
    assertThatThrownBy(() ->
        new ConsulDiscovery("localhost", 8500, ""))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Service name cannot be null or empty");
  }

  @Test
  void testWhitespaceServiceNameThrowsException() {
    // When/Then: Creating discovery with whitespace-only service name
    assertThatThrownBy(() ->
        new ConsulDiscovery("localhost", 8500, "   "))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Service name cannot be null or empty");
  }

  @Test
  void testGetName() {
    // Given: A Consul discovery service
    ConsulDiscovery discovery = new ConsulDiscovery("localhost", 8500, "arcadedb");

    // When/Then: Name is "consul"
    assertThat(discovery.getName()).isEqualTo("consul");
  }

  @Test
  void testToStringDefaultSettings() {
    // Given: A Consul discovery service with default settings
    ConsulDiscovery discovery = new ConsulDiscovery("localhost", 8500, "arcadedb");

    // When: Converting to string
    String result = discovery.toString();

    // Then: String contains configuration details
    assertThat(result).contains("ConsulDiscovery");
    assertThat(result).contains("localhost");
    assertThat(result).contains("8500");
    assertThat(result).contains("arcadedb");
    assertThat(result).contains("onlyHealthy=true");
  }

  @Test
  void testToStringCustomSettings() {
    // Given: A Consul discovery service with custom settings
    ConsulDiscovery discovery = new ConsulDiscovery(
        "consul.example.com", 8500, "arcadedb", "production", false);

    // When: Converting to string
    String result = discovery.toString();

    // Then: String contains all configuration details
    assertThat(result).contains("ConsulDiscovery");
    assertThat(result).contains("consul.example.com");
    assertThat(result).contains("8500");
    assertThat(result).contains("arcadedb");
    assertThat(result).contains("production");
    assertThat(result).contains("onlyHealthy=false");
  }

  @Test
  void testValidPortBoundaries() {
    // When: Creating discovery with port 1 (minimum valid)
    ConsulDiscovery discovery1 = new ConsulDiscovery("localhost", 1, "arcadedb");
    assertThat(discovery1).isNotNull();

    // When: Creating discovery with port 65535 (maximum valid)
    ConsulDiscovery discovery2 = new ConsulDiscovery("localhost", 65535, "arcadedb");
    assertThat(discovery2).isNotNull();
  }

  @Test
  void testDefaultConstructorSetsOnlyHealthyToTrue() {
    // Given: A Consul discovery service created with default constructor
    ConsulDiscovery discovery = new ConsulDiscovery("localhost", 8500, "arcadedb");

    // When: Converting to string
    String result = discovery.toString();

    // Then: onlyHealthy is true by default
    assertThat(result).contains("onlyHealthy=true");
  }

  @Test
  void testCustomConstructorAllowsOnlyHealthyFalse() {
    // Given: A Consul discovery service with onlyHealthy=false
    ConsulDiscovery discovery = new ConsulDiscovery("localhost", 8500, "arcadedb", null, false);

    // When: Converting to string
    String result = discovery.toString();

    // Then: onlyHealthy is false
    assertThat(result).contains("onlyHealthy=false");
  }

  @Test
  void testNullDatacenterInToString() {
    // Given: A Consul discovery service with null datacenter
    ConsulDiscovery discovery = new ConsulDiscovery("localhost", 8500, "arcadedb", null, true);

    // When: Converting to string
    String result = discovery.toString();

    // Then: datacenter shows as null
    assertThat(result).contains("datacenter=null");
  }

  @Test
  void testNonNullDatacenterInToString() {
    // Given: A Consul discovery service with datacenter specified
    ConsulDiscovery discovery = new ConsulDiscovery("localhost", 8500, "arcadedb", "dc1", true);

    // When: Converting to string
    String result = discovery.toString();

    // Then: datacenter shows as dc1
    assertThat(result).contains("datacenter=dc1");
  }
}
