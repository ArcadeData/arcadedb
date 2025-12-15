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

import com.arcadedb.server.ha.HAServer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.concurrent.TimeUnit;

/**
 * Unit tests for KubernetesDnsDiscovery implementation.
 * Note: These tests focus on initialization and validation.
 * Integration tests with actual Kubernetes DNS would require a K8s environment.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class KubernetesDnsDiscoveryTest {

  @Test
  @Timeout(value = 5, unit = TimeUnit.MINUTES)
  void testConstructorWithValidParameters() {
    // When: Creating discovery with valid parameters
    KubernetesDnsDiscovery discovery = new KubernetesDnsDiscovery(
        "arcadedb-headless", "default", "arcadedb", 2424);

    // Then: Discovery is created successfully
    assertThat(discovery).isNotNull();
    assertThat(discovery.getName()).isEqualTo("kubernetes");
  }

  @Test
  @Timeout(value = 5, unit = TimeUnit.MINUTES)
  void testConstructorWithCustomDomain() {
    // When: Creating discovery with custom domain
    KubernetesDnsDiscovery discovery = new KubernetesDnsDiscovery(
        "arcadedb-headless", "production", "arcadedb", 2424, "custom.local");

    // Then: Discovery is created successfully
    assertThat(discovery).isNotNull();
    assertThat(discovery.toString()).contains("custom.local");
  }

  @Test
  @Timeout(value = 5, unit = TimeUnit.MINUTES)
  void testNullServiceNameThrowsException() {
    // When/Then: Creating discovery with null service name
    assertThatThrownBy(() ->
        new KubernetesDnsDiscovery(null, "default", "arcadedb", 2424))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Service name cannot be null or empty");
  }

  @Test
  @Timeout(value = 5, unit = TimeUnit.MINUTES)
  void testEmptyServiceNameThrowsException() {
    // When/Then: Creating discovery with empty service name
    assertThatThrownBy(() ->
        new KubernetesDnsDiscovery("", "default", "arcadedb", 2424))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Service name cannot be null or empty");
  }

  @Test
  @Timeout(value = 5, unit = TimeUnit.MINUTES)
  void testNullNamespaceThrowsException() {
    // When/Then: Creating discovery with null namespace
    assertThatThrownBy(() ->
        new KubernetesDnsDiscovery("arcadedb", null, "arcadedb", 2424))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Namespace cannot be null or empty");
  }

  @Test
  @Timeout(value = 5, unit = TimeUnit.MINUTES)
  void testEmptyNamespaceThrowsException() {
    // When/Then: Creating discovery with empty namespace
    assertThatThrownBy(() ->
        new KubernetesDnsDiscovery("arcadedb", "", "arcadedb", 2424))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Namespace cannot be null or empty");
  }

  @Test
  @Timeout(value = 5, unit = TimeUnit.MINUTES)
  void testNullPortNameThrowsException() {
    // When/Then: Creating discovery with null port name
    assertThatThrownBy(() ->
        new KubernetesDnsDiscovery("arcadedb", "default", null, 2424))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Port name cannot be null or empty");
  }

  @Test
  @Timeout(value = 5, unit = TimeUnit.MINUTES)
  void testEmptyPortNameThrowsException() {
    // When/Then: Creating discovery with empty port name
    assertThatThrownBy(() ->
        new KubernetesDnsDiscovery("arcadedb", "default", "", 2424))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Port name cannot be null or empty");
  }

  @Test
  @Timeout(value = 5, unit = TimeUnit.MINUTES)
  void testInvalidPortThrowsException() {
    // When/Then: Creating discovery with invalid port (0)
    assertThatThrownBy(() ->
        new KubernetesDnsDiscovery("arcadedb", "default", "arcadedb", 0))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Default port must be between 1 and 65535");
  }

  @Test
  @Timeout(value = 5, unit = TimeUnit.MINUTES)
  void testPortTooHighThrowsException() {
    // When/Then: Creating discovery with port > 65535
    assertThatThrownBy(() ->
        new KubernetesDnsDiscovery("arcadedb", "default", "arcadedb", 70000))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Default port must be between 1 and 65535");
  }

  @Test
  @Timeout(value = 5, unit = TimeUnit.MINUTES)
  void testNegativePortThrowsException() {
    // When/Then: Creating discovery with negative port
    assertThatThrownBy(() ->
        new KubernetesDnsDiscovery("arcadedb", "default", "arcadedb", -1))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Default port must be between 1 and 65535");
  }

  @Test
  @Timeout(value = 5, unit = TimeUnit.MINUTES)
  void testNullDomainThrowsException() {
    // When/Then: Creating discovery with null domain
    assertThatThrownBy(() ->
        new KubernetesDnsDiscovery("arcadedb", "default", "arcadedb", 2424, null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Domain cannot be null or empty");
  }

  @Test
  @Timeout(value = 5, unit = TimeUnit.MINUTES)
  void testEmptyDomainThrowsException() {
    // When/Then: Creating discovery with empty domain
    assertThatThrownBy(() ->
        new KubernetesDnsDiscovery("arcadedb", "default", "arcadedb", 2424, ""))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Domain cannot be null or empty");
  }

  @Test
  @Timeout(value = 5, unit = TimeUnit.MINUTES)
  void testRegisterNodeIsNoOp() throws DiscoveryException {
    // Given: A Kubernetes DNS discovery service
    KubernetesDnsDiscovery discovery = new KubernetesDnsDiscovery(
        "arcadedb-headless", "default", "arcadedb", 2424);
    HAServer.ServerInfo server = new HAServer.ServerInfo("arcadedb-0", 2424, "arcadedb-0");

    // When/Then: Registering a node doesn't throw exception (no-op)
    discovery.registerNode(server);
  }

  @Test
  @Timeout(value = 5, unit = TimeUnit.MINUTES)
  void testDeregisterNodeIsNoOp() throws DiscoveryException {
    // Given: A Kubernetes DNS discovery service
    KubernetesDnsDiscovery discovery = new KubernetesDnsDiscovery(
        "arcadedb-headless", "default", "arcadedb", 2424);
    HAServer.ServerInfo server = new HAServer.ServerInfo("arcadedb-0", 2424, "arcadedb-0");

    // When/Then: Deregistering a node doesn't throw exception (no-op)
    discovery.deregisterNode(server);
  }

  @Test
  @Timeout(value = 5, unit = TimeUnit.MINUTES)
  void testGetName() {
    // Given: A Kubernetes DNS discovery service
    KubernetesDnsDiscovery discovery = new KubernetesDnsDiscovery(
        "arcadedb-headless", "default", "arcadedb", 2424);

    // When/Then: Name is "kubernetes"
    assertThat(discovery.getName()).isEqualTo("kubernetes");
  }

  @Test
  @Timeout(value = 5, unit = TimeUnit.MINUTES)
  void testToString() {
    // Given: A Kubernetes DNS discovery service
    KubernetesDnsDiscovery discovery = new KubernetesDnsDiscovery(
        "arcadedb-headless", "production", "arcadedb", 2424, "custom.local");

    // When: Converting to string
    String result = discovery.toString();

    // Then: String contains configuration details
    assertThat(result).contains("KubernetesDnsDiscovery");
    assertThat(result).contains("arcadedb-headless");
    assertThat(result).contains("production");
    assertThat(result).contains("arcadedb");
    assertThat(result).contains("custom.local");
  }

  @Test
  @Timeout(value = 5, unit = TimeUnit.MINUTES)
  void testWhitespaceServiceNameThrowsException() {
    // When/Then: Creating discovery with whitespace-only service name
    assertThatThrownBy(() ->
        new KubernetesDnsDiscovery("   ", "default", "arcadedb", 2424))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Service name cannot be null or empty");
  }

  @Test
  @Timeout(value = 5, unit = TimeUnit.MINUTES)
  void testWhitespaceNamespaceThrowsException() {
    // When/Then: Creating discovery with whitespace-only namespace
    assertThatThrownBy(() ->
        new KubernetesDnsDiscovery("arcadedb", "   ", "arcadedb", 2424))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Namespace cannot be null or empty");
  }

  @Test
  @Timeout(value = 5, unit = TimeUnit.MINUTES)
  void testWhitespacePortNameThrowsException() {
    // When/Then: Creating discovery with whitespace-only port name
    assertThatThrownBy(() ->
        new KubernetesDnsDiscovery("arcadedb", "default", "   ", 2424))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Port name cannot be null or empty");
  }

  @Test
  @Timeout(value = 5, unit = TimeUnit.MINUTES)
  void testWhitespaceDomainThrowsException() {
    // When/Then: Creating discovery with whitespace-only domain
    assertThatThrownBy(() ->
        new KubernetesDnsDiscovery("arcadedb", "default", "arcadedb", 2424, "   "))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Domain cannot be null or empty");
  }

  @Test
  @Timeout(value = 5, unit = TimeUnit.MINUTES)
  void testValidPortBoundaries() {
    // When: Creating discovery with port 1 (minimum valid)
    KubernetesDnsDiscovery discovery1 = new KubernetesDnsDiscovery(
        "arcadedb", "default", "arcadedb", 1);
    assertThat(discovery1).isNotNull();

    // When: Creating discovery with port 65535 (maximum valid)
    KubernetesDnsDiscovery discovery2 = new KubernetesDnsDiscovery(
        "arcadedb", "default", "arcadedb", 65535);
    assertThat(discovery2).isNotNull();
  }
}
