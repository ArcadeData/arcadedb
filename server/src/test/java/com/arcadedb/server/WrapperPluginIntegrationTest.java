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
package com.arcadedb.server;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration test for wrapper plugin loading mechanism.
 */
public class WrapperPluginIntegrationTest {

  @Test
  public void testWrapperPluginDetection() {
    // Test that all wrapper plugin class names are correctly identified
    assertTrue(WrapperPluginClassLoader.isWrapperPlugin("com.arcadedb.mongo.MongoDBProtocolPlugin"));
    assertTrue(WrapperPluginClassLoader.isWrapperPlugin("com.arcadedb.redis.RedisProtocolPlugin"));
    assertTrue(WrapperPluginClassLoader.isWrapperPlugin("com.arcadedb.postgres.PostgresProtocolPlugin"));
    assertTrue(WrapperPluginClassLoader.isWrapperPlugin("com.arcadedb.server.gremlin.GremlinServerPlugin"));

    // Test that non-wrapper plugins are not identified as wrappers
    assertFalse(WrapperPluginClassLoader.isWrapperPlugin("com.arcadedb.metrics.prometheus.PrometheusMetricsPlugin"));
    assertFalse(WrapperPluginClassLoader.isWrapperPlugin("com.arcadedb.server.grpc.GrpcServerPlugin"));
  }

  @Test
  public void testWrapperPluginNameExtraction() {
    assertEquals("MongoDB", WrapperPluginClassLoader.getWrapperPluginName("com.arcadedb.mongo.MongoDBProtocolPlugin"));
    assertEquals("Redis", WrapperPluginClassLoader.getWrapperPluginName("com.arcadedb.redis.RedisProtocolPlugin"));
    assertEquals("PostgreSQL", WrapperPluginClassLoader.getWrapperPluginName("com.arcadedb.postgres.PostgresProtocolPlugin"));
    assertEquals("Gremlin", WrapperPluginClassLoader.getWrapperPluginName("com.arcadedb.server.gremlin.GremlinServerPlugin"));

    assertNull(WrapperPluginClassLoader.getWrapperPluginName("com.arcadedb.metrics.prometheus.PrometheusMetricsPlugin"));
  }

  /**
   * This test demonstrates the key functionality: wrapper plugins should use dedicated class loaders
   * while regular plugins use the main class loader.
   */
  @Test
  public void testPluginClassLoaderIsolation() {
    // Test wrapper plugin class loader creation
    final WrapperPluginClassLoader mongoLoader = WrapperPluginClassLoader.getOrCreateClassLoader(
        "MongoDB",
        new java.net.URL[0],
        Thread.currentThread().getContextClassLoader()
    );

    final WrapperPluginClassLoader redisLoader = WrapperPluginClassLoader.getOrCreateClassLoader(
        "Redis",
        new java.net.URL[0],
        Thread.currentThread().getContextClassLoader()
    );

    // Verify that different wrapper plugins get different class loaders
    assertNotSame(mongoLoader, redisLoader);

    // Verify that the same plugin name returns the same class loader (singleton pattern)
    final WrapperPluginClassLoader mongoLoader2 = WrapperPluginClassLoader.getOrCreateClassLoader(
        "MongoDB",
        new java.net.URL[0],
        Thread.currentThread().getContextClassLoader()
    );

    assertSame(mongoLoader, mongoLoader2);

    // Verify that wrapper class loaders are different from the main class loader
    assertNotSame(mongoLoader, Thread.currentThread().getContextClassLoader());
    assertNotSame(redisLoader, Thread.currentThread().getContextClassLoader());
  }

  @Test
  public void testClassLoaderCleanup() {
    // Create some class loaders
    WrapperPluginClassLoader.getOrCreateClassLoader(
        "TestPlugin1",
        new java.net.URL[0],
        Thread.currentThread().getContextClassLoader()
    );

    WrapperPluginClassLoader.getOrCreateClassLoader(
        "TestPlugin2",
        new java.net.URL[0],
        Thread.currentThread().getContextClassLoader()
    );

    // This should not throw an exception and should clean up all class loaders
    assertDoesNotThrow(() -> WrapperPluginClassLoader.closeAllClassLoaders());
  }
}
