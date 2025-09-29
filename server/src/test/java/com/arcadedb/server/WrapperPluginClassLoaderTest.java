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

import java.net.URL;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * Test for the WrapperPluginClassLoader.
 */
public class WrapperPluginClassLoaderTest {

  @Test
  public void testIsWrapperPlugin() {
    assertThat(WrapperPluginClassLoader.isWrapperPlugin("com.arcadedb.mongo.MongoDBProtocolPlugin")).isTrue();
    assertThat(WrapperPluginClassLoader.isWrapperPlugin("com.arcadedb.redis.RedisProtocolPlugin")).isTrue();
    assertThat(WrapperPluginClassLoader.isWrapperPlugin("com.arcadedb.postgres.PostgresProtocolPlugin")).isTrue();
    assertThat(WrapperPluginClassLoader.isWrapperPlugin("com.arcadedb.server.gremlin.GremlinServerPlugin")).isTrue();

    assertThat(WrapperPluginClassLoader.isWrapperPlugin("com.arcadedb.server.http.HttpServerPlugin")).isFalse();
    assertThat(WrapperPluginClassLoader.isWrapperPlugin("com.example.SomeOtherPlugin")).isFalse();
    assertThat(WrapperPluginClassLoader.isWrapperPlugin(null)).isFalse();
  }

  @Test
  public void testGetWrapperPluginName() {
    assertThat(WrapperPluginClassLoader.getWrapperPluginName("com.arcadedb.mongo.MongoDBProtocolPlugin")).isEqualTo("MongoDB");
    assertThat(WrapperPluginClassLoader.getWrapperPluginName("com.arcadedb.redis.RedisProtocolPlugin")).isEqualTo("Redis");
    assertThat(WrapperPluginClassLoader.getWrapperPluginName("com.arcadedb.postgres.PostgresProtocolPlugin")).isEqualTo("PostgreSQL");
    assertThat(WrapperPluginClassLoader.getWrapperPluginName("com.arcadedb.server.gremlin.GremlinServerPlugin")).isEqualTo("Gremlin");

    assertThat(WrapperPluginClassLoader.getWrapperPluginName("com.arcadedb.server.http.HttpServerPlugin")).isNull();
    assertThat(WrapperPluginClassLoader.getWrapperPluginName("com.example.SomeOtherPlugin")).isNull();
    assertThat(WrapperPluginClassLoader.getWrapperPluginName(null)).isNull();
  }

  @Test
  public void testCreateClassLoader() {
    final URL[] urls = new URL[0];
    final ClassLoader parentClassLoader = Thread.currentThread().getContextClassLoader();

    final WrapperPluginClassLoader classLoader1 = WrapperPluginClassLoader.getOrCreateClassLoader("TestPlugin", urls, parentClassLoader);
    assertThat(classLoader1).isNotNull();

    // Should return the same instance for the same plugin name
    final WrapperPluginClassLoader classLoader2 = WrapperPluginClassLoader.getOrCreateClassLoader("TestPlugin", urls, parentClassLoader);
    assertThat(classLoader2).isSameAs(classLoader1);

    // Should create a different instance for a different plugin name
    final WrapperPluginClassLoader classLoader3 = WrapperPluginClassLoader.getOrCreateClassLoader("AnotherTestPlugin", urls, parentClassLoader);
    assertThat(classLoader3).isNotSameAs(classLoader1);
  }

  @Test
  public void testCloseAllClassLoaders() {
    final URL[] urls = new URL[0];
    final ClassLoader parentClassLoader = Thread.currentThread().getContextClassLoader();

    WrapperPluginClassLoader.getOrCreateClassLoader("TestPlugin1", urls, parentClassLoader);
    WrapperPluginClassLoader.getOrCreateClassLoader("TestPlugin2", urls, parentClassLoader);

    // This should not throw an exception
    assertThatCode(() -> WrapperPluginClassLoader.closeAllClassLoaders()).doesNotThrowAnyException();
  }
}
