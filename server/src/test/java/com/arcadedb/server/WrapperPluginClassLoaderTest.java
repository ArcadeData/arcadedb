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

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test for the WrapperPluginClassLoader.
 */
public class WrapperPluginClassLoaderTest {

  @Test
  public void testIsWrapperPlugin() {
    assertTrue(WrapperPluginClassLoader.isWrapperPlugin("com.arcadedb.mongo.MongoDBProtocolPlugin"));
    assertTrue(WrapperPluginClassLoader.isWrapperPlugin("com.arcadedb.redis.RedisProtocolPlugin"));
    assertTrue(WrapperPluginClassLoader.isWrapperPlugin("com.arcadedb.postgres.PostgresProtocolPlugin"));
    assertTrue(WrapperPluginClassLoader.isWrapperPlugin("com.arcadedb.server.gremlin.GremlinServerPlugin"));
    
    assertFalse(WrapperPluginClassLoader.isWrapperPlugin("com.arcadedb.server.http.HttpServerPlugin"));
    assertFalse(WrapperPluginClassLoader.isWrapperPlugin("com.example.SomeOtherPlugin"));
    assertFalse(WrapperPluginClassLoader.isWrapperPlugin(null));
  }

  @Test
  public void testGetWrapperPluginName() {
    assertEquals("MongoDB", WrapperPluginClassLoader.getWrapperPluginName("com.arcadedb.mongo.MongoDBProtocolPlugin"));
    assertEquals("Redis", WrapperPluginClassLoader.getWrapperPluginName("com.arcadedb.redis.RedisProtocolPlugin"));
    assertEquals("PostgreSQL", WrapperPluginClassLoader.getWrapperPluginName("com.arcadedb.postgres.PostgresProtocolPlugin"));
    assertEquals("Gremlin", WrapperPluginClassLoader.getWrapperPluginName("com.arcadedb.server.gremlin.GremlinServerPlugin"));
    
    assertNull(WrapperPluginClassLoader.getWrapperPluginName("com.arcadedb.server.http.HttpServerPlugin"));
    assertNull(WrapperPluginClassLoader.getWrapperPluginName("com.example.SomeOtherPlugin"));
    assertNull(WrapperPluginClassLoader.getWrapperPluginName(null));
  }

  @Test
  public void testCreateClassLoader() {
    final URL[] urls = new URL[0];
    final ClassLoader parentClassLoader = Thread.currentThread().getContextClassLoader();
    
    final WrapperPluginClassLoader classLoader1 = WrapperPluginClassLoader.getOrCreateClassLoader("TestPlugin", urls, parentClassLoader);
    assertNotNull(classLoader1);
    
    // Should return the same instance for the same plugin name
    final WrapperPluginClassLoader classLoader2 = WrapperPluginClassLoader.getOrCreateClassLoader("TestPlugin", urls, parentClassLoader);
    assertSame(classLoader1, classLoader2);
    
    // Should create a different instance for a different plugin name
    final WrapperPluginClassLoader classLoader3 = WrapperPluginClassLoader.getOrCreateClassLoader("AnotherTestPlugin", urls, parentClassLoader);
    assertNotSame(classLoader1, classLoader3);
  }

  @Test
  public void testCloseAllClassLoaders() {
    final URL[] urls = new URL[0];
    final ClassLoader parentClassLoader = Thread.currentThread().getContextClassLoader();
    
    WrapperPluginClassLoader.getOrCreateClassLoader("TestPlugin1", urls, parentClassLoader);
    WrapperPluginClassLoader.getOrCreateClassLoader("TestPlugin2", urls, parentClassLoader);
    
    // This should not throw an exception
    assertDoesNotThrow(() -> WrapperPluginClassLoader.closeAllClassLoaders());
  }
}