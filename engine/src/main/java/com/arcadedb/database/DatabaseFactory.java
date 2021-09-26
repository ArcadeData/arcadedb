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
 */
package com.arcadedb.database;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.engine.PaginatedFile;
import com.arcadedb.schema.EmbeddedSchema;
import com.arcadedb.security.SecurityManager;

import java.io.*;
import java.nio.charset.*;
import java.util.*;
import java.util.concurrent.*;

public class DatabaseFactory implements AutoCloseable {
  private final        ContextConfiguration                                       contextConfiguration = new ContextConfiguration();
  private final        String                                                     databasePath;
  private final        Map<DatabaseInternal.CALLBACK_EVENT, List<Callable<Void>>> callbacks            = new HashMap<>();
  private final static Charset                                                    DEFAULT_CHARSET      = StandardCharsets.UTF_8;
  private              SecurityManager                                            security;
  private              boolean                                                    autoTransaction      = false;

  public DatabaseFactory(final String path) {
    if (path == null || path.isEmpty())
      throw new IllegalArgumentException("Missing path");

    if (path.endsWith("/"))
      databasePath = path.substring(0, path.length() - 1);
    else
      databasePath = path;
  }

  @Override
  public synchronized void close() {
    callbacks.clear();
  }

  public boolean exists() {
    boolean exists = new File(databasePath + "/" + EmbeddedSchema.SCHEMA_FILE_NAME).exists();
    if (!exists)
      exists = new File(databasePath + "/" + EmbeddedSchema.SCHEMA_PREV_FILE_NAME).exists();
    return exists;
  }

  public Database open() {
    return open(PaginatedFile.MODE.READ_WRITE);
  }

  public synchronized Database open(final PaginatedFile.MODE mode) {
    final EmbeddedDatabase database = new EmbeddedDatabase(databasePath, mode, contextConfiguration, security, callbacks);
    database.setAutoTransaction(autoTransaction);
    database.open();
    return database;
  }

  public synchronized Database create() {
    final EmbeddedDatabase database = new EmbeddedDatabase(databasePath, PaginatedFile.MODE.READ_WRITE, contextConfiguration, security, callbacks);
    database.setAutoTransaction(autoTransaction);
    database.create();
    return database;
  }

  public synchronized DatabaseFactory setAutoTransaction(final boolean enabled) {
    autoTransaction = enabled;
    return this;
  }

  public ContextConfiguration getContextConfiguration() {
    return contextConfiguration;
  }

  public static Charset getDefaultCharset() {
    return DEFAULT_CHARSET;
  }

  public SecurityManager getSecurity() {
    return security;
  }

  public DatabaseFactory setSecurity(final SecurityManager security) {
    this.security = security;
    return this;
  }

  /**
   * Test only API
   */
  public void registerCallback(final DatabaseInternal.CALLBACK_EVENT event, Callable<Void> callback) {
    List<Callable<Void>> callbacks = this.callbacks.get(event);
    if (callbacks == null) {
      callbacks = new ArrayList<>();
      this.callbacks.put(event, callbacks);
    }
    callbacks.add(callback);
  }
}
