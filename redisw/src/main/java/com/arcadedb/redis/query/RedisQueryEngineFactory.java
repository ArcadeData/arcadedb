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
package com.arcadedb.redis.query;

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.exception.CommandParsingException;
import com.arcadedb.log.LogManager;
import com.arcadedb.query.QueryEngine;

import java.util.logging.*;

/**
 * Factory for creating RedisQueryEngine instances.
 * Registered at runtime if the redisw module is on the classpath.
 *
 * @see <a href="https://github.com/ArcadeData/arcadedb/issues/1010">Issue #1010</a>
 */
public class RedisQueryEngineFactory implements QueryEngine.QueryEngineFactory {

  @Override
  public String getLanguage() {
    return RedisQueryEngine.ENGINE_NAME;
  }

  @Override
  public QueryEngine getInstance(final DatabaseInternal database) {
    Object engine = database.getWrappers().get(RedisQueryEngine.ENGINE_NAME);
    if (engine != null) {
      return (RedisQueryEngine) engine;
    }

    try {
      engine = new RedisQueryEngine(database);
      database.setWrapper(RedisQueryEngine.ENGINE_NAME, engine);
      return (RedisQueryEngine) engine;
    } catch (final Throwable e) {
      LogManager.instance().log(this, Level.SEVERE, "Error on initializing Redis query engine", e);
      throw new CommandParsingException("Error on initializing Redis query engine", e);
    }
  }
}
