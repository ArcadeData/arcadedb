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
package com.arcadedb.query.polyglot;

import com.arcadedb.log.LogManager;
import org.graalvm.polyglot.Engine;

import java.util.logging.Level;

/**
 * Singleton manager for GraalVM Polyglot Engine instances.
 * The Engine is a heavyweight object that should be shared across multiple Context instances
 * for optimal performance. This manager provides a single shared Engine instance for the entire
 * ArcadeDB process.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class PolyglotEngineManager {
  private static final PolyglotEngineManager INSTANCE = new PolyglotEngineManager();
  private volatile     Engine                sharedEngine;

  private PolyglotEngineManager() {
    // Private constructor for singleton
  }

  public static PolyglotEngineManager getInstance() {
    return INSTANCE;
  }

  /**
   * Returns the shared GraalVM Engine instance. Creates it on first access (lazy initialization).
   * The Engine is thread-safe and can be used concurrently by multiple contexts.
   *
   * @return the shared Engine instance
   */
  public Engine getSharedEngine() {
    if (sharedEngine == null) {
      synchronized (this) {
        if (sharedEngine == null) {
          LogManager.instance()
              .log(this, Level.FINE, "Creating shared GraalVM Polyglot Engine for ArcadeDB process");
          sharedEngine = Engine.create();
        }
      }
    }
    return sharedEngine;
  }

  /**
   * Closes the shared Engine. This should only be called during application shutdown.
   * After calling this method, subsequent calls to getSharedEngine() will create a new Engine.
   */
  public synchronized void closeSharedEngine() {
    if (sharedEngine != null) {
      try {
        LogManager.instance().log(this, Level.FINE, "Closing shared GraalVM Polyglot Engine");
        sharedEngine.close();
      } catch (final Exception e) {
        LogManager.instance().log(this, Level.WARNING, "Error closing shared GraalVM Polyglot Engine", e);
      } finally {
        sharedEngine = null;
      }
    }
  }
}
