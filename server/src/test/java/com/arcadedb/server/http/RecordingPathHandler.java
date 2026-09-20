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
package com.arcadedb.server.http;

import io.undertow.server.HttpHandler;
import io.undertow.server.handlers.PathHandler;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

/**
 * A {@link PathHandler} that records every path registered through {@code addExactPath}/
 * {@code addPrefixPath}, so a plugin's {@code registerAPI} can be exercised directly and its actual
 * routes compared against the paths declared for it in the OpenAPI spec (issue #4896's
 * per-plugin-module anti-drift check). Behaves identically to a plain {@code PathHandler} as a
 * router; recording is a side effect only.
 * <p>
 * The handler INSTANCES are recorded alongside the paths (issue #7861), so a test can also ask a question of the
 * handlers themselves - "does every route that blocks dispatch off the Undertow IO thread" - against the real
 * inventory {@code registerAPI} installs rather than a list somebody maintains by hand and forgets.
 */
public class RecordingPathHandler extends PathHandler {

  private final Map<String, HttpHandler> registered = new LinkedHashMap<>();

  @Override
  public synchronized PathHandler addExactPath(final String path, final HttpHandler handler) {
    registered.put(path, handler);
    return super.addExactPath(path, handler);
  }

  @Override
  public synchronized PathHandler addPrefixPath(final String path, final HttpHandler handler) {
    registered.put(path, handler);
    return super.addPrefixPath(path, handler);
  }

  public synchronized Set<String> getRegisteredPaths() {
    return new LinkedHashSet<>(registered.keySet());
  }

  /** Every registered route's handler, keyed by the path it was registered at, in registration order. */
  public synchronized Map<String, HttpHandler> getRegisteredHandlers() {
    return new LinkedHashMap<>(registered);
  }
}
