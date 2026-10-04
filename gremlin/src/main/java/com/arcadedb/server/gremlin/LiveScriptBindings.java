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
package com.arcadedb.server.gremlin;

import javax.script.Bindings;

import java.util.AbstractMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * The global bindings of a script engine, with the graphs and traversal sources of the {@link ArcadeGraphManager} read at the
 * moment a script runs instead of when the Gremlin executor was built (issue #9147).
 * <p>
 * TinkerPop copies {@link ArcadeGraphManager#getAsBindings()} once, while the server starts, and every script is then evaluated
 * against that copy. A server that starts with no database, and gets its databases afterwards (Studio, {@code POST /api/v1/server},
 * an embedded {@code getOrCreateDatabase()}), therefore never had {@code g} bound for a script: "No such property: g". Reading the
 * manager on each lookup also keeps a traversal source from outliving the database it wraps, which a copy did.
 * <p>
 * Everything else a plugin or the engine puts in the global scope lives in the {@code base} bindings, which stay writable; the
 * manager's entries take precedence over a base entry of the same name.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class LiveScriptBindings extends AbstractMap<String, Object> implements Bindings {
  private final Bindings           base;
  private final ArcadeGraphManager graphManager;

  LiveScriptBindings(final Bindings base, final ArcadeGraphManager graphManager) {
    this.base = base;
    this.graphManager = graphManager;
  }

  @Override
  public Object get(final Object key) {
    final Object live = graphManager.getAsBindings().get(key);
    return live != null ? live : base.get(key);
  }

  @Override
  public boolean containsKey(final Object key) {
    return graphManager.getAsBindings().containsKey(key) || base.containsKey(key);
  }

  @Override
  public Object put(final String name, final Object value) {
    return base.put(name, value);
  }

  @Override
  public Object remove(final Object key) {
    return base.remove(key);
  }

  @Override
  public void clear() {
    base.clear();
  }

  @Override
  public Set<Entry<String, Object>> entrySet() {
    final Map<String, Object> merged = new LinkedHashMap<>(base);
    merged.putAll(graphManager.getAsBindings());
    return merged.entrySet();
  }
}
