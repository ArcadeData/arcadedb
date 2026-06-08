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
package com.arcadedb.bolt;

import com.arcadedb.query.QuerySession;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * The per-connection {@link QuerySession} for the Bolt protocol: it carries the session parameters set with
 * GQL {@code SESSION SET} so later commands on the same connection see them.
 * <p>
 * A Bolt connection runs in a single dedicated thread, so this state needs no synchronization. The Bolt
 * connection and its transaction lifecycle are governed by the Bolt protocol (BEGIN/COMMIT/ROLLBACK/RESET
 * messages and the driver's own session), so here {@code SESSION CLOSE} only clears the session parameters -
 * it does not tear down the connection or its transaction.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class BoltSession implements QuerySession {
  private final Map<String, Object> parameters = new HashMap<>();
  // Cached live read-only view over 'parameters' (reflects mutations), so getParameters() allocates nothing.
  private final Map<String, Object> parametersView = Collections.unmodifiableMap(parameters);

  @Override
  public void setParameter(final String name, final Object value) {
    parameters.put(name, value);
  }

  @Override
  public Map<String, Object> getParameters() {
    return parametersView;
  }

  @Override
  public void reset() {
    parameters.clear();
  }

  @Override
  public void close() {
    // The Bolt connection/transaction lifecycle is governed by the protocol; the session only owns its
    // parameters, so closing it clears them.
    parameters.clear();
  }
}
