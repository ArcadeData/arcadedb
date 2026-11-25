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
package com.arcadedb.function.map;

import com.arcadedb.database.Document;
import com.arcadedb.function.StatelessFunction;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Abstract base class for map functions.
 * All map functions share the "map." namespace prefix.
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public abstract class AbstractMapFunction implements StatelessFunction {
  protected static final String NAMESPACE = "map";

  /**
   * Returns the simple name without namespace prefix.
   */
  protected abstract String getSimpleName();

  @Override
  public String getName() {
    return NAMESPACE + "." + getSimpleName();
  }

  /**
   * Safely extract a Map from an argument. A node or relationship is the map of its properties, as for an APOC map argument.
   */
  @SuppressWarnings("unchecked")
  protected Map<String, Object> asMap(final Object arg) {
    if (arg == null)
      return null;
    if (arg instanceof Map) {
      return (Map<String, Object>) arg;
    }
    // A copy on purpose: it holds the properties only (no @rid, @type and the other metadata entries Document.toMap() adds), and only
    // a node or relationship argument pays for it, a map argument is read as it is
    if (arg instanceof Document document) {
      final Map<String, Object> properties = new LinkedHashMap<>();
      for (final String name : document.getPropertyNames())
        properties.put(name, document.get(name));
      return properties;
    }
    throw new IllegalArgumentException("Expected a map, got " + arg.getClass().getSimpleName());
  }
}
