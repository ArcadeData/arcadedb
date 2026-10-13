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
package com.arcadedb.graph;

import com.arcadedb.database.RID;
import com.arcadedb.schema.Property;
import com.arcadedb.utility.ExcludeFromJacocoGeneratedReport;

import java.util.LinkedHashMap;
import java.util.Map;

@ExcludeFromJacocoGeneratedReport
public interface LightEdge extends Edge {
  byte RECORD_TYPE = 6;

  /**
   * The metadata of a lightweight edge, which is all the content it has: it carries no properties, and its endpoints are
   * also its identity. Both lightweight edge classes build their {@code toMap(true)} and {@code toJSON(true)} from this one
   * place, so the edge a traversal reads and the edge {@code newEdge()} returns cannot disagree (issue #9839). Same keys,
   * same order and same value types as a record-backed edge: {@code @rid} as text, {@code @in}/{@code @out} as {@link RID}.
   *
   * @return a new, mutable map, as every other {@code toMap()} returns
   */
  default Map<String, Object> lightEdgeMetadata() {
    final Map<String, Object> map = new LinkedHashMap<>(8);
    map.put(Property.CAT_PROPERTY, "e");
    map.put(Property.TYPE_PROPERTY, getTypeName());
    map.put(Property.RID_PROPERTY, getIdentity().toString());
    map.put(Property.IN_PROPERTY, getIn());
    map.put(Property.OUT_PROPERTY, getOut());
    return map;
  }
}
