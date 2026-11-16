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
package com.arcadedb.query.opencypher.procedures.meta;

import com.arcadedb.database.Database;
import com.arcadedb.database.Record;
import com.arcadedb.query.opencypher.procedures.CypherProcedure;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Property;
import com.arcadedb.schema.VertexType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.BiConsumer;

/**
 * Abstract base class for meta/schema procedures.
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public abstract class AbstractMetaProcedure implements CypherProcedure {

  protected Map<String, Object> typeToMap(final DocumentType type) {
    final Map<String, Object> result = new HashMap<>();
    result.put("name", type.getName());
    result.put("type", type.getClass().getSimpleName().replace("Type", "").toUpperCase(Locale.ROOT));

    final List<Map<String, Object>> properties = new ArrayList<>();
    for (final String propName : type.getPropertyNames()) {
      final Property property = type.getProperty(propName);
      final Map<String, Object> propMap = new HashMap<>();
      propMap.put("name", propName);
      propMap.put("type", property.getType().name());
      propMap.put("mandatory", property.isMandatory());
      propMap.put("readOnly", property.isReadonly());
      // Issue #6134: the definition, not the evaluated value - this is schema metadata, not a record being written.
      final Object defaultValue = property.getDefaultValueDefinition();
      if (defaultValue != null) {
        propMap.put("defaultValue", defaultValue);
      }
      properties.add(propMap);
    }
    result.put("properties", properties);

    return result;
  }

  /**
   * The optional APOC {@code config} map, the first argument of the procedures that take one; absent or null is the
   * default configuration.
   */
  protected MetaConfig config(final Object[] args) {
    final Map<String, Object> config = optionalMapArg(args, 0, "config");
    return config.isEmpty() ? MetaConfig.DEFAULT : new MetaConfig(config);
  }

  /**
   * Hands {@code action} the nodes APOC's {@code nodeTypeProperties} / {@code relTypeProperties} examine: label by label
   * (the labels {@code config} accepts), every {@link MetaConfig#skipFor}-th node carrying it, with the vertex type it
   * belongs to. A node with several labels is examined once under each of them, as APOC examines it.
   */
  protected static void forEachSampledNode(final Database database, final MetaLabels labels, final MetaConfig config,
      final BiConsumer<VertexType, Record> action) {
    for (final String label : labels.allLabels()) {
      if (!config.acceptsLabel(label))
        continue;
      final List<VertexType> types = labels.typesCarrying(label);
      long count = 0L;
      for (final VertexType type : types)
        count += database.countType(type.getName(), false);
      final long skip = config.skipFor(count);
      long index = 0L;
      for (final VertexType type : types) {
        final Iterator<Record> records = database.iterateType(type.getName(), false);
        while (records.hasNext()) {
          final Record record = records.next();
          if (index++ % skip == 0L)
            action.accept(type, record);
        }
      }
    }
  }
}
