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
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.schema.VertexType;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * Procedure: meta.nodeTypeProperties([config])
 * <p>
 * Returns, as APOC's {@code apoc.meta.nodeTypeProperties}, one row per property of each label set found in the data:
 * {@code nodeType} ({@code :`A`:`B`}), {@code nodeLabels}, {@code propertyName}, {@code propertyTypes} (the APOC names of
 * the value types seen), {@code mandatory}, {@code propertyObservations} and {@code totalObservations}. The nodes are
 * sampled label by label with APOC's {@code sample} (default 1000, -1 reads them all); a label set with no property gets
 * one row with a null property. Properties the schema declares are reported too, with 0 observations when no examined
 * node carried them; {@code mandatory} is the declaration's. The config also takes {@code includeLabels} and
 * {@code excludeLabels}.
 * </p>
 * <p>
 * Example Cypher usage:
 * <pre>
 * CALL meta.nodeTypeProperties()
 * YIELD nodeType, propertyName, propertyTypes, mandatory
 * </pre>
 * </p>
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public class MetaNodeTypeProperties extends AbstractMetaProcedure {
  public static final String NAME = "meta.nodetypeproperties";

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public int getMinArgs() {
    return 0;
  }

  @Override
  public int getMaxArgs() {
    return 1;
  }

  @Override
  public String getDescription() {
    return "Returns the properties found on each node label set, with their types and how often they occur";
  }

  @Override
  public List<String> getYieldFields() {
    return List.of("nodeType", "nodeLabels", "propertyName", "propertyTypes", "mandatory", "propertyObservations",
        "totalObservations");
  }

  @Override
  public Stream<Result> execute(final Object[] args, final Result inputRow, final CommandContext context) {
    final Database database = context.getDatabase();
    final MetaConfig config = config(args);
    final MetaLabels labels = new MetaLabels(database);

    final Map<List<String>, MetaPropertyProfile> groups = new LinkedHashMap<>();
    forEachSampledNode(database, labels, config,
        (type, record) -> groups.computeIfAbsent(labels.labelsOf(type), k -> new MetaPropertyProfile()).observe(record.asDocument()));

    for (final VertexType type : labels.vertexTypes()) {
      final List<String> typeLabels = labels.labelsOf(type);
      if (!typeLabels.isEmpty() && !type.getPolymorphicPropertyNames().isEmpty() && typeLabels.stream().anyMatch(config::acceptsLabel))
        groups.computeIfAbsent(typeLabels, k -> new MetaPropertyProfile()).declare(type);
    }

    final List<Result> results = new ArrayList<>();
    for (final Map.Entry<List<String>, MetaPropertyProfile> group : groups.entrySet()) {
      final Map<String, Object> columns = new LinkedHashMap<>();
      columns.put("nodeType", MetaLabels.pattern(group.getKey()));
      columns.put("nodeLabels", group.getKey());
      results.addAll(group.getValue().rows(columns));
    }
    return results.stream();
  }
}
