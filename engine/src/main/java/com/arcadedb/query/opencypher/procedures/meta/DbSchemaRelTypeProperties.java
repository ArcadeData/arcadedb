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

import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Stream;

/**
 * Neo4j's {@code db.schema.relTypeProperties()} (issue #13543): the properties found on the relationships of each type, with the
 * columns {@code relType}, {@code propertyName}, {@code propertyTypes} and {@code mandatory}.
 * <p>
 * It is what {@link MetaRelTypeProperties} reports, read the way Neo4j reads: every relationship and not a sample, whatever nodes
 * it joins (the rows of one type are one), only what the relationships carry, and {@code mandatory} true for a property every
 * relationship of the type carries.
 * </p>
 */
public class DbSchemaRelTypeProperties extends MetaRelTypeProperties {
  public static final String NAME = "db.schema.relTypeProperties";

  /** What the relationships of one type carry, summed over the label sets they join. */
  private static final class TypeProfile {
    private long                          total;
    private final Map<String, Long>       observations = new LinkedHashMap<>();
    private final Map<String, Set<String>> types       = new LinkedHashMap<>();
  }

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public int getMaxArgs() {
    return 0;
  }

  @Override
  public String getDescription() {
    return "Show the derived property schema of the relationships in tabular form (Neo4j-compatible)";
  }

  @Override
  public List<String> getYieldFields() {
    return List.of("relType", "propertyName", "propertyTypes", "mandatory");
  }

  @Override
  @SuppressWarnings("unchecked")
  public Stream<Result> execute(final Object[] args, final Result inputRow, final CommandContext context) {
    final Map<String, TypeProfile> byType = new LinkedHashMap<>();
    // each group (one type between two label sets) states its total on every one of its rows: it is counted once
    final Set<List<Object>> counted = new HashSet<>();
    super.execute(new Object[] { Map.of("sample", -1L, "maxRels", Long.MAX_VALUE) }, inputRow, context).forEach(row -> {
      final long total = row.<Number>getProperty("totalObservations").longValue();
      if (total == 0L)
        return;
      final String relType = row.getProperty("relType");
      final TypeProfile profile = byType.computeIfAbsent(relType, k -> new TypeProfile());
      if (counted.add(List.of(relType, row.getProperty("sourceNodeLabels"), row.getProperty("targetNodeLabels"))))
        profile.total += total;
      final String propertyName = row.getProperty("propertyName");
      final long observed = row.<Number>getProperty("propertyObservations").longValue();
      if (propertyName != null && observed > 0L) {
        profile.observations.merge(propertyName, observed, Long::sum);
        profile.types.computeIfAbsent(propertyName, k -> new TreeSet<>()).addAll((List<String>) row.getProperty("propertyTypes"));
      }
    });

    final List<Result> rows = new ArrayList<>();
    for (final Map.Entry<String, TypeProfile> entry : byType.entrySet()) {
      final TypeProfile profile = entry.getValue();
      if (profile.observations.isEmpty())
        rows.add(row(entry.getKey(), null, null, false));
      for (final Map.Entry<String, Long> property : profile.observations.entrySet())
        rows.add(row(entry.getKey(), property.getKey(), new ArrayList<>(profile.types.get(property.getKey())),
            property.getValue() == profile.total));
    }
    return rows.stream();
  }

  private static Result row(final String relType, final String propertyName, final List<String> propertyTypes, final boolean mandatory) {
    final ResultInternal row = new ResultInternal();
    row.setProperty("relType", relType);
    row.setProperty("propertyName", propertyName);
    row.setProperty("propertyTypes", propertyTypes);
    row.setProperty("mandatory", mandatory);
    return row;
  }
}
