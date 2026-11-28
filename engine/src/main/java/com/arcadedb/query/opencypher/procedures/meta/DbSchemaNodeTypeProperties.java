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
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * Neo4j's {@code db.schema.nodeTypeProperties()} (issue #13543): the properties found on the nodes of each label set, with the
 * columns {@code nodeType}, {@code nodeLabels}, {@code propertyName}, {@code propertyTypes} and {@code mandatory}.
 * <p>
 * It is what {@link MetaNodeTypeProperties} reports, read the way Neo4j reads: every node and not a sample, only what the nodes
 * carry (a property that is declared but no node holds is not listed), and {@code mandatory} true for a property every node of the
 * label set carries.
 * </p>
 */
public class DbSchemaNodeTypeProperties extends MetaNodeTypeProperties {
  public static final String NAME = "db.schema.nodeTypeProperties";

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
    return "Show the derived property schema of the nodes in tabular form (Neo4j-compatible)";
  }

  @Override
  public List<String> getYieldFields() {
    return List.of("nodeType", "nodeLabels", "propertyName", "propertyTypes", "mandatory");
  }

  @Override
  public Stream<Result> execute(final Object[] args, final Result inputRow, final CommandContext context) {
    final List<Result> rows = new ArrayList<>();
    super.execute(new Object[] { Map.of("sample", -1L) }, inputRow, context).forEach(row -> {
      final long total = row.<Number>getProperty("totalObservations").longValue();
      final long observed = row.<Number>getProperty("propertyObservations").longValue();
      // a label set no node carries, or a declared property no node holds: nothing the data says
      if (total == 0L || row.getProperty("propertyName") != null && observed == 0L)
        return;
      final ResultInternal result = new ResultInternal();
      result.setProperty("nodeType", row.getProperty("nodeType"));
      result.setProperty("nodeLabels", row.getProperty("nodeLabels"));
      result.setProperty("propertyName", row.getProperty("propertyName"));
      result.setProperty("propertyTypes", row.getProperty("propertyTypes"));
      result.setProperty("mandatory", row.getProperty("propertyName") != null && observed == total);
      rows.add(result);
    });
    return rows.stream();
  }
}
