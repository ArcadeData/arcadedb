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
package com.arcadedb.query.opencypher.procedures.db;

import com.arcadedb.query.opencypher.procedures.CypherProcedure;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.FetchFromSchemaGraphAnalyticalViewsStep;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

/**
 * {@code CALL db.analyticalView.list()}: one row per Graph Analytical View, with the same figures as
 * {@code SELECT FROM schema:graphAnalyticalViews}, which builds them ({@link FetchFromSchemaGraphAnalyticalViewsStep#describeAll})
 * (issue #11438). As that query, it lists every view whatever its types: a view's definition is schema, which any user of the
 * database may read.
 * <p>
 * Every declared field is on every row, null where it does not apply (no build timestamp before the first build, no
 * {@code buildError} for a view whose last build succeeded), so a {@code YIELD} of any of them resolves.
 */
public class DbAnalyticalViewList implements CypherProcedure {
  public static final String NAME = "db.analyticalView.list";

  private static final List<String> FIELDS = List.of("name", "vertexTypes", "edgeTypes", "propertyFilter", "edgePropertyFilter",
      "updateMode", "compactionThreshold", "status", "nodeCount", "edgeCount", "memoryUsageBytes", "buildTimestamp",
      "buildDurationMs", "contractionHierarchies", "buildError");

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
    return 0;
  }

  @Override
  public String getDescription() {
    return "Lists the Graph Analytical Views with their definition, status, node and edge counts and memory use, as SELECT FROM "
        + "schema:graphAnalyticalViews does";
  }

  @Override
  public List<String> getYieldFields() {
    return FIELDS;
  }

  @Override
  public Stream<Result> execute(final Object[] args, final Result inputRow, final CommandContext context) {
    validateArgs(args);
    final List<Result> rows = new ArrayList<>();
    FetchFromSchemaGraphAnalyticalViewsStep.describeAll(context.getDatabase(), row -> {
      for (final String field : FIELDS)
        if (!row.hasProperty(field))
          row.setProperty(field, null);
      rows.add(row);
    });
    return rows.stream();
  }
}
