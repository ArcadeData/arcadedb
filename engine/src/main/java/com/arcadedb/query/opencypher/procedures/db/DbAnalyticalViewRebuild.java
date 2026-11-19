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

import com.arcadedb.graph.olap.GraphAnalyticalView;
import com.arcadedb.graph.olap.GraphAnalyticalViewCommands;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;

import java.util.List;
import java.util.stream.Stream;

/**
 * {@code CALL db.analyticalView.rebuild(name) YIELD name, status}: the Cypher form of {@code REBUILD GRAPH ANALYTICAL VIEW}
 * (issue #11438), sharing its body ({@link GraphAnalyticalViewCommands#rebuild}). As the SQL statement, it rebuilds
 * synchronously, and a view whose last build failed is built again from its definition.
 */
public class DbAnalyticalViewRebuild extends AbstractDbAnalyticalViewProcedure {
  public static final String NAME = "db.analyticalView.rebuild";

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public int getMinArgs() {
    return 1;
  }

  @Override
  public int getMaxArgs() {
    return 1;
  }

  @Override
  public String getDescription() {
    return "Rebuilds a Graph Analytical View synchronously, as REBUILD GRAPH ANALYTICAL VIEW does";
  }

  @Override
  public List<String> getYieldFields() {
    return List.of("name", "status");
  }

  @Override
  public Stream<Result> execute(final Object[] args, final Result inputRow, final CommandContext context) {
    validateArgs(args);
    final String viewName = extractName(args[0]);
    final GraphAnalyticalView view = GraphAnalyticalViewCommands.rebuild(context.getDatabase(), viewName);

    final ResultInternal result = new ResultInternal();
    result.setProperty("name", viewName);
    result.setProperty("status", view.getStatus().name());
    return Stream.of(result);
  }
}
