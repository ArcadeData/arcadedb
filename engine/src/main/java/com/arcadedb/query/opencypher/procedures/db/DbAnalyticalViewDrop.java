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

import com.arcadedb.graph.olap.GraphAnalyticalViewCommands;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

/**
 * {@code CALL db.analyticalView.drop(name [, {ifExists: true}]) YIELD name, dropped}: the Cypher form of
 * {@code DROP GRAPH ANALYTICAL VIEW [IF EXISTS]} (issue #11438), sharing its body ({@link GraphAnalyticalViewCommands#drop}).
 */
public class DbAnalyticalViewDrop extends AbstractDbAnalyticalViewProcedure {
  public static final String NAME = "db.analyticalView.drop";

  private static final Set<String> CONFIG_KEYS = Set.of("ifExists");

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
    return 2;
  }

  @Override
  public String getDescription() {
    return "Drops a Graph Analytical View and its definition, as DROP GRAPH ANALYTICAL VIEW does; config key: ifExists";
  }

  @Override
  public List<String> getYieldFields() {
    return List.of("name", "dropped");
  }

  @Override
  public Stream<Result> execute(final Object[] args, final Result inputRow, final CommandContext context) {
    validateArgs(args);
    final String viewName = extractName(args[0]);
    final Map<String, Object> config = extractConfig(args.length > 1 ? args[1] : null, CONFIG_KEYS);
    final boolean dropped = GraphAnalyticalViewCommands.drop(context.getDatabase(), viewName, extractBoolean(config, "ifExists"));

    final ResultInternal result = new ResultInternal();
    result.setProperty("name", viewName);
    result.setProperty("dropped", dropped);
    return Stream.of(result);
  }
}
