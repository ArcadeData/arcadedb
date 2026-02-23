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
package com.arcadedb.function.sql.time;

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Identifiable;
import com.arcadedb.engine.timeseries.promql.PromQLEvaluator;
import com.arcadedb.engine.timeseries.promql.PromQLParser;
import com.arcadedb.engine.timeseries.promql.PromQLResult;
import com.arcadedb.function.sql.SQLFunctionConfigurableAbstract;
import com.arcadedb.query.sql.executor.CommandContext;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * SQL function: {@code promql(expr [, evalTimeMs])}
 * <p>
 * Evaluates a PromQL instant-vector expression over the current database's TimeSeries data
 * and returns the result as a list of maps. Each map contains the metric labels
 * plus a special {@code "__value__"} key holding the numeric sample value.
 * <p>
 * Examples:
 * <pre>
 *   SELECT promql('cpu_usage') FROM #1:0
 *   SELECT promql('sum(cpu_usage) by (host)', 1700000000000) FROM #1:0
 *   SELECT promql('rate(http_requests[5m])', System.currentTimeMillis()) FROM #1:0
 * </pre>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class SQLFunctionPromQL extends SQLFunctionConfigurableAbstract {

  public static final String NAME = "promql";

  public SQLFunctionPromQL() {
    super(NAME);
  }

  @Override
  public Object execute(final Object self, final Identifiable currentRecord, final Object currentResult,
      final Object[] params, final CommandContext context) {
    if (params.length < 1 || params[0] == null)
      throw new IllegalArgumentException("promql() requires at least 1 parameter: the PromQL expression string");

    final String expr = params[0].toString();
    final long evalTimeMs;
    if (params.length >= 2 && params[1] != null)
      evalTimeMs = params[1] instanceof Number n ? n.longValue() : Long.parseLong(params[1].toString());
    else
      evalTimeMs = System.currentTimeMillis();

    final DatabaseInternal database = (DatabaseInternal) context.getDatabase();
    final PromQLEvaluator evaluator = new PromQLEvaluator(database);
    final PromQLResult result = evaluator.evaluateInstant(new PromQLParser(expr).parse(), evalTimeMs);
    return toList(result);
  }

  private static List<Map<String, Object>> toList(final PromQLResult result) {
    final List<Map<String, Object>> list = new ArrayList<>();
    if (result instanceof PromQLResult.InstantVector iv) {
      for (final PromQLResult.VectorSample sample : iv.samples()) {
        final Map<String, Object> entry = new LinkedHashMap<>(sample.labels());
        entry.put("__value__", sample.value());
        list.add(entry);
      }
    } else if (result instanceof PromQLResult.ScalarResult sr) {
      final Map<String, Object> entry = new LinkedHashMap<>();
      entry.put("__value__", sr.value());
      list.add(entry);
    }
    return list;
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
  public String getSyntax() {
    return "promql(<expr> [,<evalTimeMs>])";
  }
}
