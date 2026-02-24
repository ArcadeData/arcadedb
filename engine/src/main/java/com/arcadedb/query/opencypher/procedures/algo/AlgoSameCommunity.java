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
package com.arcadedb.query.opencypher.procedures.algo;

import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;

import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

/**
 * Procedure: algo.sameCommunity(node1, node2, communityProperty)
 * <p>
 * Link-prediction metric that returns 1.0 when both nodes share the same value for the given
 * community-label property, and 0.0 otherwise. This is typically used together with a
 * community-detection procedure: first annotate nodes with a community property (e.g. via
 * {@code algo.louvain}), then use this procedure to score candidate links.
 * </p>
 * <p>
 * Example:
 * <pre>
 * // First run community detection and write results back
 * MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'})
 * CALL algo.sameCommunity(a, b, 'community')
 * YIELD node1, node2, coefficient
 * RETURN coefficient
 * </pre>
 * </p>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class AlgoSameCommunity extends AbstractAlgoProcedure {
  public static final String NAME = "algo.sameCommunity";

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public int getMinArgs() {
    return 3;
  }

  @Override
  public int getMaxArgs() {
    return 3;
  }

  @Override
  public String getDescription() {
    return "Link prediction: 1.0 if both nodes share the same community label property value, else 0.0";
  }

  @Override
  public List<String> getYieldFields() {
    return List.of("node1", "node2", "coefficient");
  }

  @Override
  public Stream<Result> execute(final Object[] args, final Result inputRow, final CommandContext context) {
    validateArgs(args);

    final Vertex node1             = extractVertex(args[0], "node1");
    final Vertex node2             = extractVertex(args[1], "node2");
    final String communityProperty = extractString(args[2], "communityProperty");

    if (communityProperty == null || communityProperty.isBlank())
      throw new IllegalArgumentException(getName() + "(): communityProperty cannot be null or empty");

    final Object c1 = node1.get(communityProperty);
    final Object c2 = node2.get(communityProperty);
    final double coefficient = (c1 != null && Objects.equals(c1, c2)) ? 1.0 : 0.0;

    final ResultInternal r = new ResultInternal();
    r.setProperty("node1", node1);
    r.setProperty("node2", node2);
    r.setProperty("coefficient", coefficient);
    return Stream.of(r);
  }
}
