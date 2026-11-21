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
package com.arcadedb.query.sql.executor;

import com.arcadedb.database.Document;
import com.arcadedb.database.Identifiable;
import com.arcadedb.graph.IncomingEdgeLookup;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.parser.MatchPathItem;
import com.arcadedb.query.sql.parser.Rid;
import com.arcadedb.query.sql.parser.WhereClause;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * Created by luigidellaquila on 15/10/16.
 */
public class MatchReverseEdgeTraverser extends MatchEdgeTraverser {

  private final String startingPointAlias;
  private final String endPointAlias;

  public MatchReverseEdgeTraverser(final Result lastUpstreamRecord, final EdgeTraversal edge) {
    super(lastUpstreamRecord, edge);
    this.startingPointAlias = edge.edge.in.alias;
    this.endPointAlias = edge.edge.out.alias;
  }

  protected String targetClassName(final MatchPathItem item, final CommandContext iCommandContext) {
    return edge.getLeftClass();
  }

  /**
   * This walk started at the hop's target and ended at its source, so the vertices it crossed come in the opposite direction of the pattern.
   * The path a forward walk reports is the one from the source to the target, excluding the source and ending at the target: the crossed
   * vertices reversed, without the source this walk ended at, followed by the target this walk started from (issue #12945).
   */
  @Override
  protected Object pathAliasValue(final ResultInternal reached) {
    final Object walked = reached.getMetadata("$matchPath");
    if (!(walked instanceof List<?> crossed) || crossed.isEmpty())
      return walked;
    Identifiable target = sourceRecord.getElementProperty(startingPointAlias);
    if (target instanceof Result result)
      target = result.getElement().orElse(null);
    final List<Identifiable> path = new ArrayList<>(crossed.size());
    for (int i = crossed.size() - 2; i >= 0; i--)
      path.add((Identifiable) crossed.get(i));
    path.add(target);
    return path;
  }

  /**
   * {@code item.getMethod()} is still the pattern edge's method as parsed from its syntactic out-side (this class
   * never rewrites it), so the direction it names is relative to that side, not to {@link #getStartingPointAlias()}
   * (the in-side, which this reverse traverser actually starts from) - the same out<->in flip
   * {@link com.arcadedb.query.sql.parser.MethodCall#executeReverse} applies to pick the method it actually runs.
   * "both" has no opposite and stays as-is.
   */
  @Override
  protected Vertex.DIRECTION getExpandIntoDirection() {
    return MatchExecutionPlanner.directionFor(item, false);
  }

  protected String targetClusterName(final MatchPathItem item, final CommandContext iCommandContext) {
    return edge.getLeftCluster();
  }

  protected Rid targetRid(final MatchPathItem item, final CommandContext iCommandContext) {
    return edge.getLeftRid();
  }

  protected WhereClause getTargetFilter(final MatchPathItem item) {
    return edge.getLeftFilter();
  }

  @Override
  protected Iterable<ResultInternal> traversePatternEdge(final Identifiable startingPoint, final CommandContext iCommandContext) {

    final Object qR = IncomingEdgeLookup.walkingPattern(
        () -> this.item.getMethod().executeReverse(startingPoint, iCommandContext));
    if (qR == null) {
      return Collections.emptyList();
    }
    if (qR instanceof ResultInternal internal) {
      return Set.of(internal);
    }
    if (qR instanceof Document document) {
      return Set.of(new ResultInternal(document));
    }
    if (qR instanceof Iterable iterable) {
      final List<ResultInternal> result = new ArrayList<>();
      for (final Object o : iterable) {
        if (o instanceof Document document) {
          result.add(new ResultInternal(document));
        } else if (o instanceof ResultInternal internal) {
          result.add(internal);
        } else if (o == null) {
        } else {
          throw new UnsupportedOperationException();
        }
      }
      return result;
    }
    return Collections.emptyList();
  }

  @Override
  protected String getStartingPointAlias() {
    return this.startingPointAlias;
  }

  @Override
  protected String getEndpointAlias() {
    return endPointAlias;
  }

}
