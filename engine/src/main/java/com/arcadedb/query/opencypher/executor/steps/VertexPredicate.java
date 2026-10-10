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
package com.arcadedb.query.opencypher.executor.steps;

import com.arcadedb.database.Database;
import com.arcadedb.database.Document;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.graph.GraphTraversalProvider;
import com.arcadedb.graph.NeighborView;
import com.arcadedb.query.opencypher.InlineProperties;
import com.arcadedb.query.opencypher.ast.BooleanExpression;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.utility.RidLongHashMap;

import java.util.Map;

/**
 * A filter on the vertices one position of a counted pattern binds, on top of the label the position carries: the
 * inline property map written on the node ({@code (:Message {kind: 'Post'})}) and the {@code WHERE} conjuncts that read
 * that node and nothing else ({@code WHERE m.kind = 'Post'}). Either one is a property of the vertex alone, so a count
 * push-down can keep walking adjacency arrays and ask the filter once per distinct vertex a position reaches, instead
 * of leaving the whole count to the row pipeline (issue #9595).
 * <p>
 * The inline values are resolved once, when the push-down is built for an execution, so a parameter is read with the
 * value of that execution. The filter itself is immutable; the memo of the answers is held by the {@link Evaluation}
 * each operator run creates, so nothing an execution learns leaks into another one.
 * <p>
 * The answers follow the row pipeline's: an inline entry compares the way {@link InlineProperties} does for a matched
 * node, and the {@code WHERE} conjuncts are evaluated against a row binding the variable to the vertex, the way the
 * filter step evaluates them.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class VertexPredicate {
  private static final byte UNKNOWN = 0;
  private static final byte PASSES  = 1;
  private static final byte FAILS   = 2;

  private final String            variable;
  private final String[]          keys;
  private final Object[]          values;
  private final BooleanExpression where;
  private final CommandContext    context;

  /**
   * @param variable   the name the {@code WHERE} conjuncts read the vertex by, null for an anonymous node (which no
   *                   conjunct can name)
   * @param properties the inline property map with every value already resolved, null or empty for none
   * @param where      the conjunction of the {@code WHERE} conjuncts reading this vertex alone, null for none
   * @param context    the context of the execution the push-down runs in, which evaluates the conjuncts
   */
  public VertexPredicate(final String variable, final Map<String, Object> properties, final BooleanExpression where,
      final CommandContext context) {
    if (where != null && variable == null)
      throw new IllegalArgumentException("A WHERE predicate needs the variable it reads the vertex by");
    this.variable = variable;
    final int size = properties == null ? 0 : properties.size();
    this.keys = new String[size];
    this.values = new Object[size];
    if (size > 0) {
      int i = 0;
      for (final Map.Entry<String, Object> entry : properties.entrySet()) {
        keys[i] = entry.getKey();
        values[i++] = entry.getValue();
      }
    }
    this.where = where;
    this.context = context;
  }

  /**
   * A fresh evaluation, with its own memo, for one run of an operator. It must not outlive that run: the answers it
   * keeps are those of one execution's parameters and one snapshot of the graph.
   */
  public Evaluation evaluation(final Database database, final GraphTraversalProvider provider) {
    return new Evaluation(database, provider);
  }

  /** One evaluation per position: null where the position has no predicate, the array itself null when none has. */
  public static Evaluation[] evaluations(final VertexPredicate[] predicates, final Database database,
      final GraphTraversalProvider provider) {
    if (predicates == null)
      return null;
    final Evaluation[] evaluations = new Evaluation[predicates.length];
    for (int i = 0; i < predicates.length; i++)
      if (predicates[i] != null)
        evaluations[i] = predicates[i].evaluation(database, provider);
    return evaluations;
  }

  /** Whether any position has a predicate. */
  public static boolean any(final VertexPredicate[] predicates) {
    if (predicates != null)
      for (final VertexPredicate predicate : predicates)
        if (predicate != null)
          return true;
    return false;
  }

  /** The predicate as EXPLAIN shows it. */
  public String describe() {
    final StringBuilder sb = new StringBuilder();
    if (keys.length > 0) {
      sb.append('{');
      for (int i = 0; i < keys.length; i++) {
        if (i > 0)
          sb.append(", ");
        sb.append(keys[i]).append(": ").append(values[i]);
      }
      sb.append('}');
    }
    if (where != null) {
      if (!sb.isEmpty())
        sb.append(' ');
      sb.append("WHERE ").append(where.getText());
    }
    return sb.toString();
  }

  /**
   * The answers of one operator run, each vertex asked once: by dense node id over a {@link GraphTraversalProvider}, by
   * RID otherwise. Every array a count operator propagates over a provider is indexed by node id, so the memo is a byte
   * per node, allocated the first time a node id is asked about; a run that only asks by RID - the edge-list walk, or a
   * walk seeded from one bound vertex - never pays for it.
   * <p>
   * Not thread-safe, and not meant to be: one operator run asks from one thread, which is also what lets the
   * {@code WHERE} row be reused from vertex to vertex. Never cache one across executions.
   */
  public final class Evaluation {
    private final Database               database;
    private final GraphTraversalProvider provider;
    private       byte[]                 byNodeId;
    private       RidLongHashMap         byRid;
    private       ResultInternal         row;

    private Evaluation(final Database database, final GraphTraversalProvider provider) {
      this.database = database;
      this.provider = provider;
    }

    /** Whether the provider's node passes, the vertex read once per evaluation. */
    public boolean acceptsNode(final int nodeId) {
      if (byNodeId == null)
        byNodeId = new byte[provider.getNodeIdUpperBound()];
      final byte known = byNodeId[nodeId];
      if (known != UNKNOWN)
        return known == PASSES;
      final RID rid = provider.getRID(nodeId);
      final boolean passes = rid != null && evaluate(rid);
      byNodeId[nodeId] = passes ? PASSES : FAILS;
      return passes;
    }

    /** Whether the vertex of this RID passes, read once per evaluation. */
    public boolean acceptsRid(final RID rid) {
      if (rid == null)
        return false;
      if (byRid == null)
        byRid = new RidLongHashMap();
      final long known = byRid.get(rid, UNKNOWN);
      if (known != UNKNOWN)
        return known == PASSES;
      final boolean passes = evaluate(rid);
      byRid.put(rid, passes ? PASSES : FAILS);
      return passes;
    }

    /** Whether a vertex the caller already holds passes: it is not read again, nor remembered. */
    public boolean accepts(final Identifiable vertex) {
      return vertex instanceof Document document ? test(document) : acceptsRid(vertex.getIdentity());
    }

    /**
     * The nodes of a frontier that pass, in their order and with their repetitions (one per path that reached them): the
     * array itself when every node passes, a shorter copy otherwise.
     */
    public int[] retain(final int[] nodeIds) {
      int kept = 0;
      for (final int nodeId : nodeIds)
        if (acceptsNode(nodeId))
          ++kept;
      if (kept == nodeIds.length)
        return nodeIds;
      final int[] result = new int[kept];
      int pos = 0;
      for (final int nodeId : nodeIds)
        if (acceptsNode(nodeId))
          result[pos++] = nodeId;
      return result;
    }

    /** Zeroes the path counts of the nodes that fail, asking only about the nodes some path reached. */
    public void filter(final long[] counts) {
      filter(counts, null);
    }

    /**
     * Zeroes the path counts of the nodes that fail, asking only about the nodes some path reached and, when
     * {@code continuation} is given, that have an edge on it: a node the next hop leaves by no edge carries its paths
     * nowhere, so it is zeroed without reading the vertex.
     */
    public void filter(final long[] counts, final NeighborView continuation) {
      for (int v = 0; v < counts.length; v++)
        if (counts[v] != 0 && ((continuation != null && continuation.degree(v) == 0) || !acceptsNode(v)))
          counts[v] = 0;
    }

    /** Whether the vertex passes every inline entry and the {@code WHERE} conjuncts. */
    private boolean test(final Document vertex) {
      for (int i = 0; i < keys.length; i++)
        if (!InlineProperties.matchesResolvedValue(vertex.get(keys[i]), values[i]))
          return false;
      if (where == null)
        return true;
      // One row for every vertex: a per-vertex conjunct only answers a boolean and holds nothing that could keep the
      // row (CountPushDownPredicates refuses pattern comprehensions, subqueries and pattern predicates)
      if (row == null)
        row = new ResultInternal(context.getDatabase());
      row.setProperty(variable, vertex);
      return where.evaluate(row, context);
    }

    private boolean evaluate(final RID rid) {
      final Record record;
      try {
        record = database.lookupByRID(rid, true);
      } catch (final RecordNotFoundException e) {
        // a vertex deleted since the adjacency was read binds no row in the pipeline either
        return false;
      }
      return record instanceof Document document && test(document);
    }
  }
}
