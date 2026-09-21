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
package com.arcadedb.query.opencypher.procedures.refactor;

import com.arcadedb.database.Database;
import com.arcadedb.exception.CommandSemanticException;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.MutableEdge;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.opencypher.executor.CypherVertexReload;
import com.arcadedb.query.opencypher.procedures.CypherProcedure;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Stream;

/**
 * Procedure: refactor.mergeNodes(nodes, config = {})
 * <p>
 * Merges a list of nodes into the first one (the survivor). Every incoming and outgoing edge of the
 * other nodes (the absorbed nodes) is rewired onto the survivor - an edge that connected two nodes
 * both being merged becomes a self-relationship on the survivor - and the absorbed nodes are then
 * deleted.
 * </p>
 * <p>
 * {@code config.properties} controls how a property present on both the survivor and an absorbed node
 * is resolved: {@code "overwrite"} (the absorbed node's value wins, the default), {@code "discard"}
 * (the survivor's original value is kept) or {@code "combine"} (the distinct values are kept, in
 * first-seen order, as a list - or as the value itself when the merged nodes all agree on it). A
 * property present only on an absorbed node is always copied onto the survivor. The whole {@code config} argument
 * is optional and defaults to an empty map - hence to the {@code "overwrite"} policy - matching APOC's
 * {@code apoc.refactor.mergeNodes(nodes :: LIST<NODE>, config = {} :: MAP)} (issue #7427).
 * </p>
 * <p>
 * Example:
 * <pre>
 * MATCH (a:Person {name:'A'}), (b:Person {name:'B'})
 * CALL apoc.refactor.mergeNodes([a, b], {properties: 'combine'})
 * YIELD node
 * RETURN node
 * </pre>
 * </p>
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public class RefactorMergeNodes implements CypherProcedure {
  public static final String NAME = "refactor.mergeNodes";

  private static final Set<String> VALID_POLICIES = Set.of("overwrite", "discard", "combine");

  @Override
  public String getName() {
    return NAME;
  }

  /**
   * One, not two: APOC declares the trailing {@code config} with a default, so a call that omits it is a call this
   * procedure has to accept (issue #7427). {@link RefactorProcedureArgs#extractOptionalConfig} supplies the empty
   * map in its place.
   */
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
    return "Merges a list of nodes into the first one, rewiring their edges and deleting the absorbed nodes.";
  }

  @Override
  public List<String> getYieldFields() {
    return List.of("node");
  }

  @Override
  public boolean isWriteProcedure() {
    return true;
  }

  @Override
  public Stream<Result> execute(final Object[] args, final Result inputRow, final CommandContext context) {
    validateArgs(args);

    final List<Vertex> nodes = RefactorProcedureArgs.extractVertices(getName(), args[0]);
    if (nodes.size() < 2)
      throw new CommandSemanticException(getName() + "(): at least two distinct nodes are required to merge");

    final Map<String, Object> config = RefactorProcedureArgs.extractOptionalConfig(getName(), args);
    final String globalPolicy = extractPropertiesPolicy(config);

    final Database database = context.getDatabase();

    // Every node here arrives as the instance its row carried, loaded before the rows ahead of it applied their
    // merges. The survivor is the one the rows share: mergeProperties READS its properties and save() writes the
    // record back, so a row working from a stale snapshot would drop what earlier rows accumulated - and a read
    // has no safety net, unlike an edge APPEND, which substitutes the transaction's own written copy by RID.
    // Today's rows happen to carry a fresh survivor, so no reproducer exists for this one (see the test); the
    // re-read is what stops the procedure depending on that. Same re-read merge.relationship does for its
    // endpoints (issues #7174 and #7177).
    final Vertex survivor = CypherVertexReload.latest(database, nodes.get(0));
    final MutableVertex survivorMutable = survivor.modify();

    for (int i = 1; i < nodes.size(); i++) {
      // Same for the absorbed node, whose edge list rewireEdges below enumerates.
      final Vertex absorbed = CypherVertexReload.latest(database, nodes.get(i));

      mergeProperties(survivorMutable, absorbed, globalPolicy);
      survivorMutable.save();

      rewireEdges(absorbed, survivorMutable);

      absorbed.modify().delete();
    }

    return createResultStream(survivorMutable);
  }

  private void mergeProperties(final MutableVertex survivor, final Vertex absorbed, final String policy) {
    for (final String propertyName : absorbed.getPropertyNames()) {
      final Object absorbedValue = absorbed.get(propertyName);

      if (!survivor.getPropertyNames().contains(propertyName)) {
        survivor.set(propertyName, absorbedValue);
        continue;
      }

      switch (policy) {
        case "overwrite" -> survivor.set(propertyName, absorbedValue);
        case "discard" -> {
          // keep the survivor's original value
        }
        case "combine" -> {
          final List<Object> combined = new ArrayList<>();
          addDistinct(combined, survivor.get(propertyName));
          addDistinct(combined, absorbedValue);
          // APOC's contract for 'combine' is "if the values are the same, keep one; otherwise merge into a
          // list", so a list appears only once a second distinct value has actually turned up. Merging nodes
          // that agree - the common case - therefore leaves every scalar the scalar it was, instead of the
          // two-element list of duplicates this branch used to produce (issue #7428).
          survivor.set(propertyName, combined.size() == 1 ? combined.getFirst() : combined);
        }
        // unreachable in practice - extractPropertiesPolicy validates policy against VALID_POLICIES
        // before mergeProperties is ever called; kept as a defensive fallback against the two drifting
        // apart under a future edit, e.g. a new call site that skips extractPropertiesPolicy
        default -> throw new CommandSemanticException(getName() + "(): unknown properties policy '" + policy + "'");
      }
    }
  }

  /**
   * Appends {@code value} to {@code combined}, skipping anything already there so that equal contributions
   * collapse to one entry and the first-seen order is the order that survives.
   * <p>
   * A list is flattened rather than nested, because by the second iteration of the merge loop the survivor's
   * value is whatever this method last accumulated, and because an absorbed node may legitimately carry a list
   * of its own. A Java array is deliberately <b>not</b> flattened the same way: it is kept as the single opaque
   * value it is - matching how ArcadeDB already treats one everywhere else (e.g. {@code UNWIND} on a sequence
   * type) - because concatenating two array-valued properties element-by-element, or deciding what type the
   * result should be when two differently-typed arrays meet, is not a merge a caller could make sense of. A
   * vector embedding carried by both merged nodes is the practical case: it must survive as the single array it
   * was, not dissolve into a list no vector index can read (issue #8099).
   * <p>
   * The membership test is a linear scan on purpose: the list holds one entry per <i>distinct</i> value across
   * the merged nodes, which is small, and a scan costs no hash set allocation per property.
   */
  private static void addDistinct(final List<Object> combined, final Object value) {
    if (value instanceof List<?> list) {
      for (final Object element : list)
        addDistinctScalar(combined, element);
    } else
      addDistinctScalar(combined, value);
  }

  /**
   * The membership test {@link #addDistinct} applies to one non-list value: {@link Object#equals} for
   * everything except a Java array, whose {@code equals} is identity rather than content, so two equal-looking
   * {@code float[]}/{@code short[]}/... instances contributed by different nodes would otherwise never collapse
   * to one entry (issue #8099).
   */
  private static void addDistinctScalar(final List<Object> combined, final Object value) {
    if (value != null && value.getClass().isArray()) {
      for (final Object existing : combined)
        if (existing != null && existing.getClass().isArray() && Objects.deepEquals(existing, value))
          return;
      combined.add(value);
    } else if (!combined.contains(value))
      combined.add(value);
  }

  private void rewireEdges(final Vertex absorbed, final MutableVertex survivor) {
    final List<Edge> edgesToRewire = new ArrayList<>();
    for (final Edge edge : absorbed.getEdges())
      edgesToRewire.add(edge);

    for (final Edge edge : edgesToRewire) {
      final MutableEdge mutableEdge = edge.modify();
      if (mutableEdge.getOut().equals(absorbed.getIdentity()))
        mutableEdge.set("@out", survivor.getIdentity());
      if (mutableEdge.getIn().equals(absorbed.getIdentity()))
        mutableEdge.set("@in", survivor.getIdentity());
    }
  }

  private Stream<Result> createResultStream(final Vertex node) {
    final ResultInternal result = new ResultInternal();
    result.setProperty("node", node);
    return Stream.of(result);
  }

  private String extractPropertiesPolicy(final Map<String, Object> config) {
    final Object raw = config.get("properties");
    if (raw == null)
      return "overwrite";
    final String policy = raw.toString();
    if (!VALID_POLICIES.contains(policy))
      throw new CommandSemanticException(getName() + "(): unknown properties policy '" + policy + "'");
    return policy;
  }
}
