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

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;
import com.arcadedb.graph.GraphTraversalProvider;
import com.arcadedb.graph.GraphTraversalProviderRegistry;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.parser.MatchExpression;
import com.arcadedb.query.sql.parser.MatchPathItem;
import com.arcadedb.query.sql.parser.MatchStatement;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Focused unit tests for the GAV expand-into fast path in {@link MatchEdgeTraverser#init}, reproducing #6670:
 * the fast path called {@code isConnectedTo()} with no edge type at all (matching on ANY type) and derived
 * direction from the schedule's forward/reverse split instead of the pattern's actual out/in/both method.
 * <p>
 * Driving this from a full SQL MATCH query is unreliable here: the fast path only fires for an edge whose both
 * endpoints are already bound by earlier edges in the same pattern (a cycle-closing edge), and getting the
 * planner to schedule one - without it instead getting folded into a {@code MatchGAVFusedStep} chain, which
 * bypasses {@link MatchEdgeTraverser} entirely - depends on scheduling heuristics that are not part of this
 * bug. This test instead builds the minimal {@link EdgeTraversal}/bound-source-record scaffolding directly and
 * drives {@link MatchEdgeTraverser#hasNext} itself, with a {@link GraphTraversalProvider} test double that
 * records exactly which direction and edge types it was asked about.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class MatchEdgeTraverserExpandIntoTest extends TestHelper {

  /**
   * Records the direction/edge types of the last {@link #isConnectedTo} call and answers a fixed "connected"
   * verdict, so a test can assert both what the traverser asked and what it did with the answer.
   */
  private static final class RecordingProvider implements GraphTraversalProvider {
    private final Map<RID, Integer> ridToId = new HashMap<>();
    private final Map<Integer, RID> idToRid = new HashMap<>();
    private       int               nextId  = 0;
    private       boolean           connected;

    Vertex.DIRECTION lastDirection;
    String[]         lastEdgeTypes;
    int              isConnectedToCalls;

    private int idFor(final RID rid) {
      return ridToId.computeIfAbsent(rid, r -> {
        final int id = nextId++;
        idToRid.put(id, r);
        return id;
      });
    }

    @Override
    public int getNodeCount() {
      return ridToId.size();
    }

    @Override
    public boolean isReady() {
      return true;
    }

    @Override
    public String getName() {
      return "recording-fake";
    }

    @Override
    public boolean coversVertexType(final String typeName) {
      return true;
    }

    @Override
    public boolean coversEdgeType(final String edgeTypeName) {
      return true;
    }

    @Override
    public int getNodeId(final RID rid) {
      return idFor(rid);
    }

    @Override
    public RID getRID(final int nodeId) {
      return idToRid.get(nodeId);
    }

    @Override
    public int[] getNeighborIds(final int nodeId, final Vertex.DIRECTION direction, final String... edgeTypes) {
      return new int[0];
    }

    @Override
    public long countEdges(final int nodeId, final Vertex.DIRECTION direction, final String... edgeTypes) {
      return 0;
    }

    @Override
    public boolean isConnectedTo(final int nodeA, final int nodeB, final Vertex.DIRECTION direction, final String... edgeTypes) {
      isConnectedToCalls++;
      lastDirection = direction;
      lastEdgeTypes = edgeTypes;
      return connected;
    }

    @Override
    public Object getProperty(final int nodeId, final String propertyName) {
      return null;
    }
  }

  private RecordingProvider provider;

  @AfterEach
  void unregisterProvider() {
    if (provider != null)
      GraphTraversalProviderRegistry.unregister(database, provider);
  }

  private MatchPathItem parsePathItem(final String matchPattern) {
    final MatchStatement stm = (MatchStatement) ((DatabaseInternal) database).getStatementCache()
        .get("MATCH " + matchPattern + " RETURN a,b");
    final MatchExpression expr = stm.getMatchExpressions().get(0);
    return expr.getItems().get(0);
  }

  private EdgeTraversal edgeTraversal(final MatchPathItem item, final boolean forward) {
    final PatternNode outNode = new PatternNode();
    outNode.alias = "a";
    final PatternNode inNode = new PatternNode();
    inNode.alias = "b";
    final PatternEdge patternEdge = new PatternEdge();
    patternEdge.out = outNode;
    patternEdge.in = inNode;
    patternEdge.item = item;
    return new EdgeTraversal(patternEdge, forward);
  }

  private ResultInternal boundSourceRecord(final Identifiable a, final Identifiable b) {
    final ResultInternal sourceRecord = new ResultInternal(database);
    sourceRecord.setProperty("a", a);
    sourceRecord.setProperty("b", b);
    return sourceRecord;
  }

  @Test
  void expandIntoPassesPatternEdgeTypeToProvider() {
    database.getSchema().createVertexType("Person");
    database.begin();
    final MutableVertex a = database.newVertex("Person").save();
    final MutableVertex b = database.newVertex("Person").save();
    database.commit();

    provider = new RecordingProvider();
    provider.connected = true;
    GraphTraversalProviderRegistry.register(database, provider);

    final MatchPathItem item = parsePathItem("{as:a}.out('KNOWS'){as:b}");
    final MatchEdgeTraverser traverser = new MatchEdgeTraverser(boundSourceRecord(a, b), edgeTraversal(item, true));

    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);

    assertThat(traverser.hasNext(context)).isTrue();
    // Before the fix, isConnectedTo() was called with no edge types at all (any type matches); it must now
    // receive exactly the pattern's own type.
    assertThat(provider.lastEdgeTypes).containsExactly("KNOWS");
    assertThat(provider.lastDirection).isEqualTo(Vertex.DIRECTION.OUT);
  }

  @Test
  void expandIntoForwardDirectionMatchesMethodName() {
    // a.in('KNOWS') must be checked as IN from a's perspective, not OUT: before the fix, the forward traverser
    // always asked for OUT regardless of the pattern's actual out/in/both method.
    database.getSchema().createVertexType("Person");
    database.begin();
    final MutableVertex a = database.newVertex("Person").save();
    final MutableVertex b = database.newVertex("Person").save();
    database.commit();

    provider = new RecordingProvider();
    provider.connected = true;
    GraphTraversalProviderRegistry.register(database, provider);

    final MatchPathItem item = parsePathItem("{as:a}.in('KNOWS'){as:b}");
    final MatchEdgeTraverser traverser = new MatchEdgeTraverser(boundSourceRecord(a, b), edgeTraversal(item, true));

    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);

    assertThat(traverser.hasNext(context)).isTrue();
    assertThat(provider.lastDirection).isEqualTo(Vertex.DIRECTION.IN);
  }

  @Test
  void expandIntoReverseTraverserFlipsDirection() {
    // MatchReverseEdgeTraverser starts from the pattern's in-side and runs the method's *reverse*, so for
    // a.out('KNOWS') scheduled backward (starting at b) it must ask the provider for IN from b's perspective -
    // before the fix, the reverse traverser inherited the same OUT-biased formula as the forward one.
    database.getSchema().createVertexType("Person");
    database.begin();
    final MutableVertex a = database.newVertex("Person").save();
    final MutableVertex b = database.newVertex("Person").save();
    database.commit();

    provider = new RecordingProvider();
    provider.connected = true;
    GraphTraversalProviderRegistry.register(database, provider);

    final MatchPathItem item = parsePathItem("{as:a}.out('KNOWS'){as:b}");
    final MatchEdgeTraverser traverser = new MatchReverseEdgeTraverser(boundSourceRecord(a, b), edgeTraversal(item, false));

    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);

    assertThat(traverser.hasNext(context)).isTrue();
    assertThat(provider.lastDirection).isEqualTo(Vertex.DIRECTION.IN);
    assertThat(provider.lastEdgeTypes).containsExactly("KNOWS");
  }

  @Test
  void expandIntoBothDirectionStaysBoth() {
    database.getSchema().createVertexType("Person");
    database.begin();
    final MutableVertex a = database.newVertex("Person").save();
    final MutableVertex b = database.newVertex("Person").save();
    database.commit();

    provider = new RecordingProvider();
    provider.connected = true;
    GraphTraversalProviderRegistry.register(database, provider);

    final MatchPathItem item = parsePathItem("{as:a}.both('KNOWS'){as:b}");

    final BasicCommandContext forwardContext = new BasicCommandContext();
    forwardContext.setDatabase(database);
    final MatchEdgeTraverser forward = new MatchEdgeTraverser(boundSourceRecord(a, b), edgeTraversal(item, true));
    assertThat(forward.hasNext(forwardContext)).isTrue();
    assertThat(provider.lastDirection).isEqualTo(Vertex.DIRECTION.BOTH);

    final BasicCommandContext reverseContext = new BasicCommandContext();
    reverseContext.setDatabase(database);
    final MatchEdgeTraverser reverse = new MatchReverseEdgeTraverser(boundSourceRecord(a, b), edgeTraversal(item, false));
    assertThat(reverse.hasNext(reverseContext)).isTrue();
    assertThat(provider.lastDirection).isEqualTo(Vertex.DIRECTION.BOTH);
  }

  @Test
  void expandIntoNotConnectedReturnsEmpty() {
    database.getSchema().createVertexType("Person");
    database.begin();
    final MutableVertex a = database.newVertex("Person").save();
    final MutableVertex b = database.newVertex("Person").save();
    database.commit();

    provider = new RecordingProvider();
    provider.connected = false;
    GraphTraversalProviderRegistry.register(database, provider);

    final MatchPathItem item = parsePathItem("{as:a}.out('KNOWS'){as:b}");
    final MatchEdgeTraverser traverser = new MatchEdgeTraverser(boundSourceRecord(a, b), edgeTraversal(item, true));

    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);

    assertThat(traverser.hasNext(context)).isFalse();
  }

  @Test
  void expandIntoSkipsWhileConditionItems() {
    // A while-conditioned item is a variable-depth traversal: isConnectedTo() only ever answers whether two
    // vertices are joined by a single hop, so it must never be consulted here even though both endpoints are
    // already bound - the recursive executeTraversal() has to run instead.
    database.getSchema().createVertexType("Person");
    database.getSchema().createEdgeType("KNOWS");
    database.begin();
    final MutableVertex a = database.newVertex("Person").save();
    final MutableVertex b = database.newVertex("Person").save();
    a.newEdge("KNOWS", b);
    database.commit();

    provider = new RecordingProvider();
    provider.connected = true;
    GraphTraversalProviderRegistry.register(database, provider);

    final MatchPathItem item = parsePathItem("{as:a}.out('KNOWS'){as:b, while: ($depth < 2)}");
    final MatchEdgeTraverser traverser = new MatchEdgeTraverser(boundSourceRecord(a, b), edgeTraversal(item, true));

    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);

    traverser.hasNext(context);
    assertThat(provider.isConnectedToCalls).isZero();
  }

  @Test
  void expandIntoSkipsMaxDepthItems() {
    database.getSchema().createVertexType("Person");
    database.getSchema().createEdgeType("KNOWS");
    database.begin();
    final MutableVertex a = database.newVertex("Person").save();
    final MutableVertex b = database.newVertex("Person").save();
    a.newEdge("KNOWS", b);
    database.commit();

    provider = new RecordingProvider();
    provider.connected = true;
    GraphTraversalProviderRegistry.register(database, provider);

    final MatchPathItem item = parsePathItem("{as:a}.out('KNOWS'){as:b, maxDepth: 2}");
    final MatchEdgeTraverser traverser = new MatchEdgeTraverser(boundSourceRecord(a, b), edgeTraversal(item, true));

    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);

    traverser.hasNext(context);
    assertThat(provider.isConnectedToCalls).isZero();
  }
}
