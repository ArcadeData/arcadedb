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
package com.arcadedb.query.opencypher.executor;

import com.arcadedb.query.opencypher.executor.CypherExecutionPlan.SeedCorrelation;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.engine.Bucket;
import com.arcadedb.graph.GraphTraversalProvider;
import com.arcadedb.graph.GraphTraversalProviderRegistry;
import com.arcadedb.graph.IncomingEdgeLookup;
import com.arcadedb.graph.Vertex;
import com.arcadedb.graph.VertexInternal;
import com.arcadedb.query.opencypher.ast.BooleanExpression;
import com.arcadedb.query.opencypher.ast.ComparisonExpression;
import com.arcadedb.query.opencypher.ast.CypherStatement;
import com.arcadedb.query.opencypher.ast.Direction;
import com.arcadedb.query.opencypher.ast.Expression;
import com.arcadedb.query.opencypher.ast.FunctionCallExpression;
import com.arcadedb.query.opencypher.ast.LogicalExpression;
import com.arcadedb.query.opencypher.ast.MatchClause;
import com.arcadedb.query.opencypher.ast.NodePattern;
import com.arcadedb.query.opencypher.ast.PathPattern;
import com.arcadedb.query.opencypher.ast.PatternPredicateExpression;
import com.arcadedb.query.opencypher.ast.PropertyAccessExpression;
import com.arcadedb.query.opencypher.ast.QuantifiedPathPattern;
import com.arcadedb.query.opencypher.ast.RelationshipPattern;
import com.arcadedb.query.opencypher.ast.VariableExpression;
import com.arcadedb.query.opencypher.ast.WhereClause;
import com.arcadedb.query.opencypher.executor.steps.AntiJoinChainOp;
import com.arcadedb.query.opencypher.executor.steps.CountOp;
import com.arcadedb.query.opencypher.executor.steps.DegreeProductOp;
import com.arcadedb.query.opencypher.executor.steps.EdgeCountOp;
import com.arcadedb.query.opencypher.executor.steps.PairHashJoinOp;
import com.arcadedb.query.opencypher.executor.steps.PartitionedTriangleOp;
import com.arcadedb.query.opencypher.executor.steps.PropagateChainOp;
import com.arcadedb.query.opencypher.executor.steps.VertexPredicate;
import com.arcadedb.query.opencypher.executor.steps.TriangleCountOp;
import com.arcadedb.query.opencypher.executor.steps.UniqueKeyAnchor;
import com.arcadedb.query.opencypher.optimizer.CypherOptimizer;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.EdgeType;
import com.arcadedb.schema.Property;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.VertexType;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Recognises the MATCH shapes whose {@code count(*)} a {@link CountOp} answers by walking adjacency lists, without
 * materializing a row per match: one edge, a chain, a chain with an anti-join, a star, a triangle, a pair join. Each
 * {@code tryDetect...} method answers the operator for the statement, or null when the statement is not that shape, and
 * {@link CypherExecutionPlan#tryOptimizeCountStar} decides in which order they are asked and whether the operator found
 * is worth running.
 * <p>
 * Split out of {@link CypherExecutionPlan}, which keeps what is not about a shape: whether the statement is a count at
 * all, and the step the operator is wrapped in.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class CountStarShapeDetector {
  private final CypherExecutionPlan plan;
  private final DatabaseInternal    database;
  private final CypherStatement     statement;

  CountStarShapeDetector(final CypherExecutionPlan plan, final DatabaseInternal database, final CypherStatement statement) {
    this.plan = plan;
    this.database = database;
    this.statement = statement;
  }

  /**
   * {@code MATCH ()-[e:T]->() RETURN count(e)}: one hop between two nodes that nothing constrains, whose count is the number
   * of relationships of the types, every edge type when none is written, matched once from each end when the hop is
   * undirected (issue #9600). {@link EdgeCountOp} reads it off a provider's totals or one walk of the edge lists, where the
   * row pipeline loads every edge.
   * <p>
   * A labelled or filtered end is left to the chain operator, and a node named at both ends, which keeps only the self loops,
   * to the row pipeline.
   *
   * @return the operator, or null when the statement is not that shape
   */
  CountOp tryDetectEdgeCountStar(final Database db) {
    if (statement.getMatchClauses() == null || statement.getMatchClauses().size() != 1)
      return null;
    final MatchClause matchClause = statement.getMatchClauses().get(0);
    if (matchClause.isOptional() || matchClause.hasWhereClause() || statement.getWhereClause() != null)
      return null;
    if (!matchClause.hasPathPatterns() || matchClause.getPathPatterns().size() != 1)
      return null;

    final PathPattern pathPattern = matchClause.getPathPatterns().get(0);
    if (pathPattern.getRelationshipCount() != 1 || pathPattern.hasPathVariable())
      return null;

    final RelationshipPattern rel = pathPattern.getRelationship(0);
    if (rel instanceof QuantifiedPathPattern || rel.isVariableLength() || rel.hasProperties()
        || rel.getPropertiesParameterName() != null || rel.hasWhereExpression())
      return null;
    if (rel.getVariable() != null && !rel.getVariable().isEmpty() && !plan.isNamedOnce(rel.getVariable()))
      return null;

    final NodePattern source = pathPattern.getFirstNode();
    final NodePattern target = pathPattern.getLastNode();
    if (!isUnconstrainedNode(source) || !isUnconstrainedNode(target))
      return null;
    if (source.getVariable() != null && !source.getVariable().isEmpty() && source.getVariable().equals(target.getVariable()))
      return null;

    final Schema schema = db.getSchema();
    // An undirected hop over a unidirectional type never gets here: tryOptimizeCountStar declines it, its incoming side is
    // not stored
    final boolean undirected = rel.getDirection() == Direction.BOTH;
    if (!rel.hasTypes())
      return new EdgeCountOp(null, undirected);

    // The edge types of the alternatives, each family once: a type whose super-type is listed too is already counted by it.
    // Only for the plan's sake: the provider and the edge lists count a union of the types, so an edge whose type reaches
    // two listed families (an edge type may have several super-types) is counted once either way
    final List<String> declared = new ArrayList<>(rel.getTypes().size());
    for (final String name : rel.getTypes())
      if (schema.getTypeOrNull(name) instanceof EdgeType && !declared.contains(name))
        declared.add(name);
    final List<String> types = new ArrayList<>(declared.size());
    for (final String name : declared)
      if (!isSubTypeOfAnother(schema, name, declared))
        types.add(name);
    // A name that is no edge type matches nothing; with none left the row pipeline answers its 0
    if (types.isEmpty())
      return null;
    return new EdgeCountOp(types.toArray(new String[0]), undirected);
  }

  private static boolean isSubTypeOfAnother(final Schema schema, final String name, final List<String> names) {
    for (final String other : names)
      if (!other.equals(name) && schema.getType(name).isSubTypeOf(other))
        return true;
    return false;
  }

  /** A node position that admits every vertex: no label, no property map, no dynamic label, no inline predicate. */
  private static boolean isUnconstrainedNode(final NodePattern node) {
    return !node.hasLabels() && !node.hasProperties() && !node.hasDynamicLabels() && !node.hasWhereExpression();
  }

  CountOp tryDetectChainCountStar(final Database db, final SeedCorrelation correlation,
      final CommandContext context) {
    // Exactly one MATCH clause
    if (statement.getMatchClauses() == null || statement.getMatchClauses().size() != 1)
      return null;
    return tryDetectChainCountStar(db, correlation, context, statement.getMatchClauses().get(0));
  }

  /**
   * The chain and anti-join chain operators read one path pattern, so a chain cut into comma-separated parts -
   * {@code (t:Tag)<-[:HAS_TAG]-(m:Message), (m)<-[:REPLY_OF]-(c:Comment)} - reached neither, however it was ordered
   * (issue #9599). Asked only once every detector has declined the statement as written, so a plan that works today is not
   * changed: the parts are joined into the one chain they describe and read from either end. Both ends count the same
   * paths; the walk starts from the end whose label has fewer vertices, the anchors it enumerates, and a tie falls to the
   * operator's description, so the order the parts were written in decides nothing.
   */
  CountOp tryDetectJoinedChainCountStar(final CommandContext context, final SeedCorrelation correlation,
      final boolean unidirectional) {
    final Database db = context.getDatabase();
    if (statement.getMatchClauses() == null || statement.getMatchClauses().size() != 1)
      return null;
    final MatchClause matchClause = statement.getMatchClauses().get(0);
    if (matchClause.isOptional() || !matchClause.hasPathPatterns() || matchClause.getPathPatterns().size() < 2)
      return null;
    final PatternGraph graph = PatternGraph.of(matchClause.getPathPatterns());
    if (graph == null)
      return null;

    CountOp best = null;
    long bestAnchors = 0;
    String bestDescription = null;
    final PairJoinStatistics statistics = new PairJoinStatistics(db);
    for (final PathPattern chain : graph.pathOrientations()) {
      // the gate in tryOptimizeCountStar let a unidirectional type through only for outgoing hops, as written: read from
      // the other end they would walk the incoming side, which is not stored
      if (unidirectional && !allOutgoing(chain))
        continue;
      final MatchClause joined = new MatchClause(List.of(chain), false, matchClause.getWhereClause());
      CountOp op = tryDetectChainCountStar(db, correlation, context, joined);
      // the anti-join walks from a label's anchors, never from a bound name, as in tryOptimizeCountStar
      if (op == null && !correlation.isCorrelated() && !unidirectional)
        op = tryDetectAntiJoinChainCountStar(context, joined);
      if (op == null)
        continue;
      final NodePattern start = chain.getFirstNode();
      final long anchors = statistics.vertices(start.hasLabels() ? start.getLabels().get(0) : null);
      final String description = op.describe(0, 0);
      if (best == null || anchors < bestAnchors || (anchors == bestAnchors && description.compareTo(bestDescription) < 0)) {
        best = op;
        bestAnchors = anchors;
        bestDescription = description;
      }
    }
    return best;
  }

  private static boolean allOutgoing(final PathPattern path) {
    for (int i = 0; i < path.getRelationshipCount(); i++)
      if (path.getRelationship(i).getDirection() != Direction.OUT)
        return false;
    return true;
  }

  /** The chain over one MATCH clause: the statement's own, or its patterns joined into one chain (issue #9599). */
  private CountOp tryDetectChainCountStar(final Database db, final SeedCorrelation correlation, final CommandContext context,
      final MatchClause matchClause) {
    if (matchClause.isOptional())
      return null;

    // Exactly one path pattern with at least one relationship
    if (!matchClause.hasPathPatterns() || matchClause.getPathPatterns().size() != 1)
      return null;
    final PathPattern pathPattern = matchClause.getPathPatterns().get(0);
    if (pathPattern.getRelationshipCount() < 1)
      return null;
    if (pathPattern.hasPathVariable())
      return null;

    // WHERE: at most one node inequality (var1 <> var2), and any number of conjuncts that read one node of the chain
    // and nothing else, which the operator applies per vertex like an inline property map (issue #9595)
    String inequalityVar1 = null;
    String inequalityVar2 = null;
    String inequalityProperty = null;
    final CountPushDownPredicates predicates = new CountPushDownPredicates(context);
    // the parser writes a MATCH's WHERE on the clause: a statement carrying one as well would have one of them ignored
    if (matchClause.hasWhereClause() && statement.getWhereClause() != null)
      return null;
    final WhereClause whereClause = matchClause.hasWhereClause() ? matchClause.getWhereClause() : statement.getWhereClause();
    if (whereClause != null) {
      if (whereClause.getConditionExpression() == null)
        return null;
      final Set<String> nodeVariables = nodeVariablesOf(pathPattern);
      for (final BooleanExpression conjunct : CountPushDownPredicates.conjuncts(whereClause.getConditionExpression())) {
        if (inequalityVar1 == null && conjunct instanceof ComparisonExpression comparison) {
          final String[] ineqPair = extractInequalityFromComparison(comparison);
          if (ineqPair != null) {
            inequalityVar1 = ineqPair[0];
            inequalityVar2 = ineqPair[1];
            inequalityProperty = ineqPair[2];
            continue;
          }
        }
        if (!predicates.addWhereConjunct(conjunct, nodeVariables))
          return null;
      }
    }
    if (inequalityProperty != null && !propertyIdentifiesNode(db, pathPattern, inequalityVar1, inequalityVar2, inequalityProperty))
      return null;

    // All relationships must be fixed-length, anonymous, no properties
    final int hopCount = pathPattern.getRelationshipCount();
    final String[] nodeLabels = new String[hopCount + 1];
    final String[] edgeTypes = new String[hopCount];
    final Vertex.DIRECTION[] directions = new Vertex.DIRECTION[hopCount];
    final VertexPredicate[] nodePredicates = new VertexPredicate[hopCount + 1];

    for (int i = 0; i <= hopCount; i++) {
      final NodePattern node = pathPattern.getNode(i);
      // One name per hop is all the operator carries, and a conjunction or a disjunction is not one
      // name; rather than count a set the pattern did not describe, decline (issue #6322).
      if (!hasPushDownRepresentableLabel(node))
        return null;
      if (!CountPushDownPredicates.inlinePropertiesArePerVertex(node))
        return null;
      nodeLabels[i] = node.hasLabels() ? node.getLabels().get(0) : null;
    }

    for (int i = 0; i < hopCount; i++) {
      final RelationshipPattern rel = pathPattern.getRelationship(i);
      if (rel.isVariableLength())
        return null;
      // count(r) of a relationship named nowhere else is count(*) (issue #9600). Naming it is also what lets an inline
      // predicate read it, -[r:T WHERE r.x = 1]->, which the operator cannot apply
      if (rel.getVariable() != null && !rel.getVariable().isEmpty() && !plan.isNamedOnce(rel.getVariable()))
        return null;
      if (rel.hasProperties() || rel.getPropertiesParameterName() != null || rel.hasWhereExpression())
        return null;
      if (!rel.hasTypes() || rel.getTypes().size() != 1)
        return null;

      edgeTypes[i] = rel.getTypes().get(0);
      final Direction dir = rel.getDirection();
      if (dir == Direction.OUT)
        directions[i] = Vertex.DIRECTION.OUT;
      else if (dir == Direction.IN)
        directions[i] = Vertex.DIRECTION.IN;
      else
        directions[i] = Vertex.DIRECTION.BOTH;
    }

    // A node variable named twice is one vertex, a constraint the operator cannot express: it counts the chain as if
    // every position were free, so (x)-[:K]->(x) would count every K edge instead of the self-loops (issue #8941)
    if (CypherExecutionPlan.repeatsNodeVariable(pathPattern))
      return null;

    // One hop between two nodes keyed by an equality on a UNIQUE index joins two vertices at most, which the ordinary plan
    // reads off the shorter of their edge lists; the chain count walks every edge of the first one (issue #11699). A key
    // written in WHERE pins the node as an inline one does, so the guard reads the pattern with those equalities folded in.
    // An IN-list on the key is one vertex per element, which the ordinary plan seeks too when the walk is the longer and the
    // output cannot show the row order the seek changes (issue #11715)
    if (hopCount == 1) {
      final PathPattern keyed = HopEdgeTrackingAnalyzer.withWhereEqualities(List.of(pathPattern), whereClause).getFirst();
      final Map<String, Set<String>> inListKeys = CypherOptimizer.rowOrderIsInvisible(statement) ?
          HopEdgeTrackingAnalyzer.whereInListKeys(whereClause) :
          Map.of();
      if (plan.stepBuilder.matchSteps.isUniquelyKeyed(keyed.getNode(0), inListKeys)
          && plan.stepBuilder.matchSteps.isUniquelyKeyed(keyed.getNode(1), inListKeys))
        return null;
    }

    // Resolve inequality variable positions in the chain
    int inequalityIdxA = -1;
    int inequalityIdxB = -1;
    if (inequalityVar1 != null) {
      for (int i = 0; i <= hopCount; i++) {
        final NodePattern node = pathPattern.getNode(i);
        final String nv = node.getVariable();
        if (nv != null) {
          if (nv.equals(inequalityVar1))
            inequalityIdxA = i;
          else if (nv.equals(inequalityVar2))
            inequalityIdxB = i;
        }
      }
      if (inequalityIdxA < 0 || inequalityIdxB < 0)
        return null;
    }

    // Count-push-down does NOT enforce edge uniqueness, so it's only safe when no two hops can bind the same edge
    if (!chainHopsAreUnique(db, edgeTypes, directions, inequalityIdxA, inequalityIdxB))
      return null;

    // Resolved last, once the chain is known to be accepted: an inline value is evaluated here
    for (int i = 0; i <= hopCount; i++)
      nodePredicates[i] = predicates.predicateFor(pathPattern.getNode(i));

    if (!correlation.isCorrelated()) {
      // An end pinned by a UNIQUE key is one vertex at most: looked up when the count runs, the chain is walked from it alone,
      // which costs the seek and that vertex's paths, not a walk of either label (issue #11594). The seeded walk has no
      // inequality to apply, so a chain carrying one is left to what follows
      if (inequalityVar1 == null) {
        final CountOp keyed = uniqueKeySeededChainOp(db, pathPattern, whereClause, nodeLabels, edgeTypes, directions, nodePredicates,
            context);
        if (keyed != null)
          return keyed;
      }
      // The propagation enumerates the label of its first position. Written in the direction of the edge, a chain often
      // puts the anchored node last, (c:Chunk)-[:OF_RECORD]->(r:Record {external_id: $id}), which enumerated every chunk
      // to keep the paths of one record (issue #11594). The far end is reached by reading the same arrays the other way,
      // as seededChainOp does, and the count of paths is the same
      if (startsAtTheFarEnd(db, nodeLabels, edgeTypes, nodePredicates))
        return new PropagateChainOp(reversed(nodeLabels), reversed(edgeTypes), reversedDirections(directions),
            inequalityIdxA < 0 ? -1 : hopCount - inequalityIdxA, inequalityIdxB < 0 ? -1 : hopCount - inequalityIdxB, null,
            null, reversed(nodePredicates));
      return new PropagateChainOp(nodeLabels, edgeTypes, directions, inequalityIdxA, inequalityIdxB, null, null,
          nodePredicates);
    }

    return seededChainOp(db, correlation, pathPattern, nodeLabels, edgeTypes, directions, nodePredicates,
        inequalityVar1 != null);
  }

  /**
   * The chain seeded from the end that an equality on every column of a UNIQUE index pins to one vertex, the first end
   * preferred, or null when neither end is pinned (issue #11594). A key written in WHERE pins the node as an inline one does.
   * The last end is reached by reading the chain the other way, as {@link #seededChainOp} does, which a chain over an edge
   * type declared unidirectional cannot be: the incoming side is stored on no vertex.
   */
  private CountOp uniqueKeySeededChainOp(final Database db, final PathPattern pathPattern, final WhereClause where,
      final String[] nodeLabels, final String[] edgeTypes, final Vertex.DIRECTION[] directions,
      final VertexPredicate[] nodePredicates, final CommandContext context) {
    final PathPattern keyed = HopEdgeTrackingAnalyzer.withWhereEqualities(List.of(pathPattern), where).getFirst();
    final UniqueKeyAnchor first = plan.stepBuilder.matchSteps.uniqueKeyAnchorOf(keyed.getNode(0), context);
    if (first != null)
      return new PropagateChainOp(nodeLabels, edgeTypes, directions, first, nodePredicates);
    if (IncomingEdgeLookup.isAnyUnidirectional(db.getSchema(), edgeTypes))
      return null;
    final UniqueKeyAnchor last = plan.stepBuilder.matchSteps.uniqueKeyAnchorOf(keyed.getNode(edgeTypes.length), context);
    if (last == null)
      return null;
    return new PropagateChainOp(reversed(nodeLabels), reversed(edgeTypes), reversedDirections(directions), last,
        reversed(nodePredicates));
  }

  /**
   * Whether an uncorrelated chain is cheaper to enumerate from its last position than from its first (issue #11594). The end
   * that carries the only node predicate starts, since the predicate is applied to the vertices the start enumerates and
   * the rest of the chain is walked only from those that pass. With a predicate on neither end or on both, the end whose
   * label holds fewer vertices starts. An unlabeled end is never chosen over a labeled one: it enumerates every vertex. A chain
   * over an edge type declared unidirectional keeps its written orientation.
   */
  private static boolean startsAtTheFarEnd(final Database db, final String[] nodeLabels, final String[] edgeTypes,
      final VertexPredicate[] predicates) {
    // Read from its far end every hop is walked against its written direction, and the incoming side of an edge type declared
    // unidirectional is stored on no vertex: such a chain is only walked as written, the outgoing hops tryOptimizeCountStar lets
    // through for it
    if (IncomingEdgeLookup.isAnyUnidirectional(db.getSchema(), edgeTypes))
      return false;
    final int last = nodeLabels.length - 1;
    if (nodeLabels[last] == null)
      return false;
    if (nodeLabels[0] == null)
      return true;
    final boolean firstFiltered = predicates[0] != null;
    final boolean lastFiltered = predicates[last] != null;
    if (firstFiltered != lastFiltered)
      return lastFiltered;
    return labelCardinality(db, nodeLabels[last]) < labelCardinality(db, nodeLabels[0]);
  }

  /** The vertices a label holds, its subtypes included, or 0 when no vertex type carries the name. */
  private static long labelCardinality(final Database db, final String label) {
    return db.getSchema().getTypeOrNull(label) instanceof VertexType ? db.countType(label, true) : 0L;
  }

  /** The node variables a path pattern binds, anonymous positions left out. */
  private static Set<String> nodeVariablesOf(final PathPattern pathPattern) {
    final Set<String> variables = new HashSet<>();
    for (int i = 0; i <= pathPattern.getRelationshipCount(); i++) {
      final String variable = pathPattern.getNode(i).getVariable();
      if (variable != null && !variable.isEmpty())
        variables.add(variable);
    }
    return variables;
  }

  /**
   * Whether no two hops of a chain can bind the same edge, so that counting adjacency paths is counting relationship
   * paths (issues #6322, #8426). Two hops can share an edge only when their types overlap, and overlap is
   * inheritance-aware: a {@code KC} edge matches both {@code [:K]} and {@code [:KC]} when {@code KC EXTENDS K}.
   * <p>
   * An overlapping pair is rescued by an inequality between two nodes that binding the same edge would force equal: hop
   * {@code i} runs between nodes {@code i} and {@code i + 1}, so the same edge for hops {@code i} and {@code j} means the
   * source of one is the source of the other and likewise the targets. For adjacent hops that is {@code v(i) = v(i + 2)}
   * (the self-loop case), for {@code (a)<-[:T]-(m)<-[:R]-(c)-[:T]->(b)} it is {@code m = c} and {@code a = b}, so
   * {@code WHERE a <> b} protects the two {@code T} hops although they are not adjacent (LSQB Q5). An undirected hop can
   * be traversed either way, so the inequality has to hold under every orientation of such hops. A three-hop chain
   * with {@code WHERE a <> d} still lets its first and third hop bind one edge when they overlap, because only the
   * middle nodes would have to coincide with the ends, and no inequality names the nodes that are forced equal, so
   * that chain is declined. Every overlapping pair must be protected by the one inequality the operator carries.
   */
  private static boolean chainHopsAreUnique(final Database db, final String[] edgeTypes, final Vertex.DIRECTION[] directions,
      final int inequalityIdxA, final int inequalityIdxB) {
    for (int i = 0; i < edgeTypes.length; i++)
      for (int j = i + 1; j < edgeTypes.length; j++)
        if (CypherOptimizer.edgeTypesMayOverlap(db.getSchema(), edgeTypes[i], edgeTypes[j])
            && !inequalityForcedBySharedEdge(directions, i, j, inequalityIdxA, inequalityIdxB))
          return false;
    return true;
  }

  /**
   * Whether hops {@code i < j} binding one edge forces nodes {@code a} and {@code b} to be the same vertex, under every
   * orientation the hops' directions allow. Source and target of a hop are the nodes it joins, ordered by direction.
   */
  private static boolean inequalityForcedBySharedEdge(final Vertex.DIRECTION[] directions, final int i, final int j,
      final int a, final int b) {
    if (a < 0 || b < 0 || a == b)
      return false;

    // forward: node k is the source, node k + 1 the target; reversed: the other way round
    final boolean[] orientationsI = directions[i] == Vertex.DIRECTION.BOTH ? new boolean[] { true, false } :
        new boolean[] { directions[i] == Vertex.DIRECTION.OUT };
    final boolean[] orientationsJ = directions[j] == Vertex.DIRECTION.BOTH ? new boolean[] { true, false } :
        new boolean[] { directions[j] == Vertex.DIRECTION.OUT };

    for (final boolean forwardI : orientationsI)
      for (final boolean forwardJ : orientationsJ) {
        // union-find over node positions, a chain has few of them
        final int[] parent = new int[Math.max(Math.max(i, j) + 2, Math.max(a, b) + 1)];
        for (int n = 0; n < parent.length; n++)
          parent[n] = n;
        sharedEdgeUnion(parent, forwardI ? i : i + 1, forwardJ ? j : j + 1);
        sharedEdgeUnion(parent, forwardI ? i + 1 : i, forwardJ ? j + 1 : j);
        if (sharedEdgeFind(parent, a) != sharedEdgeFind(parent, b))
          return false;
      }
    return true;
  }

  private static int sharedEdgeFind(final int[] parent, int n) {
    while (parent[n] != n)
      n = parent[n] = parent[parent[n]];
    return n;
  }

  private static void sharedEdgeUnion(final int[] parent, final int x, final int y) {
    parent[sharedEdgeFind(parent, x)] = sharedEdgeFind(parent, y);
  }

  /**
   * The same chain walked from the vertex the outer row bound, or null when the correlation is not one anchor at one
   * end of the chain.
   * <p>
   * The enumerating operator seeds its propagation from a label's bucket set, which is why a body reading a seeded
   * name had to lose the push-down: {@code MATCH (q)-[:LINKS]->(x:Q) RETURN count(*)} with {@code q} bound would have
   * been answered with the count over every {@code q} in the graph. A bound anchor does not make that count expensive
   * though, it makes it cheaper - an O(degree) read of the adjacency arrays - so the guard narrows from "the body
   * reads a seeded name" to "the body reads a seeded name at a position this operator cannot seed from" (issue
   * #5758).
   * <p>
   * What it can seed from is <b>one</b> name, bound to <b>one</b> vertex, at <b>one</b> end of the chain:
   * <ul>
   * <li>Two seeded names are two anchor sets, and the propagation carries one.</li>
   * <li>A name written at two positions - {@code (q)-[:LINKS]->(q)} - is an equality between the two ends that the
   * propagation does not enforce; it would count every path, not the returning ones.</li>
   * <li>A name in the middle of the chain would have to be walked outwards in both directions at once.</li>
   * <li>A name read somewhere other than a node position - only in the projection, say - names no position at all,
   * so {@code anchorIdx} stays -1 and the same test refuses it.</li>
   * <li>An inequality is refused outright: its two positions are indexes into the chain, and the reversal below
   * would have to renumber them for a case worth no complication.</li>
   * </ul>
   * The far end is reached by reversing the chain rather than by a second walk: {@code (a)-[:E]->(b)} counted from
   * {@code b} is {@code (b)<-[:E]-(a)}, which is the same arrays read the other way. That is what keeps
   * {@code COUNT { (:Person)-[:KNOWS]->(p) }} - "how many know me" - on the fast path alongside its mirror.
   * <p>
   * <b>The bound value is always the outer one.</b> The whole thing rests on the seeded name still meaning what the
   * outer row bound, and what could break that is a body rebinding the name for itself. It cannot: the only clauses
   * that bind a name to something other than a pattern position are {@code UNWIND} and {@code WITH}, and
   * {@link CypherExecutionPlan#isMatchReturnOnlyStatement()} - asked by {@link CypherExecutionPlan#tryOptimizeCountStar} before any detector runs - admits
   * nothing but {@code MATCH} and {@code RETURN}. A body's own {@code MATCH (q:Q)} written under an outer {@code q}
   * is not a rebinding but the correlation itself, which is exactly what this seeds from.
   */
  private CountOp seededChainOp(final Database db, final SeedCorrelation correlation, final PathPattern pathPattern,
      final String[] nodeLabels, final String[] edgeTypes, final Vertex.DIRECTION[] directions,
      final VertexPredicate[] nodePredicates, final boolean hasInequality) {
    if (!correlation.isSeedable() || hasInequality || correlation.readNames().size() != 1)
      return null;

    final String seededName = correlation.readNames().iterator().next();
    final int hopCount = edgeTypes.length;

    int anchorIdx = -1;
    for (int i = 0; i <= hopCount; i++)
      if (seededName.equals(pathPattern.getNode(i).getVariable())) {
        if (anchorIdx >= 0)
          return null;
        anchorIdx = i;
      }

    if (anchorIdx != 0 && anchorIdx != hopCount)
      return null;

    final Object bound = boundElement(correlation.boundValue(seededName));
    final RID anchorRid = boundVertexRid(db, bound);
    if (anchorRid == null)
      return null;
    // The bound vertex itself when the row holds one that reads its own edge lists, so the seeded walk does not load
    // it again. A GAVVertex is left out on purpose: it is not a VertexInternal, and it answers adjacency from
    // whichever provider produced it, which may hold only part of the graph (issue #5757).
    final VertexInternal anchorVertex = bound instanceof VertexInternal vertex ? vertex : null;

    if (anchorIdx == 0)
      return new PropagateChainOp(nodeLabels, edgeTypes, directions, -1, -1, anchorRid, anchorVertex, nodePredicates);

    return new PropagateChainOp(reversed(nodeLabels), reversed(edgeTypes), reversedDirections(directions), -1, -1,
        anchorRid, anchorVertex, reversed(nodePredicates));
  }

  /**
   * The RID of a bound value that is a vertex, or null for anything else.
   * <p>
   * A node pattern matches only vertices, so a name bound to a scalar, to a document or to an edge matches nothing.
   * Rather than teach the operator to answer 0 for those, they are left to the ordinary pipeline, which is the only
   * thing here that knows what each of them means - and which is what answered them before this existed.
   */
  private static RID boundVertexRid(final Database db, final Object bound) {
    if (!(bound instanceof Identifiable identifiable))
      return null;

    final RID rid = identifiable.getIdentity();
    if (rid == null)
      return null;
    return db.getSchema().getTypeByBucketId(rid.getBucketId()) instanceof VertexType ? rid : null;
  }

  /** What a bound name holds: a row carries its element inside a {@link Result}. */
  private static Object boundElement(final Object value) {
    return value instanceof Result result ? result.getElement().orElse(null) : value;
  }

  private static String[] reversed(final String[] values) {
    final String[] out = new String[values.length];
    for (int i = 0; i < values.length; i++)
      out[i] = values[values.length - 1 - i];
    return out;
  }

  private static VertexPredicate[] reversed(final VertexPredicate[] values) {
    final VertexPredicate[] out = new VertexPredicate[values.length];
    for (int i = 0; i < values.length; i++)
      out[i] = values[values.length - 1 - i];
    return out;
  }

  /** Reverses the hop order and flips each direction, so the chain reads identically from its far end. */
  private static Vertex.DIRECTION[] reversedDirections(final Vertex.DIRECTION[] directions) {
    final Vertex.DIRECTION[] out = new Vertex.DIRECTION[directions.length];
    for (int i = 0; i < directions.length; i++) {
      final Vertex.DIRECTION direction = directions[directions.length - 1 - i];
      if (direction == Vertex.DIRECTION.OUT)
        out[i] = Vertex.DIRECTION.IN;
      else if (direction == Vertex.DIRECTION.IN)
        out[i] = Vertex.DIRECTION.OUT;
      else
        out[i] = Vertex.DIRECTION.BOTH;
    }
    return out;
  }

  /**
   * Attempts to optimize a star-join pattern with {@code RETURN count(*)}.
   * <p>
   * Detects patterns where multiple MATCH/OPTIONAL MATCH path patterns share a single
   * central node variable, and all other nodes are anonymous. For each central node,
   * the count is the product of degrees (or max(1,degree) for optional arms).
   * <p>
   * Covers Q4: {@code MATCH (:Tag)<-[:HAS_TAG]-(m:Message)-[:HAS_CREATOR]->(:Person), (m)<-[:LIKES]-(:Person), (m)<-[:REPLY_OF]-(:Comment)}
   * Covers Q7: same mandatory + OPTIONAL MATCH arms
   *
   * @return optimized CountStarJoinStep if pattern matches, null otherwise
   */
  CountOp tryDetectStarCountStar(final CommandContext context) {
    // Must have at least one MATCH clause
    if (statement.getMatchClauses() == null || statement.getMatchClauses().isEmpty())
      return null;

    // No statement-level WHERE
    if (statement.getWhereClause() != null)
      return null;

    // Find the central variable: the one that appears in multiple path patterns.
    // First pass: count occurrences of each variable across all path patterns.
    final HashMap<String, Integer> varCounts = new HashMap<>();
    for (final MatchClause mc : statement.getMatchClauses()) {
      if (!mc.hasPathPatterns())
        continue;
      for (final PathPattern pp : mc.getPathPatterns())
        for (int i = 0; i <= pp.getRelationshipCount(); i++) {
          final String nv = pp.getNode(i).getVariable();
          if (nv != null && !nv.isEmpty())
            varCounts.merge(nv, 1, Integer::sum);
        }
    }
    // The central variable must be the ONLY variable appearing in multiple patterns.
    // If two or more variables appear in multiple patterns, it's a pair-join, not a star.
    String centralVar = null;
    String centralLabel = null;
    for (final var entry : varCounts.entrySet())
      if (entry.getValue() > 1) {
        if (centralVar != null)
          return null; // two shared variables → not a star join (likely a pair join)
        centralVar = entry.getKey();
      }
    if (centralVar == null)
      return null; // no variable appears in multiple patterns

    // Find the label for the central variable. The variable is written once per arm and the
    // operator enumerates one type of central node, so every occurrence has to agree on it: a label
    // set the single name cannot stand for, or two occurrences naming different types, declines the
    // push-down and leaves the query to the ordinary pipeline, which applies each of them (#6322).
    // A dynamic label on any occurrence is just as unenforceable as a rejected label set - the operator has
    // no way to check it - so it declines the push-down too, exactly as the arm-endpoint loop below does
    // (#6431). In practice this is caught earlier by hasDynamicLabel() in tryOptimizeCountStar (#5071);
    // this check exists so the detector is correct standing alone, not only in combination with that outer
    // guard. An inline property map is a per-vertex filter on the central node (issue #9595), written on
    // one occurrence of a mandatory clause: in an OPTIONAL clause it is a condition of that arm alone, which
    // a filter on the central node does not express.
    NodePattern centralWithProperties = null;
    for (final MatchClause mc : statement.getMatchClauses()) {
      if (!mc.hasPathPatterns()) continue;
      for (final PathPattern pp : mc.getPathPatterns())
        for (int i = 0; i <= pp.getRelationshipCount(); i++) {
          final NodePattern node = pp.getNode(i);
          if (!centralVar.equals(node.getVariable()))
            continue;
          if (node.hasDynamicLabels())
            return null;
          if (node.hasProperties()) {
            if (mc.isOptional() || centralWithProperties != null || !CountPushDownPredicates.inlinePropertiesArePerVertex(node))
              return null;
            centralWithProperties = node;
          }
          if (!node.hasLabels())
            continue;
          if (!hasPushDownRepresentableLabel(node))
            return null;
          if (labelsConflict(centralLabel, node.getLabels().get(0)))
            return null;
          centralLabel = node.getLabels().get(0);
        }
    }

    // The WHERE of each clause may hold conjuncts that read one node of the star: a mandatory clause filters the
    // central node or the node a mandatory arm reaches, an OPTIONAL clause only what its own arms reach, because there
    // the condition decides whether that arm matched, not whether the row exists (issue #9595)
    final CountPushDownPredicates predicates = new CountPushDownPredicates(context);
    final Set<String> mandatoryVariables = new HashSet<>();
    for (final MatchClause mc : statement.getMatchClauses())
      if (!mc.isOptional() && mc.hasPathPatterns())
        for (final PathPattern pp : mc.getPathPatterns())
          mandatoryVariables.addAll(nodeVariablesOf(pp));
    for (final MatchClause mc : statement.getMatchClauses()) {
      if (!mc.hasWhereClause())
        continue;
      if (mc.getWhereClause().getConditionExpression() == null || !mc.hasPathPatterns())
        return null;
      final Set<String> allowed;
      if (mc.isOptional()) {
        allowed = new HashSet<>();
        for (final PathPattern pp : mc.getPathPatterns())
          allowed.addAll(nodeVariablesOf(pp));
        allowed.remove(centralVar);
      } else
        allowed = mandatoryVariables;
      for (final BooleanExpression conjunct : CountPushDownPredicates.conjuncts(mc.getWhereClause().getConditionExpression()))
        if (!predicates.addWhereConjunct(conjunct, allowed))
          return null;
    }

    final ArrayList<DegreeProductOp.Arm> armList = new ArrayList<>();

    for (final MatchClause matchClause : statement.getMatchClauses()) {
      if (!matchClause.hasPathPatterns())
        return null;
      final boolean isOptional = matchClause.isOptional();
      // The degree product counts every combination of arms, including the one where two arms are the same edge (issue #9485)
      if (plan.hopEdges.clauseHopsMayShareAnEdge(matchClause))
        return null;
      final int armsBefore = armList.size();

      for (final PathPattern pathPattern : matchClause.getPathPatterns()) {
        if (pathPattern.hasPathVariable())
          return null;

        if (pathPattern.getRelationshipCount() < 1) {
          // Single-node pattern of the central variable: an anchor that adds no arm. A single node of any other
          // variable is a separate part whose count multiplies the star's, which the degree product does not compute
          if (pathPattern.isSingleNode() && centralVar.equals(pathPattern.getNode(0).getVariable()))
            continue;
          return null;
        }

        // Find the central variable's position in this path pattern.
        // Non-central named variables are ignored — they're endpoint bindings
        // that don't affect the degree-product logic for count(*).
        int centralNodeIdx = -1;
        for (int i = 0; i <= pathPattern.getRelationshipCount(); i++) {
          final NodePattern node = pathPattern.getNode(i);
          if (centralVar.equals(node.getVariable())) {
            centralNodeIdx = i;
            break;
          }
        }

        if (centralNodeIdx < 0)
          return null;

        // The arm walks from ONE position of the central node, so the same variable at a second position of the
        // arm (a path closing on the central vertex) is a constraint the degree product drops (issue #8941)
        for (int i = centralNodeIdx + 1; i <= pathPattern.getRelationshipCount(); i++)
          if (centralVar.equals(pathPattern.getNode(i).getVariable()))
            return null;

        final int totalHops = pathPattern.getRelationshipCount();

        // A label on a non-central node (the far endpoint or an interior node of a multi-hop arm) is a filter on what the
        // hop reaches: the operator carries it per hop and enforces it (#6337), so (:Author) and () no longer build the
        // same operator, and so is an inline property map of constants (issue #9595). A dynamic label, a label set one
        // name cannot stand for or a property value read off the row is still something the operator has no way to
        // check, so it declines the push-down rather than silently drop it (#6431, #6322).
        for (int i = 0; i <= totalHops; i++) {
          if (i == centralNodeIdx)
            continue;
          final NodePattern node = pathPattern.getNode(i);
          if (node.hasDynamicLabels() || !CountPushDownPredicates.inlinePropertiesArePerVertex(node))
            return null;
          if (node.hasLabels() && !hasPushDownRepresentableLabel(node))
            return null;
        }

        if (centralNodeIdx == 0) {
          final DegreeProductOp.Arm arm = buildArmForward(pathPattern, 0, totalHops, isOptional, predicates);
          if (arm == null) return null;
          armList.add(arm);
        } else if (centralNodeIdx == totalHops) {
          final DegreeProductOp.Arm arm = buildArmBackward(pathPattern, totalHops, 0, isOptional, predicates);
          if (arm == null) return null;
          armList.add(arm);
        } else {
          final DegreeProductOp.Arm leftArm = buildArmBackward(pathPattern, centralNodeIdx, 0, isOptional, predicates);
          if (leftArm == null) return null;
          armList.add(leftArm);
          final DegreeProductOp.Arm rightArm = buildArmForward(pathPattern, centralNodeIdx, totalHops, isOptional, predicates);
          if (rightArm == null) return null;
          armList.add(rightArm);
        }
      }

      // An OPTIONAL clause matches as a whole: when one of its arms reaches nothing the clause contributes a single
      // row, so two arms of it count max(1, d1 * d2), not the max(1, d1) * max(1, d2) of two independent optional arms
      if (isOptional && armList.size() - armsBefore > 1)
        return null;
    }

    if (centralVar == null || centralLabel == null || armList.isEmpty())
      return null;

    final VertexPredicate centralPredicate = centralWithProperties != null ? predicates.predicateFor(centralWithProperties)
        : predicates.whereFor(centralVar);
    return new DegreeProductOp(centralLabel, centralPredicate, armList.toArray(new DegreeProductOp.Arm[0]));
  }

  /**
   * Detects the Q3 "triangle in country" pattern:
   * <pre>
   *   MATCH (co:Anchor)
   *   MATCH (p1:Node)-[:CHAIN1]->(:Mid)-[:CHAIN2]->(co)
   *   MATCH (p2:Node)-[:CHAIN1]->(:Mid)-[:CHAIN2]->(co)
   *   MATCH (p3:Node)-[:CHAIN1]->(:Mid)-[:CHAIN2]->(co)
   *   MATCH (p1)-[:TRI]-(p2)-[:TRI]-(p3)-[:TRI]-(p1)
   *   RETURN count(*) AS count
   * </pre>
   * Requires: 5+ MATCH clauses, no WHERE, RETURN count(*), one cycle MATCH, three partition MATCHes.
   */
  CountOp tryDetectTriangleCountStar() {
    if (statement.getMatchClauses() == null || statement.getMatchClauses().size() < 4)
      return null;
    if (statement.getWhereClause() != null)
      return null;

    // No MATCH clause should have WHERE
    for (final MatchClause mc : statement.getMatchClauses())
      if (mc.hasWhereClause() || mc.isOptional())
        return null;

    // Find the cycle MATCH: a path pattern where first and last node share the same variable
    // e.g., (p1)-[:KNOWS]-(p2)-[:KNOWS]-(p3)-[:KNOWS]-(p1)
    MatchClause cycleMC = null;
    PathPattern cyclePP = null;
    String cycleEdgeType = null;
    final ArrayList<String> cycleVars = new ArrayList<>();
    for (final MatchClause mc : statement.getMatchClauses()) {
      if (!mc.hasPathPatterns() || mc.getPathPatterns().size() != 1)
        continue;
      final PathPattern pp = mc.getPathPatterns().get(0);
      if (pp.getRelationshipCount() < 3)
        continue;
      final String firstVar = pp.getFirstNode().getVariable();
      final String lastVar = pp.getLastNode().getVariable();
      if (firstVar != null && firstVar.equals(lastVar) && pp.getRelationshipCount() == 3) {
        // Check all relationships use the same edge type and are anonymous
        boolean valid = true;
        String edgeType = null;
        for (int i = 0; i < 3; i++) {
          final RelationshipPattern rel = pp.getRelationship(i);
          // the operator intersects the undirected adjacency, so only an undirected hop is its pattern: a written
          // direction is a constraint it cannot check (#9387)
          if (rel.isVariableLength() || (rel.getVariable() != null && !rel.getVariable().isEmpty())
              || !rel.hasTypes() || rel.getTypes().size() != 1 || rel.hasProperties() || rel.hasWhereExpression() || rel.getDirection() != Direction.BOTH) {
            valid = false;
            break;
          }
          if (edgeType == null)
            edgeType = rel.getTypes().get(0);
          else if (!edgeType.equals(rel.getTypes().get(0))) {
            valid = false;
            break;
          }
        }
        if (valid) {
          cycleMC = mc;
          cyclePP = pp;
          cycleEdgeType = edgeType;
          // Collect the 3 distinct variables (first=last, so 3 unique vars)
          for (int i = 0; i < 3; i++) {
            final String nv = pp.getNode(i).getVariable();
            if (nv != null && !cycleVars.contains(nv))
              cycleVars.add(nv);
          }
          break;
        }
      }
    }
    if (cycleMC == null || cycleVars.size() != 3)
      return null;

    // The operator filters every node it visits by ONE label per position, so a label it cannot stand for (a conjunction,
    // a disjunction) declines the push-down, and so does one variable carrying two different labels (#9388). The label of
    // a cycle variable is the one written wherever the variable appears: in the cycle and at the head of its chain.
    final HashMap<String, String> cycleLabels = new HashMap<>();
    for (int i = 0; i < 3; i++) {
      final NodePattern node = cyclePP.getNode(i);
      if (!hasPushDownRepresentableLabel(node) || node.hasProperties() || node.hasDynamicLabels())
        return null;
      if (node.hasLabels() && !mergeVariableLabel(cycleLabels, node.getVariable(), node.getLabels().get(0)))
        return null;
    }

    // Find the anchor MATCH: single node pattern (e.g., (co:Country))
    String anchorVar = null;
    String anchorLabel = null;
    for (final MatchClause mc : statement.getMatchClauses()) {
      if (mc == cycleMC)
        continue;
      if (!mc.hasPathPatterns() || mc.getPathPatterns().size() != 1)
        continue;
      final PathPattern pp = mc.getPathPatterns().get(0);
      if (pp.isSingleNode() && pp.getFirstNode().getVariable() != null) {
        final NodePattern anchor = pp.getFirstNode();
        if (!hasPushDownRepresentableLabel(anchor))
          return null;
        anchorVar = anchor.getVariable();
        anchorLabel = anchor.hasLabels() ? anchor.getLabels().get(0) : null;
        break;
      }
    }

    // Find partition chain MATCHes: each cycle var linked to the anchor via a chain
    // e.g., (p1:Person)-[:IS_LOCATED_IN]->(:City)-[:IS_PART_OF]->(co)
    String[] partitionEdgeTypes = null;
    Vertex.DIRECTION[] partitionDirections = null;
    String[] chainLabels = null;
    final HashSet<String> chainHeads = new HashSet<>();
    final HashSet<String> chainInteriorVars = new HashSet<>();
    int chainMatchCount = 0;
    for (final MatchClause mc : statement.getMatchClauses()) {
      if (mc == cycleMC)
        continue;
      if (!mc.hasPathPatterns() || mc.getPathPatterns().size() != 1)
        continue;
      final PathPattern pp = mc.getPathPatterns().get(0);
      if (pp.isSingleNode())
        continue; // anchor match

      // Check: first node is a cycle var, last node is anchor var
      final String firstVar = pp.getFirstNode().getVariable();
      final String lastVar = pp.getLastNode().getVariable();
      if (firstVar == null || lastVar == null)
        continue;
      if (!cycleVars.contains(firstVar) || !lastVar.equals(anchorVar))
        continue;

      // Extract chain edge types and directions
      final int hops = pp.getRelationshipCount();
      final String[] chainET = new String[hops];
      final Vertex.DIRECTION[] chainDir = new Vertex.DIRECTION[hops];
      boolean valid = true;
      for (int i = 0; i < hops; i++) {
        final RelationshipPattern rel = pp.getRelationship(i);
        if (rel.isVariableLength() || !rel.hasTypes() || rel.getTypes().size() != 1 || rel.hasProperties()) {
          valid = false;
          break;
        }
        chainET[i] = rel.getTypes().get(0);
        final Direction d = rel.getDirection();
        chainDir[i] = d == Direction.OUT ? Vertex.DIRECTION.OUT
            : d == Direction.IN ? Vertex.DIRECTION.IN : Vertex.DIRECTION.BOTH;
      }
      if (!valid)
        continue;

      // One label per position of the chain, and the chain's end is the anchor, which carries its own label (#9388). An
      // interior node variable shared between two chains would be a join the operator does not make, so it declines.
      final String[] labels = new String[hops + 1];
      for (int i = 0; i <= hops; i++) {
        final NodePattern node = pp.getNode(i);
        if (!hasPushDownRepresentableLabel(node) || node.hasProperties() || node.hasDynamicLabels())
          return null;
        labels[i] = node.hasLabels() ? node.getLabels().get(0) : null;
        if (i > 0 && i < hops && node.getVariable() != null && !chainInteriorVars.add(node.getVariable()))
          return null;
      }
      if (labelsConflict(labels[hops], anchorLabel))
        return null;
      if (labels[hops] == null)
        labels[hops] = anchorLabel;
      if (!chainHeads.add(firstVar))
        return null; // two chains for one cycle variable: the third variable has none
      if (labels[0] != null && !mergeVariableLabel(cycleLabels, firstVar, labels[0]))
        return null;

      // All chain MATCHes must have the same chain structure
      if (partitionEdgeTypes == null) {
        partitionEdgeTypes = chainET;
        partitionDirections = chainDir;
        chainLabels = labels;
      } else {
        if (chainET.length != partitionEdgeTypes.length)
          return null;
        for (int i = 0; i < chainET.length; i++)
          if (!chainET[i].equals(partitionEdgeTypes[i]) || chainDir[i] != partitionDirections[i])
            return null;
        // the interior and end labels must agree too: the operator filters on one set per position. Position 0 is the
        // person label, settled below from the variables themselves.
        for (int i = 1; i <= hops; i++)
          if (!Objects.equals(labels[i], chainLabels[i]))
            return null;
      }
      chainMatchCount++;
    }

    // Must have exactly 3 chain MATCHes (one per cycle variable)
    if (chainMatchCount != 3 || partitionEdgeTypes == null)
      return null;

    // The three cycle variables are interchangeable in the operator, which has one label for all of them, so a label on
    // one variable and none on another (or two different ones) is a filter it cannot express.
    String personLabel = null;
    boolean first = true;
    for (final String var : cycleVars) {
      final String label = cycleLabels.get(var);
      if (first)
        personLabel = label;
      else if (!Objects.equals(personLabel, label))
        return null;
      first = false;
    }
    chainLabels[0] = personLabel;

    return new PartitionedTriangleOp(partitionEdgeTypes, partitionDirections, cycleEdgeType, chainLabels);
  }

  /**
   * Detects a triangle written as one MATCH, with no WHERE or with one that orders the three variables strictly on one
   * property (issue #10255):
   * <pre>
   *   MATCH (a:Person)-[:KNOWS]-(b:Person)-[:KNOWS]-(c:Person)-[:KNOWS]-(a) WHERE a.id &lt; b.id AND b.id &lt; c.id
   *   RETURN count(*)
   * </pre>
   * The three hops are undirected, anonymous and of one type, and the three variables carry one label or none, so the
   * pattern is symmetric in its variables and {@link TriangleCountOp} can count each triangle once and weigh it by the
   * bindings it stands for. The WHERE is taken only when it is nothing but {@code <} and {@code >} comparisons of one
   * property between two of the variables, joined by AND, that order all three in exactly one way.
   *
   * @return the operator, or null when the statement is not that shape
   */
  CountOp tryDetectSingleTriangleCountStar() {
    if (statement.getMatchClauses() == null || statement.getMatchClauses().size() != 1)
      return null;
    final MatchClause matchClause = statement.getMatchClauses().getFirst();
    if (matchClause.isOptional() || !matchClause.hasPathPatterns() || matchClause.getPathPatterns().size() != 1)
      return null;
    // the parser writes a MATCH's WHERE on the clause: a statement carrying one as well would have one of them ignored
    if (matchClause.hasWhereClause() && statement.getWhereClause() != null)
      return null;
    final PathPattern pathPattern = matchClause.getPathPatterns().getFirst();
    if (pathPattern.getRelationshipCount() != 3 || pathPattern.hasPathVariable())
      return null;

    // (a)-(b)-(c)-(a): the walk closes on its first variable, and the two in between are two other vertices
    final String[] variables = new String[3];
    for (int i = 0; i < 3; i++)
      variables[i] = pathPattern.getNode(i).getVariable();
    final String closing = pathPattern.getNode(3).getVariable();
    if (variables[0] == null || variables[0].isEmpty() || !variables[0].equals(closing))
      return null;
    for (int i = 1; i < 3; i++)
      if (variables[i] != null && !variables[i].isEmpty() && (variables[i].equals(variables[0])
          || (i == 2 && variables[i].equals(variables[1]))))
        return null;

    String edgeType = null;
    for (int i = 0; i < 3; i++) {
      final RelationshipPattern rel = pathPattern.getRelationship(i);
      // the operator intersects the undirected adjacency, so only an undirected hop is its pattern (#9387)
      if (rel.isVariableLength() || (rel.getVariable() != null && !rel.getVariable().isEmpty()) || !rel.hasTypes()
          || rel.getTypes().size() != 1 || rel.hasProperties() || rel.hasWhereExpression() || rel.getDirection() != Direction.BOTH)
        return null;
      if (edgeType == null)
        edgeType = rel.getTypes().getFirst();
      else if (!edgeType.equals(rel.getTypes().getFirst()))
        return null;
    }

    // One label for the three positions, or none: the operator binds any vertex of a triangle to any variable
    String label = null;
    boolean labelled = false;
    for (int i = 0; i <= 3; i++) {
      final NodePattern node = pathPattern.getNode(i);
      if (!hasPushDownRepresentableLabel(node) || node.hasProperties() || node.hasDynamicLabels())
        return null;
      if (!node.hasLabels())
        continue;
      if (labelled && !label.equals(node.getLabels().getFirst()))
        return null;
      label = node.getLabels().getFirst();
      labelled = true;
    }
    // the closing node repeats the first, so its label counts for that variable: every other position must carry it too
    if (labelled)
      for (int i = 0; i < 3; i++)
        if (!pathPattern.getNode(i).hasLabels() && !(i == 0 && pathPattern.getNode(3).hasLabels()))
          return null;

    final WhereClause whereClause = matchClause.hasWhereClause() ? matchClause.getWhereClause() : statement.getWhereClause();
    if (whereClause == null)
      return new TriangleCountOp(edgeType, label);
    if (whereClause.getConditionExpression() == null || variables[1] == null || variables[2] == null)
      return null;

    final List<ComparisonExpression> comparisons = new ArrayList<>();
    if (!collectConjuncts(whereClause.getConditionExpression(), comparisons))
      return null;
    final int count = comparisons.size();
    final int[] left = new int[count];
    final int[] right = new int[count];
    // less[x][y]: the WHERE asks x.p < y.p
    final boolean[][] less = new boolean[3][3];
    String property = null;
    for (int c = 0; c < count; c++) {
      final ComparisonExpression comparison = comparisons.get(c);
      if (!(comparison.getLeft() instanceof PropertyAccessExpression l) || !(comparison.getRight() instanceof PropertyAccessExpression r))
        return null;
      if (property == null)
        property = l.getPropertyName();
      if (!property.equals(l.getPropertyName()) || !property.equals(r.getPropertyName()))
        return null;
      left[c] = indexOf(variables, l.getVariableName());
      right[c] = indexOf(variables, r.getVariableName());
      if (left[c] < 0 || right[c] < 0 || left[c] == right[c])
        return null;
      switch (comparison.getOperator()) {
      case LESS_THAN -> less[left[c]][right[c]] = true;
      case GREATER_THAN -> less[right[c]][left[c]] = true;
      default -> {
        return null;
      }
      }
    }

    // exactly one order of the three variables, x < y < z, has to agree with every comparison and be implied by them
    int[] order = null;
    for (final int[] candidate : new int[][] { { 0, 1, 2 }, { 0, 2, 1 }, { 1, 0, 2 }, { 1, 2, 0 }, { 2, 0, 1 }, { 2, 1, 0 } }) {
      if (!less[candidate[0]][candidate[1]] || !less[candidate[1]][candidate[2]])
        continue;
      if (less[candidate[1]][candidate[0]] || less[candidate[2]][candidate[1]] || less[candidate[2]][candidate[0]])
        continue;
      if (order != null)
        return null;
      order = candidate;
    }
    if (order == null)
      return null;

    return new TriangleCountOp(edgeType, label, property,
        new String[] { variables[order[0]], variables[order[1]], variables[order[2]] },
        comparisons.toArray(new ComparisonExpression[0]), left, right);
  }

  /** Collects the comparisons of a condition that is nothing but comparisons joined by AND; false when it is anything else. */
  private static boolean collectConjuncts(final BooleanExpression condition, final List<ComparisonExpression> comparisons) {
    if (condition instanceof LogicalExpression logical)
      return logical.getOperator() == LogicalExpression.Operator.AND && logical.getRight() != null
          && collectConjuncts(logical.getLeft(), comparisons) && collectConjuncts(logical.getRight(), comparisons);
    if (!(condition instanceof ComparisonExpression comparison))
      return false;
    comparisons.add(comparison);
    return true;
  }

  private static int indexOf(final String[] variables, final String variable) {
    for (int i = 0; i < variables.length; i++)
      if (variables[i] != null && variables[i].equals(variable))
        return i;
    return -1;
  }

  /**
   * Detects two comma-separated path patterns sharing two endpoint variables + count(*).
   * One pattern is a single-hop "probe" edge, the other is a multi-hop "build" chain.
   * <pre>
   *   MATCH (p1:Person)-[:KNOWS]-(p2:Person),
   *         (p1)<-[:HAS_CREATOR]-(c:Comment)-[:REPLY_OF]->(po:Post)-[:HAS_CREATOR]->(p2)
   *   RETURN count(*) AS count
   * </pre>
   */
  CountOp tryDetectPairJoinCountStar(final CommandContext context) {
    // Exactly one non-optional MATCH, of two path patterns or more
    if (statement.getMatchClauses() == null || statement.getMatchClauses().size() != 1)
      return null;
    final MatchClause matchClause = statement.getMatchClauses().get(0);
    if (matchClause.isOptional())
      return null;
    if (!matchClause.hasPathPatterns() || matchClause.getPathPatterns().size() < 2)
      return null;

    // WHERE: only conjuncts that read one node of the pattern and nothing else, which the operator applies per vertex like
    // an inline property map (issue #9608)
    final CountPushDownPredicates predicates = new CountPushDownPredicates(context);
    // the parser writes a MATCH's WHERE on the clause: a statement carrying one as well would have one of them ignored
    if (matchClause.hasWhereClause() && statement.getWhereClause() != null)
      return null;
    final WhereClause whereClause = matchClause.hasWhereClause() ? matchClause.getWhereClause() : statement.getWhereClause();
    if (whereClause != null) {
      if (whereClause.getConditionExpression() == null)
        return null;
      final Set<String> nodeVariables = new HashSet<>();
      for (final PathPattern pattern : matchClause.getPathPatterns())
        nodeVariables.addAll(nodeVariablesOf(pattern));
      for (final BooleanExpression conjunct : CountPushDownPredicates.conjuncts(whereClause.getConditionExpression()))
        if (!predicates.addWhereConjunct(conjunct, nodeVariables))
          return null;
    }

    // A MATCH is a pattern graph, and the order and cuts it is written in are no plan (issue #9599): LSQB Q2 written from
    // the post, as four one-hop patterns, is the same cycle as a KNOWS hop and the three-hop chain closing it. Every way the
    // cycle splits into a probe hop and that chain is a candidate, and the statistics pick the one whose build phase makes
    // the fewest pairs, so every spelling of the cycle gets the same operator
    final PatternGraph graph = PatternGraph.of(matchClause.getPathPatterns());
    final List<PathPattern[]> splits = graph != null ? graph.cycleSplits() : List.of();
    if (splits.isEmpty()) {
      if (matchClause.getPathPatterns().size() != 2
          || plan.hopEdges.clauseHopsMayShareAnEdge(new MatchClause(HopEdgeTrackingAnalyzer.withWhereEqualities(matchClause.getPathPatterns(), whereClause), false)))
        return null;
      return pairJoinOf(matchClause.getPathPatterns().get(0), matchClause.getPathPatterns().get(1), predicates);
    }

    // The join counts adjacency paths, so it cannot stand in for the one-relationship-per-hop rule (issue #9485). Every split
    // holds the same hops between the same nodes, each written with all of its variable's labels and properties, so one
    // answers for all
    if (plan.hopEdges.clauseHopsMayShareAnEdge(new MatchClause(HopEdgeTrackingAnalyzer.withWhereEqualities(List.of(splits.get(0)), whereClause), false)))
      return null;

    PairHashJoinOp best = null;
    double bestPairs = 0;
    String bestDescription = null;
    final PairJoinStatistics statistics = new PairJoinStatistics(database);
    for (final PathPattern[] split : splits) {
      final PairHashJoinOp op = pairJoinOf(split[0], split[1], predicates);
      if (op == null)
        continue;
      final double pairs = op.estimatedBuildPairs(statistics::vertices, statistics::fanOut);
      // a tie is broken on the operator itself, so it does not fall to the order the cycle was written in. Its description
      // names every type, direction and label of both arms and the probe, so two different splits never describe alike
      final String description = op.describe(0, 0);
      if (best == null || pairs < bestPairs || (pairs == bestPairs && description.compareTo(bestDescription) < 0)) {
        best = op;
        bestPairs = pairs;
        bestDescription = description;
      }
    }
    return best;
  }

  /**
   * The statistics a pair join's cost is estimated from, each read once per planning: the vertices of a label, from its
   * counter, and the fan-out of a hop from the label it leaves.
   * <p>
   * The fan-out is measured on a sample of the label's vertices rather than derived from the edge type's size: a type's
   * edges leave several labels, and LSQB's HAS_CREATOR divided by the posts makes a post look like it has two creators
   * because the comments have one each too. Reading the edge lists of a few vertices is the price of telling the splits of a
   * cycle apart by what they cost, and it is bounded by the sample. A sample is an estimate: on a skewed label (a few
   * supernodes among many small vertices) it can pick the costlier split, which changes the time and never the count. The
   * count push-downs are not part of the cached plan, so it is taken again at every execution and follows the graph. An
   * unlabelled position, which has no set to sample from, falls back to the type's edges over every vertex, off a ready
   * provider when one holds the type (it counts light edges, which keep no record) and off the type's record counter
   * otherwise.
   */
  private static final class PairJoinStatistics {
    private static final int SAMPLE = 32;

    private final Database            database;
    private final Map<String, Long>   vertices = new HashMap<>();
    private final Map<String, Double> fanOuts  = new HashMap<>();

    PairJoinStatistics(final Database database) {
      this.database = database;
    }

    long vertices(final String label) {
      return vertices.computeIfAbsent(label == null ? "" : label, key -> {
        final Schema schema = database.getSchema();
        if (!key.isEmpty())
          return schema.getTypeOrNull(key) instanceof VertexType ? database.countType(key, true) : 0L;
        long total = 0;
        for (final DocumentType type : schema.getTypes())
          if (type instanceof VertexType)
            total += database.countType(type.getName(), false);
        return total;
      });
    }

    double fanOut(final String fromLabel, final String edgeType, final Vertex.DIRECTION direction) {
      return fanOuts.computeIfAbsent(fromLabel + '\u0000' + edgeType + '\u0000' + direction, key -> {
        if (fromLabel != null && database.getSchema().getTypeOrNull(fromLabel) instanceof VertexType type) {
          // spread over the label's buckets, sub-types' included, rather than the first records of the first one: those are
          // the oldest, and in a graph that grew they are often the best connected
          final List<Bucket> buckets = type.getBuckets(true);
          final int perBucket = Math.max(1, (SAMPLE + buckets.size() - 1) / Math.max(1, buckets.size()));
          long sampled = 0;
          long degrees = 0;
          for (int b = 0; b < buckets.size() && sampled < SAMPLE; b++) {
            int fromBucket = 0;
            for (final Iterator<Record> it = database.iterateBucket(buckets.get(b).getName());
                 it.hasNext() && fromBucket < perBucket && sampled < SAMPLE; ++fromBucket, ++sampled)
              degrees += it.next().asVertex().countEdges(direction, edgeType);
          }
          return sampled == 0 ? 0D : (double) degrees / sampled;
        }
        final long sources = vertices(null);
        final double perVertex = sources == 0 ? 0D : (double) edges(edgeType) / sources;
        return direction == Vertex.DIRECTION.BOTH ? 2 * perVertex : perVertex;
      });
    }

    private long edges(final String type) {
      final GraphTraversalProvider provider = GraphTraversalProviderRegistry.findProviderWithoutWaiting(database, type);
      final long total = provider != null ? provider.countAllEdges(type) : -1;
      if (total >= 0)
        return total;
      return database.getSchema().getTypeOrNull(type) instanceof EdgeType ? database.countType(type, true) : 0L;
    }
  }

  /**
   * The pair join of two path patterns, one a single-hop probe and the other the multi-hop build chain between the probe's
   * ends, or null when they are not that shape.
   *
   * @param predicates the {@code WHERE} conjuncts recorded per node variable, applied with the inline property maps
   */
  private PairHashJoinOp pairJoinOf(final PathPattern pp0, final PathPattern pp1, final CountPushDownPredicates predicates) {

    PathPattern probePattern, buildPattern;
    if (pp0.getRelationshipCount() == 1 && pp1.getRelationshipCount() >= 2) {
      probePattern = pp0;
      buildPattern = pp1;
    } else if (pp1.getRelationshipCount() == 1 && pp0.getRelationshipCount() >= 2) {
      probePattern = pp1;
      buildPattern = pp0;
    } else
      return null; // Neither is single-hop

    // Probe pattern: must be a single anonymous relationship with one edge type
    final RelationshipPattern probeRel = probePattern.getRelationship(0);
    if (probeRel.isVariableLength() || !probeRel.hasTypes() || probeRel.getTypes().size() != 1)
      return null;
    if (probeRel.getVariable() != null && !probeRel.getVariable().isEmpty())
      return null;
    final String probeEdgeType = probeRel.getTypes().get(0);
    final Direction probeDir = probeRel.getDirection();
    final Vertex.DIRECTION probeDirection = probeDir == Direction.OUT ? Vertex.DIRECTION.OUT
        : probeDir == Direction.IN ? Vertex.DIRECTION.IN : Vertex.DIRECTION.BOTH;

    // Get the two shared endpoint variables from probe pattern
    final NodePattern probeNode1 = probePattern.getFirstNode();
    final NodePattern probeNode2 = probePattern.getLastNode();
    final String probeVar1 = probeNode1.getVariable();
    final String probeVar2 = probeNode2.getVariable();
    if (probeVar1 == null || probeVar2 == null)
      return null;
    if (!hasPushDownRepresentableLabel(probeNode1) || !hasPushDownRepresentableLabel(probeNode2))
      return null;

    // The two patterns name the same two variables, so a label written on the probe side constrains the arm
    // that ends there just as one written on the build side does. Reading only the build side dropped it
    // silently, on the CSR path and the OLTP one alike (issue #6304).
    final String probeLabel1 = probeNode1.hasLabels() ? probeNode1.getLabels().get(0) : null;
    final String probeLabel2 = probeNode2.hasLabels() ? probeNode2.getLabels().get(0) : null;

    // Build pattern: extract chain and find which hops reach the shared endpoints
    final int buildHops = buildPattern.getRelationshipCount();

    // Every node's inline property map must be one a per-vertex filter can apply (issue #9608), and a node the build chain
    // names twice is one vertex, a constraint the arms cannot express
    for (final PathPattern pattern : new PathPattern[] { probePattern, buildPattern })
      for (final NodePattern node : pattern.getNodes())
        if (!CountPushDownPredicates.inlinePropertiesArePerVertex(node))
          return null;
    if (CypherExecutionPlan.repeatsNodeVariable(buildPattern))
      return null;

    // Find the build chain's start node (a non-shared variable, e.g., "c" in Q2)
    // The shared variables (probeVar1, probeVar2) should appear as targets of hops
    int startNodeIdx = -1;
    for (int i = 0; i <= buildHops; i++) {
      final String nv = buildPattern.getNode(i).getVariable();
      if (nv != null && !nv.equals(probeVar1) && !nv.equals(probeVar2)) {
        startNodeIdx = i;
        break;
      }
    }
    // If no non-shared named variable found, try anonymous nodes
    if (startNodeIdx < 0) {
      for (int i = 0; i <= buildHops; i++) {
        final String nv = buildPattern.getNode(i).getVariable();
        if (nv == null || nv.isEmpty()) {
          startNodeIdx = i;
          break;
        }
      }
    }
    if (startNodeIdx < 0)
      return null;

    // Determine the build chain start label
    final NodePattern startNode = buildPattern.getNode(startNodeIdx);
    if (!hasPushDownRepresentableLabel(startNode))
      return null;
    final String buildStartLabel = startNode.hasLabels() ? startNode.getLabels().get(0) : null;
    if (buildStartLabel == null)
      return null;

    // Build two arms from startNodeIdx: backward (toward position 0) and forward (toward end).
    // Each arm reaches one of the shared endpoint variables.

    // Walk backward from startNodeIdx to find endpoint reaching probeVar1 or probeVar2
    final ArrayList<String> bwdET = new ArrayList<>();
    final ArrayList<Vertex.DIRECTION> bwdDir = new ArrayList<>();
    final ArrayList<String> bwdLabels = new ArrayList<>();
    final ArrayList<NodePattern> bwdNodes = new ArrayList<>();
    String bwdEndpointVar = null;
    int bwdEndpointIdx = -1;
    for (int i = startNodeIdx - 1; i >= 0; i--) {
      final RelationshipPattern rel = buildPattern.getRelationship(i);
      if (rel.isVariableLength() || !rel.hasTypes() || rel.getTypes().size() != 1
          || (rel.getVariable() != null && !rel.getVariable().isEmpty()))
        return null;
      bwdET.add(rel.getTypes().get(0));
      final Direction d = rel.getDirection().reverse();
      bwdDir.add(d == Direction.OUT ? Vertex.DIRECTION.OUT
          : d == Direction.IN ? Vertex.DIRECTION.IN : Vertex.DIRECTION.BOTH);
      final NodePattern targetNode = buildPattern.getNode(i);
      if (!hasPushDownRepresentableLabel(targetNode))
        return null;
      bwdLabels.add(targetNode.hasLabels() ? targetNode.getLabels().get(0) : null);
      bwdNodes.add(targetNode);
      final String nodeVar = targetNode.getVariable();
      if (nodeVar != null && (nodeVar.equals(probeVar1) || nodeVar.equals(probeVar2))) {
        bwdEndpointVar = nodeVar;
        bwdEndpointIdx = i;
        break;
      }
    }

    // Walk forward from startNodeIdx to find the other endpoint
    final ArrayList<String> fwdET = new ArrayList<>();
    final ArrayList<Vertex.DIRECTION> fwdDir = new ArrayList<>();
    final ArrayList<String> fwdLabels = new ArrayList<>();
    final ArrayList<NodePattern> fwdNodes = new ArrayList<>();
    String fwdEndpointVar = null;
    int fwdEndpointIdx = -1;
    for (int i = startNodeIdx; i < buildHops; i++) {
      final RelationshipPattern rel = buildPattern.getRelationship(i);
      if (rel.isVariableLength() || !rel.hasTypes() || rel.getTypes().size() != 1
          || (rel.getVariable() != null && !rel.getVariable().isEmpty()))
        return null;
      fwdET.add(rel.getTypes().get(0));
      final Direction d = rel.getDirection();
      fwdDir.add(d == Direction.OUT ? Vertex.DIRECTION.OUT
          : d == Direction.IN ? Vertex.DIRECTION.IN : Vertex.DIRECTION.BOTH);
      final NodePattern targetNode = buildPattern.getNode(i + 1);
      if (!hasPushDownRepresentableLabel(targetNode))
        return null;
      fwdLabels.add(targetNode.hasLabels() ? targetNode.getLabels().get(0) : null);
      fwdNodes.add(targetNode);
      final String nodeVar = targetNode.getVariable();
      if (nodeVar != null && (nodeVar.equals(probeVar1) || nodeVar.equals(probeVar2))) {
        fwdEndpointVar = nodeVar;
        fwdEndpointIdx = i + 1;
        break;
      }
    }

    if (bwdEndpointVar == null || fwdEndpointVar == null)
      return null;
    if (bwdEndpointVar.equals(fwdEndpointVar))
      return null; // Both arms reach the same endpoint
    // The arms are the whole build chain: a hop written past a shared endpoint would be left out of the count
    if (bwdEndpointIdx != 0 || fwdEndpointIdx != buildHops)
      return null;

    // The property predicate of every position. A shared endpoint holds what both patterns wrote on it, which the pattern
    // graph has merged already for a split it made, and is merged here for the patterns as written
    final VertexPredicate startPredicate = predicates.predicateFor(startNode);
    final VertexPredicate[] bwdPredicates = armPredicates(bwdNodes, probeNode1, probeNode2, predicates);
    final VertexPredicate[] fwdPredicates = armPredicates(fwdNodes, probeNode1, probeNode2, predicates);
    if (bwdPredicates == null || fwdPredicates == null)
      return null;

    // Arm reaching probeVar1 and arm reaching probeVar2
    final String[] arm1ET, arm2ET, arm1Labels, arm2Labels;
    final Vertex.DIRECTION[] arm1Dir, arm2Dir;
    final VertexPredicate[] arm1Predicates, arm2Predicates;
    if (bwdEndpointVar.equals(probeVar1)) {
      arm1ET = bwdET.toArray(new String[0]);
      arm1Dir = bwdDir.toArray(new Vertex.DIRECTION[0]);
      arm1Labels = bwdLabels.toArray(new String[0]);
      arm1Predicates = bwdPredicates;
      arm2ET = fwdET.toArray(new String[0]);
      arm2Dir = fwdDir.toArray(new Vertex.DIRECTION[0]);
      arm2Labels = fwdLabels.toArray(new String[0]);
      arm2Predicates = fwdPredicates;
    } else {
      arm1ET = fwdET.toArray(new String[0]);
      arm1Dir = fwdDir.toArray(new Vertex.DIRECTION[0]);
      arm1Labels = fwdLabels.toArray(new String[0]);
      arm1Predicates = fwdPredicates;
      arm2ET = bwdET.toArray(new String[0]);
      arm2Dir = bwdDir.toArray(new Vertex.DIRECTION[0]);
      arm2Labels = bwdLabels.toArray(new String[0]);
      arm2Predicates = bwdPredicates;
    }

    // Each arm's last hop lands on a shared variable, which is the one the probe pattern may also have
    // labelled. Both labels are in force, and the operator carries one per hop: when the two disagree the
    // filter is their intersection, which one name cannot express, so the push-down declines and the ordinary
    // pipeline - which evaluates both - answers instead.
    if (labelsConflict(arm1Labels[arm1Labels.length - 1], probeLabel1)
        || labelsConflict(arm2Labels[arm2Labels.length - 1], probeLabel2))
      return null;
    if (probeLabel1 != null)
      arm1Labels[arm1Labels.length - 1] = probeLabel1;
    if (probeLabel2 != null)
      arm2Labels[arm2Labels.length - 1] = probeLabel2;

    return new PairHashJoinOp(buildStartLabel, startPredicate, arm1ET, arm1Dir, arm1Labels, arm1Predicates,
        arm2ET, arm2Dir, arm2Labels, arm2Predicates, probeEdgeType, probeDirection);
  }

  /**
   * The predicate of each node an arm reaches, its last one being the shared endpoint with what the probe pattern wrote on
   * it; null when the two writings of that endpoint cannot be said as one node.
   */
  private static VertexPredicate[] armPredicates(final List<NodePattern> reached, final NodePattern probeNode1,
      final NodePattern probeNode2, final CountPushDownPredicates predicates) {
    final VertexPredicate[] result = new VertexPredicate[reached.size()];
    for (int i = 0; i < result.length; i++) {
      final NodePattern node = i == result.length - 1 ? withProbeSide(reached.get(i), probeNode1, probeNode2) : reached.get(i);
      if (node == null)
        return null;
      result[i] = predicates.predicateFor(node);
    }
    return result;
  }

  /**
   * The build chain's node of a shared endpoint with what the probe pattern wrote on the same variable, or null when the two
   * cannot be said as one node (see {@link PatternGraph#merged}).
   */
  private static NodePattern withProbeSide(final NodePattern buildNode, final NodePattern probeNode1, final NodePattern probeNode2) {
    final NodePattern probeNode = buildNode.getVariable().equals(probeNode1.getVariable()) ? probeNode1 : probeNode2;
    return PatternGraph.merged(buildNode, probeNode);
  }

  /**
   * Whether a count push-down can express this node's labels as the single name it keys a bucket set on.
   * <p>
   * {@code (a:A:B)} keeps only what carries both and {@code (a:A|B)} keeps what carries either; taking the first
   * of the list turns each of them into {@code (a:A)}, which is neither. An operator built on that filter counts
   * a set the pattern did not describe, so the detector declines and leaves the query to the ordinary pipeline
   * (issue #6304, extended to the remaining detectors by issue #6322).
   * <p>
   * The one place a multi-label node is <em>not</em> answered by declining is
   * {@link HopEdgeTrackingAnalyzer#nodeLabelsAreTypeDisjoint(NodePattern, NodePattern)}, which produces a proof rather than a filter
   * and takes the two spellings apart instead.
   */
  static boolean hasPushDownRepresentableLabel(final NodePattern node) {
    return !node.hasLabels() || (node.getLabels().size() == 1 && !node.isLabelDisjunction());
  }

  /**
   * Whether an endpoint of an OPTIONAL MATCH hop filters nothing an edge count with at most one label cannot apply: no
   * dynamic label, no inline WHERE and at most one static label (issue #11583).
   */
  static boolean isCountableEndpoint(final NodePattern node) {
    return !node.hasDynamicLabels() && !node.hasWhereExpression() && hasPushDownRepresentableLabel(node);
  }

  /** Records the label a variable carries; false when it already carries a different one, which no single label can stand for. */
  private static boolean mergeVariableLabel(final Map<String, String> labels, final String variable, final String label) {
    if (variable == null)
      return true;
    final String previous = labels.putIfAbsent(variable, label);
    return previous == null || previous.equals(label);
  }

  /** Whether two labels written on the same variable name different types, which no single label can stand for. */
  private static boolean labelsConflict(final String a, final String b) {
    return a != null && b != null && !a.equals(b);
  }

  /**
   * Detects a chain pattern with a negative path predicate (anti-join) in WHERE.
   * <p>
   * Handles patterns like:
   * <pre>
   *   MATCH (p1:Person)-[:KNOWS]-(p2:Person)-[:KNOWS]-(p3:Person)-[:HAS_INTEREST]->(t:Tag)
   *   WHERE NOT (p1)-[:KNOWS]-(p3) AND p1 <> p3
   *   RETURN count(*) AS count
   * </pre>
   * The WHERE clause must contain a negated single-hop pattern predicate between two chain nodes,
   * optionally combined with a simple inequality via AND.
   */
  CountOp tryDetectAntiJoinChainCountStar(final CommandContext context) {
    // Exactly one MATCH clause
    if (statement.getMatchClauses() == null || statement.getMatchClauses().size() != 1)
      return null;
    return tryDetectAntiJoinChainCountStar(context, statement.getMatchClauses().get(0));
  }

  /** The anti-join chain over one MATCH clause: the statement's own, or its patterns joined into one chain (issue #9599). */
  private CountOp tryDetectAntiJoinChainCountStar(final CommandContext context, final MatchClause matchClause) {
    if (matchClause.isOptional())
      return null;

    // Must have a WHERE clause with an anti-join pattern
    // the parser writes a MATCH's WHERE on the clause: a statement carrying one as well would have one of them ignored
    if (matchClause.hasWhereClause() && statement.getWhereClause() != null)
      return null;
    final WhereClause whereClause = matchClause.hasWhereClause() ? matchClause.getWhereClause() : statement.getWhereClause();
    if (whereClause == null || whereClause.getConditionExpression() == null)
      return null;

    // Exactly one path pattern with at least one relationship
    if (!matchClause.hasPathPatterns() || matchClause.getPathPatterns().size() != 1)
      return null;
    final PathPattern pathPattern = matchClause.getPathPatterns().get(0);
    // The shape is checked before the WHERE is parsed, which costs far more and is wasted on a pattern too short for any
    // anti-join (issue #9652)
    if (pathPattern.getRelationshipCount() < 2) // need at least 2 hops for anti-join to make sense
      return null;
    if (pathPattern.hasPathVariable())
      return null;

    // Parse the WHERE clause: extract anti-join pattern, optional inequality and the per-vertex predicates (issue #9608)
    final CountPushDownPredicates predicates = new CountPushDownPredicates(context);
    final AntiJoinInfo antiJoin = extractAntiJoinInfo(whereClause, predicates, nodeVariablesOf(pathPattern));
    if (antiJoin == null)
      return null;

    if (antiJoin.inequalityProperty != null
        && !propertyIdentifiesNode(database, pathPattern, antiJoin.inequalityVar1, antiJoin.inequalityVar2, antiJoin.inequalityProperty))
      return null;

    // Extract chain structure
    final int hopCount = pathPattern.getRelationshipCount();
    final String[] nodeLabels = new String[hopCount + 1];
    final String[] edgeTypes = new String[hopCount];
    final Vertex.DIRECTION[] directions = new Vertex.DIRECTION[hopCount];
    final VertexPredicate[] nodePredicates = new VertexPredicate[hopCount + 1];

    for (int i = 0; i <= hopCount; i++) {
      final NodePattern node = pathPattern.getNode(i);
      // One name per hop is all the operator carries, and a conjunction or a disjunction is not one
      // name; rather than count a set the pattern did not describe, decline (issue #6322).
      if (!hasPushDownRepresentableLabel(node))
        return null;
      if (!CountPushDownPredicates.inlinePropertiesArePerVertex(node))
        return null;
      nodeLabels[i] = node.hasLabels() ? node.getLabels().get(0) : null;
    }

    for (int i = 0; i < hopCount; i++) {
      final RelationshipPattern rel = pathPattern.getRelationship(i);
      if (rel.isVariableLength())
        return null;
      if (rel.getVariable() != null && !rel.getVariable().isEmpty())
        return null;
      if (rel.hasProperties())
        return null;
      if (!rel.hasTypes() || rel.getTypes().size() != 1)
        return null;

      edgeTypes[i] = rel.getTypes().get(0);
      final Direction dir = rel.getDirection();
      if (dir == Direction.OUT)
        directions[i] = Vertex.DIRECTION.OUT;
      else if (dir == Direction.IN)
        directions[i] = Vertex.DIRECTION.IN;
      else
        directions[i] = Vertex.DIRECTION.BOTH;
    }

    // Resolve anti-join variable positions in the chain
    int antiJoinSourceIdx = -1;
    int antiJoinTargetIdx = -1;
    for (int i = 0; i <= hopCount; i++) {
      final NodePattern node = pathPattern.getNode(i);
      final String nv = node.getVariable();
      if (nv != null) {
        if (nv.equals(antiJoin.sourceVar))
          antiJoinSourceIdx = i;
        if (nv.equals(antiJoin.targetVar))
          antiJoinTargetIdx = i;
      }
    }
    if (antiJoinSourceIdx < 0 || antiJoinTargetIdx < 0)
      return null;
    // A label on an end node of the negated pattern is implied by the MATCH only when it is the label the chain binds that variable to
    if (antiJoin.sourceLabel != null && !antiJoin.sourceLabel.equals(nodeLabels[antiJoinSourceIdx]))
      return null;
    if (antiJoin.targetLabel != null && !antiJoin.targetLabel.equals(nodeLabels[antiJoinTargetIdx]))
      return null;

    // Do NOT swap source/target — the direction depends on the original order.
    // AntiJoinChainOp handles both cases:
    //   Case A (Q9): anti-join from anchor(0) to later position → merge-scan
    //   Case B (Q8): anti-join from later position to anchor(0) → per-frontier binary search

    // Resolve inequality positions (if present)
    int inequalityIdxA = -1;
    int inequalityIdxB = -1;
    if (antiJoin.inequalityVar1 != null) {
      for (int i = 0; i <= hopCount; i++) {
        final NodePattern node = pathPattern.getNode(i);
        final String nv = node.getVariable();
        if (nv != null) {
          if (nv.equals(antiJoin.inequalityVar1))
            inequalityIdxA = i;
          else if (nv.equals(antiJoin.inequalityVar2))
            inequalityIdxB = i;
        }
      }
      if (inequalityIdxA < 0 || inequalityIdxB < 0)
        return null;
    }

    // a node variable named twice is one vertex, which the degree products of the operator cannot express (a closing hop back to the
    // first node would be counted for any target)
    if (CypherExecutionPlan.repeatsNodeVariable(pathPattern))
      return null;
    if (!isAntiJoinShapeCountedExactly(database, nodeLabels, edgeTypes, antiJoin, antiJoinSourceIdx, antiJoinTargetIdx, inequalityIdxA,
        inequalityIdxB)
        && !isOverlapShapeCountedExactly(database, nodeLabels, edgeTypes, directions, antiJoin, antiJoinSourceIdx, antiJoinTargetIdx,
        inequalityIdxA, inequalityIdxB))
      return null;

    // a node variable is named once (repeatsNodeVariable), so each position takes its own map and the conjuncts on its name
    for (int i = 0; i <= hopCount; i++)
      nodePredicates[i] = predicates.predicateFor(pathPattern.getNode(i));

    return new AntiJoinChainOp(nodeLabels, nodePredicates, edgeTypes, directions,
        antiJoinSourceIdx, antiJoinTargetIdx,
        antiJoin.antiJoinEdgeType, antiJoin.antiJoinDirection,
        inequalityIdxA, inequalityIdxB);
  }

  /**
   * Whether {@link AntiJoinChainOp} counts this chain exactly as the row pipeline would (issue #9290). The operator counts walks by
   * degree products, so it only applies Cypher's relationship uniqueness through the inequality between the two ends of the negated
   * pattern, and only for the one shape verified against the row pipeline:
   * <ul>
   *   <li>the check position is 2 and both hops before it are of the anti-join's type, one end of the negated pattern being the
   *   first node, and the inequality is between the negated pattern's two nodes (a walk back over the same relationship is then
   *   excluded);</li>
   *   <li>at most one hop follows the check position and its type does not overlap (same type or inheritance) a type used before it
   *   (it could reuse a relationship the operator does not track);</li>
   *   <li>every node of the chain is labelled unless the negated pattern starts at the first node (an unlabelled node takes the
   *   recursive path, which never applies the anti-join set when the negated pattern ends at the first node).</li>
   * </ul>
   * Any other shape is declined and takes the row pipeline: over random graphs the operator answered wrong for most of the shapes
   * outside this one, and this one answered right in every query checked (issue #9290).
   */
  private static boolean isAntiJoinShapeCountedExactly(final Database database, final String[] nodeLabels, final String[] edgeTypes, final AntiJoinInfo antiJoin,
      final int sourceIdx, final int targetIdx, final int inequalityIdxA, final int inequalityIdxB) {
    // an unlabelled node makes the operator walk the chain recursively, which never applies the anti-join set when the negated
    // pattern starts at a later node and ends at the first one
    if (sourceIdx != 0)
      for (final String label : nodeLabels)
        if (label == null)
          return false;
    if (Math.min(sourceIdx, targetIdx) != 0 || Math.max(sourceIdx, targetIdx) != 2)
      return false;
    if (inequalityIdxA < 0 || inequalityIdxB < 0 || Math.min(inequalityIdxA, inequalityIdxB) != 0 || Math.max(inequalityIdxA, inequalityIdxB) != 2)
      return false;
    if (!Objects.equals(antiJoin.antiJoinEdgeType, edgeTypes[0]) || !Objects.equals(antiJoin.antiJoinEdgeType, edgeTypes[1]))
      return false;
    // the operator multiplies the degree of the check vertex for the hops after it, which is exact for one hop and not for a chain of them
    if (edgeTypes.length > 3)
      return false;
    for (int i = 2; i < edgeTypes.length; i++)
      if (edgeTypesOverlap(database, edgeTypes[i], edgeTypes[0]) || edgeTypesOverlap(database, edgeTypes[i], edgeTypes[1]))
        return false;
    return true;
  }

  /**
   * Whether the chain is LSQB Q8, the one other shape {@link AntiJoinChainOp} counts exactly (the operator's own isOverlapShape and
   * its javadoc give the formula):
   * <pre>
   *   (t1:T)&lt;-[:E]-(m:M)&lt;-[:R]-(c:C)-[:E]-&gt;(t2:T) WHERE NOT (c)-[:E]-&gt;(t1) AND t1 &lt;&gt; t2
   * </pre>
   * Three hops, every node labelled, no undirected hop, hops 0 and 2 of the negated pattern's type {@code E} and running the same way
   * from {@code m} and from {@code c}, the negated pattern from {@code c} to {@code t1} in that way, the inequality between the ends,
   * and a middle type {@code R} that is not {@code E} nor related to it by inheritance (a relationship it could share with a
   * {@code E} hop would break the relationship uniqueness the formula assumes). The shape was counted without these checks before
   * issue #9290 and answered wrong for labels at the ends and for parallel edges, which the operator now handles.
   */
  private static boolean isOverlapShapeCountedExactly(final Database database, final String[] nodeLabels, final String[] edgeTypes,
      final Vertex.DIRECTION[] directions, final AntiJoinInfo antiJoin, final int sourceIdx, final int targetIdx, final int inequalityIdxA,
      final int inequalityIdxB) {
    if (edgeTypes.length != 3)
      return false;
    for (final String label : nodeLabels)
      if (label == null)
        return false;
    for (final Vertex.DIRECTION direction : directions)
      if (direction == Vertex.DIRECTION.BOTH)
        return false;
    if (Math.min(sourceIdx, targetIdx) != 0 || Math.max(sourceIdx, targetIdx) != 2)
      return false;
    if (inequalityIdxA < 0 || inequalityIdxB < 0 || Math.min(inequalityIdxA, inequalityIdxB) != 0
        || Math.max(inequalityIdxA, inequalityIdxB) != 3)
      return false;
    if (!Objects.equals(antiJoin.antiJoinEdgeType, edgeTypes[0]) || !Objects.equals(antiJoin.antiJoinEdgeType, edgeTypes[2])
        || edgeTypesOverlap(database, edgeTypes[1], edgeTypes[0]))
      return false;
    final Vertex.DIRECTION antiFromC = sourceIdx == 2 ? antiJoin.antiJoinDirection : reverseDirection(antiJoin.antiJoinDirection);
    return antiFromC == directions[2] && reverseDirection(directions[0]) == directions[2];
  }

  private static Vertex.DIRECTION reverseDirection(final Vertex.DIRECTION direction) {
    return direction == Vertex.DIRECTION.OUT ? Vertex.DIRECTION.IN
        : direction == Vertex.DIRECTION.IN ? Vertex.DIRECTION.OUT : Vertex.DIRECTION.BOTH;
  }

  /** Whether one relationship can belong to both types: they are the same type or one inherits from the other. */
  private static boolean edgeTypesOverlap(final Database database, final String a, final String b) {
    if (Objects.equals(a, b))
      return true;
    if (!database.getSchema().existsType(a) || !database.getSchema().existsType(b))
      return false;
    final DocumentType typeA = database.getSchema().getType(a);
    return typeA.instanceOf(b) || database.getSchema().getType(b).instanceOf(a);
  }

  /**
   * Information extracted from a WHERE clause containing an anti-join pattern.
   */
  private static final class AntiJoinInfo {
    final String sourceVar;
    final String targetVar;
    final String antiJoinEdgeType;
    final Vertex.DIRECTION antiJoinDirection;
    final String inequalityVar1; // null if no inequality
    final String inequalityVar2;
    final String inequalityProperty; // null when the inequality compares the nodes themselves
    final String sourceLabel; // label written on the negated pattern's end nodes, null when none
    final String targetLabel;

    AntiJoinInfo(final String sourceVar, final String targetVar,
        final String antiJoinEdgeType, final Vertex.DIRECTION antiJoinDirection,
        final String inequalityVar1, final String inequalityVar2, final String inequalityProperty,
        final String sourceLabel, final String targetLabel) {
      this.sourceLabel = sourceLabel;
      this.targetLabel = targetLabel;
      this.sourceVar = sourceVar;
      this.targetVar = targetVar;
      this.antiJoinEdgeType = antiJoinEdgeType;
      this.antiJoinDirection = antiJoinDirection;
      this.inequalityVar1 = inequalityVar1;
      this.inequalityVar2 = inequalityVar2;
      this.inequalityProperty = inequalityProperty;
    }
  }

  /**
   * Extracts anti-join pattern info from a WHERE clause: its conjuncts, in any order, are
   * <ul>
   *   <li>exactly one negated pattern, {@code NOT (a)-[:TYPE]-(b)};</li>
   *   <li>at most one inequality, {@code a <> b};</li>
   *   <li>any number of conjuncts reading one node of the chain and nothing else, recorded in {@code predicates} for the
   *   operator to apply per vertex (issue #9608).</li>
   * </ul>
   *
   * @param nodeVariables the node variables of the chain, the only ones a per-vertex conjunct may read
   *
   * @return extracted info, or null if the WHERE clause doesn't match
   */
  private static AntiJoinInfo extractAntiJoinInfo(final WhereClause whereClause, final CountPushDownPredicates predicates,
      final Set<String> nodeVariables) {
    if (whereClause == null || whereClause.getConditionExpression() == null)
      return null;

    PatternPredicateExpression negated = null;
    String[] inequality = null;
    for (final BooleanExpression conjunct : CountPushDownPredicates.conjuncts(whereClause.getConditionExpression())) {
      final PatternPredicateExpression pattern = extractNegatedPattern(conjunct);
      if (pattern != null) {
        if (negated != null)
          return null;
        negated = pattern;
        continue;
      }
      if (inequality == null && conjunct instanceof ComparisonExpression comparison) {
        inequality = extractInequalityFromComparison(comparison);
        if (inequality != null)
          continue;
      }
      if (!predicates.addWhereConjunct(conjunct, nodeVariables))
        return null;
    }
    if (negated == null)
      return null;
    return inequality != null ? extractFromPatternPredicate(negated, inequality[0], inequality[1], inequality[2])
        : extractFromPatternPredicate(negated, null, null, null);
  }

  /**
   * Extracts a negated pattern predicate from a boolean expression.
   * Handles two forms:
   * <ul>
   *   <li>{@code PatternPredicateExpression(isNegated=true)}</li>
   *   <li>{@code LogicalExpression(NOT, PatternPredicateExpression)}</li>
   * </ul>
   *
   * @return the pattern predicate (always with isNegated semantics), or null
   */
  private static PatternPredicateExpression extractNegatedPattern(final BooleanExpression expr) {
    if (expr instanceof PatternPredicateExpression) {
      final PatternPredicateExpression ppe = (PatternPredicateExpression) expr;
      return ppe.isNegated() ? ppe : null;
    }
    if (expr instanceof LogicalExpression) {
      final LogicalExpression logical = (LogicalExpression) expr;
      if (logical.getOperator() == LogicalExpression.Operator.NOT
          && logical.getLeft() instanceof PatternPredicateExpression) {
        // NOT wrapping a non-negated pattern predicate = negated pattern
        return (PatternPredicateExpression) logical.getLeft();
      }
    }
    return null;
  }

  /**
   * Extracts anti-join info from a negated pattern predicate.
   * The pattern must be a single-hop, single-type, anonymous relationship between two variables.
   */
  /**
   * Extracts anti-join info from a pattern predicate expression.
   * The pattern must be a single-hop, single-type, anonymous relationship between two variables.
   * Note: the negation may come from either PatternPredicateExpression.isNegated() or from
   * a wrapping LogicalExpression(NOT, ...) — the caller ensures negation semantics.
   */
  private static AntiJoinInfo extractFromPatternPredicate(final PatternPredicateExpression ppe,
      final String inequalityVar1, final String inequalityVar2, final String inequalityProperty) {
    final PathPattern pp = ppe.getPathPattern();
    if (pp == null || pp.getRelationshipCount() != 1)
      return null;

    final RelationshipPattern rel = pp.getRelationship(0);
    if (rel.isVariableLength())
      return null;
    if (!rel.hasTypes() || rel.getTypes().size() != 1)
      return null;
    // THE PUSH-DOWN ONLY LOOKS AT THE EXISTENCE OF AN EDGE OF THE TYPE: ANYTHING THAT NEEDS THE EDGE OR END NODE RECORDS IS LEFT TO
    // THE ROW PIPELINE (issue #9278)
    if (rel.hasProperties() || rel.hasWhereExpression() || rel.getVariable() != null)
      return null;
    final NodePattern first = pp.getFirstNode();
    final NodePattern last = pp.getLastNode();
    if (first.hasProperties() || last.hasProperties() || !hasPushDownRepresentableLabel(first) || !hasPushDownRepresentableLabel(last))
      return null;

    final String sourceVar = first.getVariable();
    final String targetVar = last.getVariable();
    if (sourceVar == null || targetVar == null)
      return null;

    final String edgeType = rel.getTypes().get(0);
    final Direction dir = rel.getDirection();
    final Vertex.DIRECTION direction = dir == Direction.OUT ? Vertex.DIRECTION.OUT
        : dir == Direction.IN ? Vertex.DIRECTION.IN : Vertex.DIRECTION.BOTH;

    return new AntiJoinInfo(sourceVar, targetVar, edgeType, direction,
        inequalityVar1, inequalityVar2, inequalityProperty,
        first.hasLabels() ? first.getLabels().get(0) : null, last.hasLabels() ? last.getLabels().get(0) : null);
  }

  /**
   * Extracts inequality info from a comparison expression.
   * Returns [var1, var2] if the expression is "var1 <> var2", null otherwise.
   */
  private static String[] extractInequalityFromComparison(final ComparisonExpression cmp) {
    if (cmp.getOperator() != ComparisonExpression.Operator.NOT_EQUALS)
      return null;
    return nodeInequalityOperands(cmp.getLeft(), cmp.getRight());
  }

  /**
   * The two variables an inequality compares as nodes, as {@code [var1, var2, property]}, or null when it is not one. The
   * property is null for {@code a <> b} and {@code id(a) <> id(b)}, which are the node inequality as written; it is the name
   * for {@code a.p <> b.p}, which stands for the node inequality only when {@code p} is unique over the node's type (see
   * {@link #propertyIdentifiesNode}), a proof the caller owes before it trusts the pair (issue #9203).
   */
  private static String[] nodeInequalityOperands(final Expression left, final Expression right) {
    if (left instanceof VariableExpression l && right instanceof VariableExpression r)
      return new String[] { l.getVariableName(), r.getVariableName(), null };
    if (left instanceof FunctionCallExpression l && right instanceof FunctionCallExpression r && "id".equalsIgnoreCase(l.getFunctionName())
        && "id".equalsIgnoreCase(r.getFunctionName()) && l.getArguments().size() == 1 && r.getArguments().size() == 1
        && !l.isDistinct() && !r.isDistinct() && l.getArguments().get(0) instanceof VariableExpression lv
        && r.getArguments().get(0) instanceof VariableExpression rv)
      return new String[] { lv.getVariableName(), rv.getVariableName(), null };
    if (left instanceof PropertyAccessExpression l && right instanceof PropertyAccessExpression r
        && l.getPropertyName().equals(r.getPropertyName()))
      return new String[] { l.getVariableName(), r.getVariableName(), l.getPropertyName() };
    return null;
  }

  /**
   * Whether {@code a.property <> b.property} says the same as {@code a <> b} for the two nodes of the pattern: both carry the
   * one same label, and the property of that type is unique (a UNIQUE index on it alone) and never null (mandatory and not
   * null), so two nodes share a value only when they are the same node, and a null never turns the comparison into null.
   */
  private static boolean propertyIdentifiesNode(final Database db, final PathPattern pathPattern, final String variableA,
      final String variableB, final String property) {
    final String label = uniqueLabelOf(pathPattern, variableA);
    if (label == null || !label.equals(uniqueLabelOf(pathPattern, variableB)) || !db.getSchema().existsType(label))
      return false;
    final DocumentType type = db.getSchema().getType(label);
    if (!type.existsProperty(property))
      return false;
    final Property prop = type.getProperty(property);
    if (!prop.isMandatory() || !prop.isNotNull())
      return false;
    // any unique index on exactly this property. getAllIndexes(true) walks UP: the type's own indexes and its ancestors', which span
    // (and enforce uniqueness across) the type's buckets, never an index that only a sub type has
    for (final TypeIndex index : TypeIndex.filterReadyForQueries(type.getAllIndexes(true)))
      if (index.isUniqueIfPresent() && index.getPropertyNames().size() == 1 && index.getPropertyNames().get(0).equals(property))
        return true;
    return false;
  }

  /** The single label every occurrence of the variable carries in the pattern, or null when it has none or several. */
  private static String uniqueLabelOf(final PathPattern pathPattern, final String variable) {
    String label = null;
    for (int i = 0; i < pathPattern.getNodeCount(); i++) {
      final NodePattern node = pathPattern.getNode(i);
      if (!variable.equals(node.getVariable()) || !node.hasLabels())
        continue;
      if (!hasPushDownRepresentableLabel(node) || (label != null && !label.equals(node.getLabels().get(0))))
        return null;
      label = node.getLabels().get(0);
    }
    return label;
  }

  /**
   * Builds a star-join arm going forward from centralIdx toward endIdx in the path pattern.
   * Direction is preserved as-is from the pattern.
   */
  /**
   * The one label a star-join arm carries for the node a hop reaches, or null when it has none. Relies on
   * {@link #hasPushDownRepresentableLabel} having declined every other label shape (a label set, a dynamic label) in
   * {@code tryDetectStarCountStar} before an arm is built, so taking the first label is taking the only one.
   */
  private static String endpointLabelOf(final NodePattern node) {
    assert !node.hasLabels() || node.getLabels().size() == 1 : "a multi-label endpoint must have declined the push-down";
    return node.hasLabels() ? node.getLabels().get(0) : null;
  }

  private DegreeProductOp.Arm buildArmForward(final PathPattern pathPattern, final int centralIdx,
      final int endIdx, final boolean optional, final CountPushDownPredicates predicates) {
    final int hops = endIdx - centralIdx;
    final String[] edgeTypes = new String[hops];
    final String[] endpointLabels = new String[hops];
    final VertexPredicate[] endpointPredicates = new VertexPredicate[hops];
    final Vertex.DIRECTION[] directions = new Vertex.DIRECTION[hops];
    for (int i = 0; i < hops; i++) {
      endpointLabels[i] = endpointLabelOf(pathPattern.getNode(centralIdx + i + 1));
      endpointPredicates[i] = predicates.predicateFor(pathPattern.getNode(centralIdx + i + 1));
      final RelationshipPattern rel = pathPattern.getRelationship(centralIdx + i);
      if (rel.isVariableLength() || (rel.getVariable() != null && !rel.getVariable().isEmpty())
          || rel.hasProperties() || !rel.hasTypes() || rel.getTypes().size() != 1)
        return null;
      edgeTypes[i] = rel.getTypes().get(0);
      final Direction dir = rel.getDirection();
      directions[i] = dir == Direction.OUT ? Vertex.DIRECTION.OUT
          : dir == Direction.IN ? Vertex.DIRECTION.IN : Vertex.DIRECTION.BOTH;
    }
    return new DegreeProductOp.Arm(edgeTypes, directions, optional, endpointLabels, endpointPredicates);
  }

  /**
   * Builds a star-join arm going backward from centralIdx toward endIdx in the path pattern.
   * Directions are reversed since we traverse from the central node toward position 0.
   */
  private DegreeProductOp.Arm buildArmBackward(final PathPattern pathPattern, final int centralIdx,
      final int endIdx, final boolean optional, final CountPushDownPredicates predicates) {
    final int hops = centralIdx - endIdx;
    final String[] edgeTypes = new String[hops];
    final String[] endpointLabels = new String[hops];
    final VertexPredicate[] endpointPredicates = new VertexPredicate[hops];
    final Vertex.DIRECTION[] directions = new Vertex.DIRECTION[hops];
    for (int i = 0; i < hops; i++) {
      endpointLabels[i] = endpointLabelOf(pathPattern.getNode(centralIdx - 1 - i));
      endpointPredicates[i] = predicates.predicateFor(pathPattern.getNode(centralIdx - 1 - i));
      // Walk backward from centralIdx: rel at (centralIdx-1), (centralIdx-2), ...
      final RelationshipPattern rel = pathPattern.getRelationship(centralIdx - 1 - i);
      if (rel.isVariableLength() || (rel.getVariable() != null && !rel.getVariable().isEmpty())
          || rel.hasProperties() || !rel.hasTypes() || rel.getTypes().size() != 1)
        return null;
      edgeTypes[i] = rel.getTypes().get(0);
      // Reverse direction since we're traversing the arm in the opposite direction
      final Direction dir = rel.getDirection().reverse();
      directions[i] = dir == Direction.OUT ? Vertex.DIRECTION.OUT
          : dir == Direction.IN ? Vertex.DIRECTION.IN : Vertex.DIRECTION.BOTH;
    }
    return new DegreeProductOp.Arm(edgeTypes, directions, optional, endpointLabels, endpointPredicates);
  }
}
