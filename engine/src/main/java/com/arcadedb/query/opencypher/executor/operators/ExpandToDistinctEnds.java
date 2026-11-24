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
package com.arcadedb.query.opencypher.executor.operators;

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.EdgeIdentitySet;
import com.arcadedb.graph.GhostEdgeReporter;
import com.arcadedb.graph.IncomingEdgeLookup;
import com.arcadedb.graph.Vertex;
import com.arcadedb.graph.VertexInternal;
import com.arcadedb.query.opencypher.ast.Direction;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.HeapEstimator;
import com.arcadedb.query.sql.executor.OperationHeapLimit;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.query.sql.executor.WorkGuard;
import com.arcadedb.utility.RidHashSet;
import com.arcadedb.utility.RidIntHashMap;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * A chain of {@link ExpandAll} hops over anonymous relationships and anonymous intermediate nodes, answered for a statement
 * that only reads the DISTINCT far ends: each far end reachable from an input row is produced once for that row, instead of
 * once per path that reaches it.
 * <p>
 * In {@code MATCH (p)-[:K]-()-[:K]-()-[:K]-(x) RETURN count(DISTINCT x)} the walk enumerates every path, hundreds of
 * thousands of them from a well-connected start, to find a few thousand distinct x. Most of those paths share their last
 * intermediate vertex b with many others, and the walk reads b's relationships once per path that reaches b. Here the
 * paths are walked up to b, grouped by b, and b's relationships are read once: the far end x across the relationship e is
 * reached when at least one path to b uses neither e nor anything else the pattern rules out, which is what relationship
 * uniqueness asks. Only the relationships of a path that touch b can be e, since e touches b, so a path to b is kept as
 * just those ("blocking" relationships); two paths with no blocking relationship in common let every e through, and b then
 * keeps nothing.
 * <p>
 * The rows differ from the chain's only in how many times each one comes: the planner substitutes this operator only for
 * a statement that cannot tell (see {@code DistinctEndsRewrite}). Each hop keeps the checks of the {@link ExpandAll} it
 * replaces: the label of its far end, its relationships against the ones bound before it in its clause, a ghost entry
 * skipped and reported. A hop that has to look up the incoming side of a unidirectional type runs the replaced chain
 * itself.
 * <p>
 * Memory: the paths to b are held per input row, at most a few references per distinct b, charged to the query's heap
 * budget with the far ends found for the row.
 */
public class ExpandToDistinctEnds extends AbstractPhysicalOperator {
  // AN ENTRY OF THE FRONTIER: THE INDEX IN THE MAP, THE VERTEX, THE FLAG, THE LIST OF BLOCKING SETS
  private static final int FRONTIER_ENTRY_BYTES = 96;

  private final ExpandAll[]   hops;
  private final ExpandAll     replacedTop;
  // checkedAgainst[i][j]: hop i must not reuse the relationship hop j bound (j < i)
  private final boolean[][]   checkedAgainst;
  // The relationship variables bound below the chain that hop i must not reuse, or null
  private final String[][]    externalVariables;

  /**
   * @param hops the hops of the chain, from the one reading the input row to the one binding the far end
   */
  public ExpandToDistinctEnds(final List<ExpandAll> hops) {
    super(hops.getFirst().getChild(), hops.getLast().getEstimatedCost(), hops.getLast().getEstimatedCardinality());
    this.hops = hops.toArray(new ExpandAll[0]);
    this.replacedTop = hops.getLast();

    final int count = this.hops.length;
    checkedAgainst = new boolean[count][count];
    externalVariables = new String[count][];
    for (int i = 0; i < count; i++) {
      final Set<String> preceding = this.hops[i].getSameClausePrecedingRelVars();
      if (preceding == null || preceding.isEmpty())
        continue;
      final List<String> external = new ArrayList<>();
      for (final String variable : preceding) {
        boolean internal = false;
        for (int j = 0; j < i; j++)
          if (variable.equals(this.hops[j].getEdgeTrackingVar())) {
            checkedAgainst[i][j] = true;
            internal = true;
          }
        if (!internal)
          external.add(variable);
      }
      if (!external.isEmpty())
        externalVariables[i] = external.toArray(new String[0]);
    }
  }

  @Override
  public ResultSet execute(final CommandContext context, final int nRecords) {
    for (final ExpandAll hop : hops)
      if (IncomingEdgeLookup.isNeeded(context, context.getDatabase(), hop.getDirection().toArcadeDirection(), hop.getEdgeTypes()))
        // The incoming side of a unidirectional type is answered by the query's lookup, which the replaced chain knows
        return replacedTop.execute(context, nRecords);

    final WorkGuard guard = WorkGuard.forCommandDeadline(context);
    final ResultSet inputResults = child.execute(context, nRecords);
    final OperationHeapLimit limit = OperationHeapLimit.of(context, "distinct far ends of " + getOperatorType());

    return new ResultSet() {
      private final List<Result> buffer      = new ArrayList<>();
      private       int          bufferIndex = 0;
      private       boolean      finished    = false;
      private       Result       currentInput;
      private       List<Vertex> currentEnds;
      private       int          currentEndIndex;
      private       long         chargedBytes;

      @Override
      public boolean hasNext() {
        if (bufferIndex < buffer.size())
          return true;
        if (finished)
          return false;
        fetchMore(nRecords > 0 ? nRecords : 100);
        return bufferIndex < buffer.size();
      }

      @Override
      public Result next() {
        if (!hasNext())
          throw new NoSuchElementException();
        return buffer.get(bufferIndex++);
      }

      private void fetchMore(final int n) {
        buffer.clear();
        bufferIndex = 0;
        final String endVariable = hops[hops.length - 1].getTargetVariable();

        while (buffer.size() < n) {
          if (currentEnds != null && currentEndIndex < currentEnds.size()) {
            final ResultInternal row = ResultInternal.copyBindings(currentInput, 1);
            row.setProperty(endVariable, currentEnds.get(currentEndIndex++));
            buffer.add(row);
            continue;
          }

          releaseCharge();
          if (!inputResults.hasNext()) {
            finished = true;
            break;
          }
          currentInput = inputResults.next();
          currentEnds = distinctEnds(currentInput, context, guard, this::charge);
          currentEndIndex = 0;
        }
      }

      private void charge(final long elements, final long bytes) {
        limit.check(elements);
        limit.charge(bytes);
        chargedBytes += bytes;
      }

      private void releaseCharge() {
        if (chargedBytes > 0) {
          limit.release(chargedBytes);
          chargedBytes = 0;
        }
        currentEnds = null;
      }

      @Override
      public void close() {
        releaseCharge();
        inputResults.close();
      }
    };
  }

  @FunctionalInterface
  private interface Charge {
    void charge(long elements, long bytes);
  }

  /** The vertex at the end of the paths of the chain from {@code input}, each once, in the order they are first found. */
  private List<Vertex> distinctEnds(final Result input, final CommandContext context, final WorkGuard guard, final Charge charge) {
    final Vertex start = input.getProperty(hops[0].getSourceVariable());
    if (start == null)
      // OPTIONAL MATCH leaves the source unbound: the chain produces nothing for it
      return List.of();

    final EdgeIdentitySet[] external = new EdgeIdentitySet[hops.length];
    for (int i = 0; i < hops.length; i++)
      if (externalVariables[i] != null)
        external[i] = RelationshipBindings.collectEdgeIdentities(input, Set.of(externalVariables[i]));

    final Frontier frontier = new Frontier(hops.length - 1, charge);
    final Edge[] path = new Edge[hops.length];
    final Vertex[] vertices = new Vertex[hops.length + 1];
    vertices[0] = start;
    walkPrefix(0, path, vertices, external, frontier, context, guard);

    // The last hop, once per distinct vertex the paths before it end on
    final int last = hops.length - 1;
    final ExpandAll lastHop = hops[last];
    final RidHashSet found = new RidHashSet();
    final List<Vertex> ends = new ArrayList<>();
    for (int f = 0; f < frontier.size(); f++) {
      final Vertex b = frontier.vertices.get(f);
      final List<RID[]> blocking = frontier.free.get(f) ? null : frontier.blocking.get(f);
      final Iterator<?> step = steps(lastHop, b, context);
      while (step.hasNext()) {
        guard.check();
        final Vertex x;
        if (step instanceof EdgeStep edges) {
          final Edge edge = edges.next();
          final RID identity = edge.getIdentity();
          if (external[last] != null && external[last].contains(identity))
            continue;
          if (blocking != null && !anyLeavesThrough(blocking, identity))
            continue;
          try {
            x = farEnd(edge, b, lastHop.getDirection());
          } catch (final RecordNotFoundException e) {
            GhostEdgeReporter.reportSkipped(e);
            continue;
          }
        } else {
          try {
            x = (Vertex) step.next();
          } catch (final RecordNotFoundException e) {
            GhostEdgeReporter.reportSkipped(e);
            continue;
          }
        }
        if (lastHop.getTargetLabel() != null && !x.getType().instanceOf(lastHop.getTargetLabel()))
          continue;
        if (found.add(x.getIdentity())) {
          ends.add(x);
          // COUNTED AS THE DISTINCT AGGREGATION THAT READS THEM COUNTS ITS SET: THE OPERATOR NEVER STOPS A QUERY THAT WOULD PASS THERE
          charge.charge(Math.max(frontier.size(), ends.size()), HeapEstimator.REFERENCE_BYTES * 3L);
        }
      }
    }
    return ends;
  }

  /**
   * Walks the hops before the last one depth-first from {@code vertices[depth]}, with every check of each hop, and records
   * in the frontier the vertex each complete path ends on with the relationships of the path the last hop must not reuse.
   */
  private void walkPrefix(final int depth, final Edge[] path, final Vertex[] vertices, final EdgeIdentitySet[] external,
      final Frontier frontier, final CommandContext context, final WorkGuard guard) {
    final int last = hops.length - 1;
    if (depth == last) {
      frontier.add(vertices[depth], path, checkedAgainst[last]);
      return;
    }

    final ExpandAll hop = hops[depth];
    final Vertex source = vertices[depth];
    final Iterator<?> step = steps(hop, source, context);
    while (step.hasNext()) {
      guard.check();
      final Vertex target;
      if (step instanceof EdgeStep edges) {
        final Edge edge = edges.next();
        final RID identity = edge.getIdentity();
        if (external[depth] != null && external[depth].contains(identity))
          continue;
        if (reusesPathRelationship(depth, path, identity))
          continue;
        try {
          target = farEnd(edge, source, hop.getDirection());
        } catch (final RecordNotFoundException e) {
          GhostEdgeReporter.reportSkipped(e);
          continue;
        }
        path[depth] = edge;
      } else {
        try {
          target = (Vertex) step.next();
        } catch (final RecordNotFoundException e) {
          GhostEdgeReporter.reportSkipped(e);
          continue;
        }
        path[depth] = null;
      }
      if (hop.getTargetLabel() != null && !target.getType().instanceOf(hop.getTargetLabel()))
        continue;

      vertices[depth + 1] = target;
      walkPrefix(depth + 1, path, vertices, external, frontier, context, guard);
    }
    path[depth] = null;
  }

  /** Whether {@code identity} is a relationship an earlier hop of the path bound that hop {@code depth} must not reuse. */
  private boolean reusesPathRelationship(final int depth, final Edge[] path, final RID identity) {
    for (int j = 0; j < depth; j++)
      if (checkedAgainst[depth][j] && path[j] != null && sameEdge(path[j].getIdentity(), identity))
        return true;
    return false;
  }

  /** Whether at least one of the paths, kept as its blocking relationships, leaves the relationship {@code identity} free. */
  private static boolean anyLeavesThrough(final List<RID[]> blocking, final RID identity) {
    for (final RID[] set : blocking) {
      boolean blocked = false;
      for (final RID rid : set)
        if (sameEdge(rid, identity)) {
          blocked = true;
          break;
        }
      if (!blocked)
        return true;
    }
    return false;
  }

  /** The identity of a relationship as {@link EdgeIdentitySet} tells them apart, lightweight ones included. */
  static boolean sameEdge(final RID a, final RID b) {
    if (a.getPosition() >= 0 || b.getPosition() >= 0)
      return a.getPosition() == b.getPosition() && a.getBucketId() == b.getBucketId();
    final EdgeIdentitySet one = new EdgeIdentitySet();
    one.add(a);
    return one.contains(b);
  }

  /** The steps of {@code hop} from {@code source}: its relationships when it has to tell them apart, its neighbours otherwise. */
  private Iterator<?> steps(final ExpandAll hop, final Vertex source, final CommandContext context) {
    final Vertex.DIRECTION direction = hop.getDirection().toArcadeDirection();
    if (hop.getEdgeTrackingVar() == null && (hop.getSameClausePrecedingRelVars() == null || hop.getSameClausePrecedingRelVars()
        .isEmpty()))
      return source.getVertices(direction, hop.getEdgeTypes()).iterator();
    final Iterator<Edge> edges = source instanceof VertexInternal internal ?
        ((DatabaseInternal) context.getDatabase()).getGraphEngine().getEdgesKnowingEndpoints(internal, direction, hop.getEdgeTypes()) :
        source.getEdges(direction, hop.getEdgeTypes()).iterator();
    return new EdgeStep(edges);
  }

  /** The vertex at the other end of {@code edge}, as {@link ExpandAll} resolves it: only that end, without the edge record. */
  private static Vertex farEnd(final Edge edge, final Vertex source, final Direction direction) {
    if (direction == Direction.OUT)
      return edge.getInVertex();
    if (direction == Direction.IN)
      return edge.getOutVertex();
    return edge.getOut().equals(source.getIdentity()) ? edge.getInVertex() : edge.getOutVertex();
  }

  /** The relationships of a hop, told apart from its neighbours by type. */
  private static final class EdgeStep implements Iterator<Edge> {
    private final Iterator<Edge> edges;

    private EdgeStep(final Iterator<Edge> edges) {
      this.edges = edges;
    }

    @Override
    public boolean hasNext() {
      return edges.hasNext();
    }

    @Override
    public Edge next() {
      return edges.next();
    }
  }

  /**
   * The vertices the paths before the last hop end on, each with what keeps the last hop from reusing a relationship: free
   * when some path to it, or two paths together, block nothing, otherwise the blocking relationships of each distinct path.
   */
  private static final class Frontier {
    private final int                 lastHop;
    private final Charge              charge;
    private final RidIntHashMap       index     = new RidIntHashMap(256);
    private final List<Vertex>        vertices  = new ArrayList<>();
    private final List<Boolean>       free      = new ArrayList<>();
    private final List<List<RID[]>>   blocking  = new ArrayList<>();

    private Frontier(final int lastHop, final Charge charge) {
      this.lastHop = lastHop;
      this.charge = charge;
    }

    int size() {
      return vertices.size();
    }

    /** Records a path ending on {@code end}: of its relationships, the ones the last hop is checked against and touching end. */
    void add(final Vertex end, final Edge[] path, final boolean[] checkedByLastHop) {
      final RID endIdentity = end.getIdentity();
      int slot = index.get(endIdentity.getBucketId(), endIdentity.getPosition(), -1);
      if (slot < 0) {
        slot = vertices.size();
        index.put(endIdentity.getBucketId(), endIdentity.getPosition(), slot);
        vertices.add(end);
        free.add(Boolean.FALSE);
        blocking.add(new ArrayList<>(2));
        charge.charge(vertices.size(), FRONTIER_ENTRY_BYTES);
      } else if (free.get(slot))
        return;

      // THE RELATIONSHIPS OF THE PATH THAT THE LAST HOP COULD REUSE: ONLY ONES TOUCHING end, SINCE ITS OWN TOUCH end
      RID[] set = null;
      int size = 0;
      for (int j = 0; j < lastHop; j++) {
        final Edge edge = path[j];
        if (edge == null || !checkedByLastHop[j] || !(edge.getOut().equals(endIdentity) || edge.getIn().equals(endIdentity)))
          continue;
        if (set == null)
          set = new RID[lastHop];
        set[size++] = edge.getIdentity();
      }
      if (size == 0) {
        free.set(slot, Boolean.TRUE);
        blocking.set(slot, null);
        return;
      }
      if (size < set.length) {
        final RID[] trimmed = new RID[size];
        System.arraycopy(set, 0, trimmed, 0, size);
        set = trimmed;
      }

      final List<RID[]> sets = blocking.get(slot);
      for (final RID[] other : sets) {
        if (disjoint(other, set)) {
          // NO RELATIONSHIP IS IN BOTH: WHATEVER THE LAST HOP TAKES, ONE OF THE TWO PATHS LEAVES IT FREE
          free.set(slot, Boolean.TRUE);
          blocking.set(slot, null);
          return;
        }
        if (sameSet(other, set))
          return;
      }
      sets.add(set);
    }

    private static boolean disjoint(final RID[] a, final RID[] b) {
      for (final RID x : a)
        for (final RID y : b)
          if (sameEdge(x, y))
            return false;
      return true;
    }

    private static boolean sameSet(final RID[] a, final RID[] b) {
      if (a.length != b.length)
        return false;
      for (final RID x : a) {
        boolean in = false;
        for (final RID y : b)
          if (sameEdge(x, y)) {
            in = true;
            break;
          }
        if (!in)
          return false;
      }
      return true;
    }
  }

  @Override
  public String getOperatorType() {
    return "ExpandToDistinctEnds";
  }

  @Override
  public String explain(final int depth) {
    final StringBuilder sb = new StringBuilder();
    sb.append(getIndent(depth)).append("+ ExpandToDistinctEnds(").append(hops[0].getSourceVariable()).append(")");
    for (final ExpandAll hop : hops) {
      sb.append(hop.getDirection() == Direction.IN ? "<-[" : "-[");
      if (hop.getEdgeTypes() != null && hop.getEdgeTypes().length > 0)
        sb.append(":").append(String.join("|", hop.getEdgeTypes()));
      sb.append(hop.getDirection() == Direction.OUT ? "]->(" : "]-(");
      if (hop == hops[hops.length - 1])
        sb.append(hop.getTargetVariable());
      if (hop.getTargetLabel() != null)
        sb.append(":").append(hop.getTargetLabel());
      sb.append(")");
    }
    sb.append(" [distinct ends, cost=").append(String.format(Locale.US, "%.2f", estimatedCost));
    sb.append(", rows=").append(estimatedCardinality).append("]\n");
    if (child != null)
      sb.append(child.explain(depth + 1));
    return sb.toString();
  }

  public List<ExpandAll> getHops() {
    return List.of(hops);
  }
}
