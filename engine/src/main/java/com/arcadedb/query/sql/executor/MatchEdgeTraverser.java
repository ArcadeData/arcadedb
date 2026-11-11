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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Document;
import com.arcadedb.database.Identifiable;
import com.arcadedb.graph.GraphTraversalProvider;
import com.arcadedb.graph.GraphTraversalProviderRegistry;
import com.arcadedb.graph.IncomingEdgeLookup;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.parser.MatchPathItem;
import com.arcadedb.query.sql.parser.Rid;
import com.arcadedb.query.sql.parser.WhereClause;
import com.arcadedb.schema.DocumentType;

import java.util.*;

/**
 * Created by luigidellaquila on 23/09/16.
 */
public class MatchEdgeTraverser {
  protected final Result        sourceRecord;
  protected       EdgeTraversal edge;
  protected final MatchPathItem item;

  Iterator<ResultInternal> downstream;

  public MatchEdgeTraverser(final Result lastUpstreamRecord, final EdgeTraversal edge) {
    this.sourceRecord = lastUpstreamRecord;
    this.edge = edge;
    this.item = edge.edge.item;
  }

  public MatchEdgeTraverser(final Result lastUpstreamRecord, final MatchPathItem item) {
    this.sourceRecord = lastUpstreamRecord;
    this.item = item;
  }

  public boolean hasNext(final CommandContext context) {
    init(context);
    return downstream.hasNext();
  }

  public Result next(final CommandContext context) {
    init(context);
    if (!downstream.hasNext())
      throw new NoSuchElementException();

    final String endPointAlias = getEndpointAlias();
    final ResultInternal nextR = downstream.next();
    final Document nextElement = nextR.getElement().get();
    final Object prevValue = sourceRecord.getProperty(endPointAlias);
    if (prevValue != null && !equals(prevValue, nextElement))
      return null;

    final ResultInternal result = new ResultInternal(context.getDatabase());
    for (final String prop : sourceRecord.getPropertyNames())
      result.setProperty(prop, sourceRecord.getProperty(prop));

    result.setProperty(endPointAlias, toResult(nextElement));
    if (edge.edge.item.getFilter().getDepthAlias() != null)
      result.setProperty(edge.edge.item.getFilter().getDepthAlias(), nextR.getMetadata("$depth"));

    if (edge.edge.item.getFilter().getPathAlias() != null)
      result.setProperty(edge.edge.item.getFilter().getPathAlias(), nextR.getMetadata("$matchPath"));

    return result;
  }

  /**
   * Gives back the heap a {@code while:}/{@code maxDepth:} walk still holds: one stopped before its end (a {@code LIMIT}, a
   * closed result set) holds its current path, which nothing else would release before the query context is collected.
   * Idempotent.
   */
  public void close() {
    if (downstream instanceof DepthFirstPaths paths)
      paths.release();
  }

  protected boolean equals(Object prevValue, Identifiable nextElement) {
    if (prevValue instanceof Result result) {
      prevValue = result.getElement().orElse(null);
    }
    if (nextElement instanceof Result result) {
      nextElement = result.getElement().orElse(null);
    }
    return prevValue != null && prevValue.equals(nextElement);
  }

  protected Object toResult(final Document nextElement) {
    return new ResultInternal(nextElement);
  }

  protected String getStartingPointAlias() {
    return this.edge.edge.out.alias;
  }

  protected String getEndpointAlias() {
    if (this.item != null) {
      return this.item.getFilter().getAlias();
    }
    return this.edge.edge.in.alias;
  }

  /**
   * Returns {@code false} for a while/maxDepth pattern item: {@link GraphTraversalProvider#isConnectedTo} only
   * ever answers whether two vertices are joined by a single hop, so a variable-depth item must keep going
   * through the recursive {@link #executeTraversal} instead of taking the expand-into fast path.
   */
  private static boolean isSingleHopExpandable(final MatchPathItem item) {
    if (item == null)
      return false;
    final var filter = item.getFilter();
    return filter == null || (filter.getWhileCondition() == null && filter.getMaxDepth() == null);
  }

  /**
   * Returns the {@link Vertex.DIRECTION} the GAV expand-into fast path (see {@link #init}) should query, as seen
   * from this traverser's own starting point (i.e. the vertex {@link #getStartingPointAlias()} names): the same
   * direction {@link #traversePatternEdge} would use by invoking {@code item.getMethod().execute(...)}. Returns
   * {@code null} when the pattern edge has no single direction to ask a provider about - no method (a
   * {@code FieldMatchPathItem}/{@code MultiMatchPathItem}) or a method other than out/in/both (outE/inE/bothE/
   * outV/inV need the edge or the far vertex itself, not just a connectivity check) - in which case the fast path
   * is skipped in favour of the slow, always-correct traversal.
   */
  protected Vertex.DIRECTION getExpandIntoDirection() {
    return MatchExecutionPlanner.directionFor(item, true);
  }

  protected void init(final CommandContext context) {
    if (downstream == null) {
      Identifiable startingElem = sourceRecord.getElementProperty(getStartingPointAlias());
      if (startingElem instanceof Result result) {
        startingElem = result.getElement().orElse(null);
      }

      // Expand-into optimization: when both endpoints are already bound, use isConnectedTo()
      // for O(log degree) binary search instead of traversing all neighbors. This does not re-check the
      // endpoint's own filters/class/cluster/rid (unlike executeTraversal(), which calls matchesFilters() etc.
      // per candidate): those were already applied when the endpoint alias was first bound, and next()'s
      // equals(prevValue, nextElement) check below (the same one the slow path relies on to reject a
      // subsequent binding that disagrees with an earlier one) is what re-validates identity here, not a
      // second filter pass. This is by design, not an oversight.
      final String endPointAlias = getEndpointAlias();
      final Object prevValue = sourceRecord.getProperty(endPointAlias);
      if (prevValue != null && startingElem instanceof Vertex sourceVertex) {
        Identifiable targetElem = null;
        if (prevValue instanceof Identifiable identifiable)
          targetElem = identifiable;
        else if (prevValue instanceof Result r)
          targetElem = r.getElement().orElse(null);

        // Only a plain out()/in()/both() hop is eligible: the fast path returns just the bound vertex, which is
        // wrong for outE()/inE()/bothE()/outV()/inV() (an edge/vertex object is expected) and item.getMethod() is
        // null for a FieldMatchPathItem/MultiMatchPathItem (no single direction to ask the provider about). A
        // while/maxDepth item is a variable-depth traversal - isConnectedTo() only ever answers a single hop, so
        // it must keep going through the recursive executeTraversal() below instead.
        final Vertex.DIRECTION direction = targetElem != null && edge != null && isSingleHopExpandable(item)
            ? getExpandIntoDirection() : null;
        // hasAnyProviders() short-circuits on a single volatile read, avoiding extractEdgeLabels()'s array/string
        // work below for the overwhelmingly common case where no GAV provider is registered for any database.
        if (direction != null && GraphTraversalProviderRegistry.hasAnyProviders()) {
          // direction != null already proved item.getMethod() != null (see getExpandIntoDirection()), which is
          // what extractEdgeLabels() dereferences without its own null check - keep this call after that guard.
          final String[] edgeTypes = MatchExecutionPlanner.extractEdgeLabels(edge);
          // Pinned for the statement, so the two ids and the connection belong to one snapshot (issue #10415)
          final GraphTraversalProvider provider = GraphTraversalProviderRegistry.findProvider(context, edgeTypes);
          if (provider != null) {
            final int srcId = provider.getNodeId(sourceVertex.getIdentity());
            final int tgtId = provider.getNodeId(targetElem.getIdentity());
            if (srcId >= 0 && tgtId >= 0) {
              if (provider.isConnectedTo(srcId, tgtId, direction, edgeTypes)) {
                // Connected: return the target as the single result
                final Document doc = (Document) targetElem.getRecord();
                downstream = List.of(new ResultInternal(doc)).iterator();
                return;
              } else {
                // Not connected: empty result
                downstream = Collections.emptyIterator();
                return;
              }
            }
          }
        }
      }

      downstream = executeTraversal(context, this.item, startingElem, 0, null).iterator();
    }
  }

  protected Iterable<ResultInternal> executeTraversal(final CommandContext iCommandContext, final MatchPathItem item,
      final Identifiable startingPoint, final int depth, final List<Identifiable> pathToHere) {

    WhereClause filter = null;
    WhereClause whileCondition = null;
    Integer maxDepth = null;
    String className = null;
    Integer clusterId = null;
    Rid targetRid = null;
    if (item.getFilter() != null) {
      filter = getTargetFilter(item);
      whileCondition = item.getFilter().getWhileCondition();
      maxDepth = item.getFilter().getMaxDepth();
      className = targetClassName(item, iCommandContext);
      final String clusterName = targetClusterName(item, iCommandContext);
      if (clusterName != null) {
        clusterId = iCommandContext.getDatabase().getSchema().getBucketByName(clusterName).getFileId();
      }
      targetRid = targetRid(item, iCommandContext);
    }

    final Iterable<ResultInternal> result;

    if (whileCondition == null && maxDepth == null) { // in this case starting point is not returned and only one level depth is
      // evaluated

      final Iterable<ResultInternal> queryResult = traversePatternEdge(startingPoint, iCommandContext);
      final WhereClause theFilter = filter;
      final String theClassName = className;
      final Integer theClusterId = clusterId;
      final Rid theTargetRid = targetRid;
      result = () -> {
        final Iterator<ResultInternal> iter = queryResult.iterator();

        return new Iterator() {

          private ResultInternal nextElement = null;

          @Override
          public boolean hasNext() {
            if (nextElement == null) {
              fetchNext();
            }
            return nextElement != null;
          }

          @Override
          public Object next() {
            if (nextElement == null) {
              fetchNext();
            }
            if (nextElement == null) {
              throw new NoSuchElementException();
            }
            final ResultInternal res = nextElement;
            nextElement = null;
            return res;
          }

          public void fetchNext() {
            final Object previousMatch = iCommandContext.getVariable("currentMatch");
            // $matched IS THE PARTIAL MATCH THIS HOP STARTS FROM, NOT THE ROW THE STEP LAST EMITTED (ISSUE #7434)
            final Object previousMatched = iCommandContext.getVariable(MatchBindMatchedStep.MATCHED_VARIABLE);
            iCommandContext.setVariable(MatchBindMatchedStep.MATCHED_VARIABLE, sourceRecord);
            try {
              while (iter.hasNext()) {
                final ResultInternal next = iter.next();
                final Document elem = next.toElement();
                iCommandContext.setVariable("currentMatch", elem);
                if (matchesFilters(iCommandContext, theFilter, elem) && matchesClass(theClassName, elem) && matchesCluster(
                    theClusterId, elem) && matchesRid(iCommandContext, theTargetRid, elem)) {
                  nextElement = next;
                  break;
                }
              }
            } finally {
              // THE FILTER IS ARBITRARY SQL: RESTORE EVEN WHEN IT THROWS, OR THE CONTEXT KEEPS THIS HOP'S BINDINGS
              iCommandContext.setVariable("currentMatch", previousMatch);
              iCommandContext.setVariable(MatchBindMatchedStep.MATCHED_VARIABLE, previousMatched);
            }
          }
        };
      };

    } else { // in this case also zero level (starting point) is considered and traversal depth is
      // given by the while condition

      // A null starting point means a previous optional segment produced no match: there is nothing to traverse from and no
      // record to evaluate the zero-depth filters against. Return empty (as the non-while branch does) so the optional handling
      // preserves the base row with null aliases instead of hitting an NPE while resolving filters such as @rid. Issue #4919.
      if (startingPoint == null)
        return Collections.emptyList();

      // STREAMED, NOT COLLECTED: THE NUMBER OF PATHS IS EXPONENTIAL IN THE DEPTH, SO A LIST OF THEM IS UNBOUNDED BY ANYTHING
      // BUT THE HEAP, AND LIMIT COULD NOT STOP IT (ISSUE #11501)
      final WhereClause theFilter = filter;
      final WhereClause theWhileCondition = whileCondition;
      final Integer theMaxDepth = maxDepth;
      final String theClassName = className;
      final Integer theClusterId = clusterId;
      final Rid theTargetRid = targetRid;
      result = () -> new DepthFirstPaths(iCommandContext, startingPoint, depth, pathToHere, theFilter, theWhileCondition,
          theMaxDepth, theClassName, theClusterId, theTargetRid);
    }
    return result;
  }

  /**
   * The paths of a {@code while:}/{@code maxDepth:} item, produced one at a time in depth-first pre-order: a vertex, then the
   * paths through each of its neighbours in the order the pattern edge returns them. The order and the {@code $depth} /
   * {@code $matchPath} of every path are the ones the recursive walk this replaces collected into a list (issue #11501).
   * <p>
   * The walk keeps an explicit stack, one frame per vertex of the current path holding the neighbours it has still to
   * descend into, so what it holds is the current path and its pending neighbours, not every path met: {@code LIMIT} stops
   * it, and a cycle under {@code while: (true)} is an endless stream rather than a {@code StackOverflowError}. What the stack
   * holds is counted against {@link GlobalConfiguration#QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP} and charged to the query's
   * heap budget, and the command deadline is checked at every vertex reached, since a filter that rejects every vertex keeps
   * the whole walk inside a single {@link #hasNext()}.
   */
  private final class DepthFirstPaths implements Iterator<ResultInternal> {
    /**
     * What one vertex held by the stack costs the heap: the {@link ResultInternal} the pattern edge wrapped it in, its slot
     * in the neighbour list, and the header of the vertex record it points at.
     */
    private static final int ENTRY_BYTES = 96;

    private final CommandContext       context;
    private final List<Identifiable>   pathToHere;
    private final int                  startDepth;
    private final WhereClause          filter;
    private final WhereClause          whileCondition;
    private final Integer              maxDepth;
    private final String               className;
    private final Integer              clusterId;
    private final Rid                  targetRid;
    private final WorkGuard            guard;
    private       OperationHeapLimit   heapLimit;
    /** The vertex of each frame: frame {@code i} is at depth {@code startDepth + i}. */
    private       Identifiable[]       vertices  = new Identifiable[8];
    /** The neighbours of each frame's vertex still to descend into, {@code null} when the vertex is not expanded. */
    private       Iterator<ResultInternal>[] neighbours = new Iterator[8];
    private       int                  size;
    /** Vertices the stack holds: one per frame plus the neighbours still pending in it. */
    private       long                 held;
    private       Identifiable         start;
    private       ResultInternal       nextPath;

    private DepthFirstPaths(final CommandContext context, final Identifiable start, final int startDepth,
        final List<Identifiable> pathToHere, final WhereClause filter, final WhereClause whileCondition, final Integer maxDepth,
        final String className, final Integer clusterId, final Rid targetRid) {
      this.context = context;
      this.start = start;
      this.startDepth = startDepth;
      this.pathToHere = pathToHere;
      this.filter = filter;
      this.whileCondition = whileCondition;
      this.maxDepth = maxDepth;
      this.className = className;
      this.clusterId = clusterId;
      this.targetRid = targetRid;
      this.guard = WorkGuard.forCommandDeadline(context);
    }

    @Override
    public boolean hasNext() {
      if (nextPath == null)
        fetchNext();
      return nextPath != null;
    }

    @Override
    public ResultInternal next() {
      if (!hasNext())
        throw new NoSuchElementException();
      final ResultInternal path = nextPath;
      nextPath = null;
      return path;
    }

    private void fetchNext() {
      try {
        while (nextPath == null) {
          if (start != null) {
            final Identifiable first = start;
            start = null;
            nextPath = visit(first);
            continue;
          }

          if (size == 0)
            return;

          final Iterator<ResultInternal> pending = neighbours[size - 1];
          if (pending != null && pending.hasNext()) {
            final ResultInternal neighbour = pending.next();
            hold(-1);
            nextPath = visit(neighbour.toElement());
          } else
            pop();
        }
      } catch (final RuntimeException | Error e) {
        // THE WALK IS ABANDONED: GIVE THE HEAP BACK NOW RATHER THAN WHEN THE PLAN IS COLLECTED
        release();
        throw e;
      }
    }

    /**
     * Pushes {@code vertex} as the frame one level below the current top, expanding it when the depth bound and the
     * {@code while:} condition allow, and returns its path when it passes the item's filters.
     */
    private ResultInternal visit(final Identifiable vertex) {
      guard.check();

      final int depth = startDepth + size;
      final Object previousDepth = context.getVariable("depth");
      final Object previousMatch = context.getVariable("currentMatch");
      final Object previousMatched = context.getVariable(MatchBindMatchedStep.MATCHED_VARIABLE);
      context.setVariable("depth", depth);
      context.setVariable("currentMatch", vertex);
      context.setVariable(MatchBindMatchedStep.MATCHED_VARIABLE, sourceRecord);
      try {
        ResultInternal path = null;
        if (matchesFilters(context, filter, vertex) && matchesClass(className, vertex) && matchesCluster(clusterId, vertex)
            && matchesRid(context, targetRid, vertex)) {
          path = new ResultInternal(vertex.getRecord());
          path.setMetadata("$depth", depth);
          path.setMetadata("$matchPath", pathTo(vertex));
        }

        Iterator<ResultInternal> pending = null;
        if ((maxDepth == null || depth < maxDepth) && (whileCondition == null || whileCondition.matchesFilters(vertex, context))) {
          final Iterable<ResultInternal> expanded = traversePatternEdge(vertex, context);
          final Collection<ResultInternal> candidates;
          if (expanded instanceof Collection<ResultInternal> collection)
            candidates = collection;
          else {
            candidates = new ArrayList<>();
            for (final ResultInternal candidate : expanded)
              candidates.add(candidate);
          }
          if (!candidates.isEmpty()) {
            hold(candidates.size());
            pending = candidates.iterator();
          }
        }

        push(vertex, pending);
        return path;
      } finally {
        context.setVariable("depth", previousDepth);
        context.setVariable("currentMatch", previousMatch);
        context.setVariable(MatchBindMatchedStep.MATCHED_VARIABLE, previousMatched);
      }
    }

    /**
     * The vertices from the item's starting point to {@code vertex}, the starting point excluded and {@code vertex}
     * included, after {@link #pathToHere}: the vertices of frames 1..top, then {@code vertex}, which is not pushed yet.
     */
    private List<Identifiable> pathTo(final Identifiable vertex) {
      if (size == 0)
        return pathToHere == null ? Collections.emptyList() : pathToHere;

      final List<Identifiable> path = new ArrayList<>((pathToHere == null ? 0 : pathToHere.size()) + size);
      if (pathToHere != null)
        path.addAll(pathToHere);
      for (int i = 1; i < size; i++)
        path.add(vertices[i].getIdentity());
      path.add(vertex.getIdentity());
      return path;
    }

    private void push(final Identifiable vertex, final Iterator<ResultInternal> pending) {
      if (size == vertices.length) {
        vertices = Arrays.copyOf(vertices, size * 2);
        neighbours = Arrays.copyOf(neighbours, size * 2);
      }
      vertices[size] = vertex;
      neighbours[size] = pending;
      ++size;
      hold(1);
    }

    private void pop() {
      --size;
      vertices[size] = null;
      neighbours[size] = null;
      hold(-1);
      if (size == 0)
        // EXHAUSTED: NOTHING IS HELD ANY MORE
        release();
    }

    /** Accounts {@code delta} more vertices held by the stack against the element cap and the query's heap budget. */
    private void hold(final long delta) {
      if (delta == 0)
        return;
      if (heapLimit == null)
        heapLimit = OperationHeapLimit.of(context, "vertices", "MATCH while/maxDepth traversal");
      held += delta;
      if (delta > 0) {
        heapLimit.check(held);
        heapLimit.charge(delta * ENTRY_BYTES);
      } else
        heapLimit.release(-delta * ENTRY_BYTES);
    }

    private void release() {
      if (heapLimit != null)
        heapLimit.release();
    }
  }

  protected WhereClause getTargetFilter(final MatchPathItem item) {
    return item.getFilter().getFilter();
  }

  protected String targetClassName(final MatchPathItem item, final CommandContext iCommandContext) {
    return item.getFilter().getTypeName(iCommandContext);
  }

  protected String targetClusterName(final MatchPathItem item, final CommandContext iCommandContext) {
    return item.getFilter().getBucketName(iCommandContext);
  }

  protected Rid targetRid(final MatchPathItem item, final CommandContext iCommandContext) {
    return item.getFilter().getRid(iCommandContext);
  }

  private boolean matchesClass(final String className, final Identifiable origin) {
    if (className == null) {
      return true;
    }
    Document element = null;
    if (origin instanceof Document document) {
      element = document;
    } else {
      final Object record = origin.getRecord();
      if (record instanceof Document document) {
        element = document;
      }
    }
    if (element != null) {
      final DocumentType typez = element.getType();
      if (typez == null)
        return false;

      return typez.isSubTypeOf(className);
    }
    return false;
  }

  private boolean matchesCluster(final Integer bucketId, final Identifiable origin) {
    if (bucketId == null)
      return true;

    if (origin == null)
      return false;

    if (origin.getIdentity() == null)
      return false;

    return bucketId.equals(origin.getIdentity().getBucketId());
  }

  private boolean matchesRid(final CommandContext iCommandContext, final Rid rid, final Identifiable origin) {
    if (rid == null)
      return true;

    if (origin == null)
      return false;

    if (origin.getIdentity() == null)
      return false;

    return origin.getIdentity().equals(rid.toRecordId(origin, iCommandContext));
  }

  protected boolean matchesFilters(final CommandContext iCommandContext, final WhereClause filter, final Identifiable origin) {
    return filter == null || filter.matchesFilters(origin, iCommandContext);
  }

  //TODO refactor this method to receive the item.

  protected Iterable<ResultInternal> traversePatternEdge(final Identifiable startingPoint, final CommandContext iCommandContext) {

    Iterable possibleResults = null;
    if (this.item.getFilter() != null) {
      final String alias = getEndpointAlias();
      final Object matchedNodes = iCommandContext.getVariable(MatchPrefetchStep.PREFETCHED_MATCH_ALIAS_PREFIX + alias);
      if (matchedNodes != null) {
        if (matchedNodes instanceof Iterable iterable) {
          possibleResults = iterable;
        } else {
          possibleResults = Set.of(matchedNodes);
        }
      }
    }

    final Object prevCurrent = iCommandContext.getVariable("current");
    iCommandContext.setVariable("current", startingPoint);
    Object qR;
    try {
      final Iterable candidates = possibleResults;
      qR = IncomingEdgeLookup.walkingPattern(() -> this.item.getMethod().execute(startingPoint, candidates, iCommandContext));
    } finally {
      iCommandContext.setVariable("current", prevCurrent);
    }

    if (qR == null) {
      return Collections.emptyList();
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

}
