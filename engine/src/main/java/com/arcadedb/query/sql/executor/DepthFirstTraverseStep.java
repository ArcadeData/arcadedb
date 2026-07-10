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
import com.arcadedb.database.RID;
import com.arcadedb.query.sql.parser.PInteger;
import com.arcadedb.query.sql.parser.TraverseProjectionItem;
import com.arcadedb.query.sql.parser.WhereClause;
import com.arcadedb.utility.RidHashSet;

import java.util.*;

import static com.arcadedb.schema.Property.RID_PROPERTY;

/**
 * Created by luigidellaquila on 26/10/16.
 */
public class DepthFirstTraverseStep extends AbstractTraverseStep {

  // Relaxation state so TRAVERSE ... MAXDEPTH d returns the correct d-hop ball. A depth-first walk can reach a node through a long path before a short one; the
  // legacy code recorded only that first-arrival depth and gated subtree expansion on it, so when MAXDEPTH fell between the two path lengths a whole subtree was
  // wrongly pruned and the result became non-monotonic in d (issue #5159). We instead keep the smallest depth seen per node and re-expand it whenever a strictly
  // shorter path arrives, so every node reachable within d hops is expanded. Nodes are still emitted once. On single-path graphs no relaxation fires, so the walk
  // is byte-for-byte identical to before.
  private final Map<RID, Integer> bestDepthByRid = new HashMap<>();
  private final RidHashSet        emitted        = new RidHashSet();

  public DepthFirstTraverseStep(final List<TraverseProjectionItem> projections, final WhereClause whileClause, final PInteger maxDepth,
      final CommandContext context) {
    this(projections, whileClause, null, maxDepth, context);
  }

  public DepthFirstTraverseStep(final List<TraverseProjectionItem> projections, final WhereClause whileClause, final WhereClause postFilter,
      final PInteger maxDepth, final CommandContext context) {
    super(projections, whileClause, postFilter, maxDepth, context);
  }

  @Override
  protected void fetchNextEntryPoints(final CommandContext context, final int nRecords) {
    final ResultSet nextN = getPrev().syncPull(context, nRecords);
    while (nextN.hasNext()) {
      final Result item = toTraverseResult(nextN.next());
      if (item == null)
        continue;

      ((ResultInternal) item).setMetadata("$depth", 0);

      final List stack = new ArrayList();
      item.getIdentity().ifPresent(x -> stack.add(x));
      ((ResultInternal) item).setMetadata("$stack", stack);

      final List<Identifiable> path = new ArrayList<>();
      if (item.getIdentity().isPresent()) {
        path.add(item.getIdentity().get());
      } else if (item.getProperty(RID_PROPERTY) != null) {
        path.add(item.getProperty(RID_PROPERTY));
      }
      ((ResultInternal) item).setMetadata("$path", path);

      if (item.isElement() && !traversed.contains(item.getElement().get().getIdentity())) {
        final RID rid = item.getElement().get().getIdentity();
        tryAddEntryPointAtTheEnd(item, context);
        traversed.add(rid);
        bestDepthByRid.put(rid, 0);
      } else if (item.getProperty(RID_PROPERTY) instanceof Identifiable) {
        final RID rid = ((Identifiable) item.getProperty(RID_PROPERTY)).getIdentity();
        tryAddEntryPointAtTheEnd(item, context);
        traversed.add(rid);
        bestDepthByRid.put(rid, 0);
      }
    }
  }

  private Result toTraverseResult(final Result item) {
    TraverseResult res = null;
    if (item instanceof TraverseResult result)
      res = result;
    else if (item.isElement() && item.getElement().get().getIdentity() != null) {
      res = new TraverseResult(item.getElement().get());
      res.depth = 0;
    } else if (item.getPropertyNames().size() == 1) {
      final Object val = item.getProperty(item.getPropertyNames().iterator().next());
      if (val instanceof Identifiable identifiable) {
        res = new TraverseResult((Document) identifiable.getRecord());
        res.depth = 0;
        res.setMetadata("$depth", 0);
      }
    } else {
      res = new TraverseResult();
      for (final String key : item.getPropertyNames())
        res.setProperty(key, item.getProperty(key));

      for (final String md : item.getMetadataKeys())
        res.setMetadata(md, item.getMetadata(md));
    }

    return res;
  }

  @Override
  protected void fetchNextResults(final CommandContext context, final int nRecords) {
    if (!this.entryPoints.isEmpty()) {
      final TraverseResult item = (TraverseResult) this.entryPoints.removeFirst();
      final Integer depth = item.depth != null ? item.depth : (Integer) item.getMetadata("$depth");
      final RID rid = item.getIdentity().orElse(null);

      // A strictly shorter path to this node was found after this copy was queued: a better-depth copy is (or was) in the queue, so this one is stale. Skipping it
      // avoids both a duplicate emission and a redundant (deeper) expansion; the better copy carries the correct depth and re-expands the subtree.
      if (rid != null) {
        final Integer best = bestDepthByRid.get(rid);
        if (best != null && depth > best)
          return;
      }

      // Emit each node at most once (its first non-stale visit). Non-element results carry no RID and keep the historical emit-always behavior.
      if ((rid == null || emitted.add(rid)) && (postFilter == null || postFilter.matchesFilters(item, context)))
        this.results.add(item);

      for (final TraverseProjectionItem proj : projections) {
        final Object nextStep = proj.execute(item, context);
        if (this.maxDepth == null || this.maxDepth.getValue().intValue() > depth)
          addNextEntryPoints(nextStep, depth + 1, (List) item.getMetadata("$path"), (List) item.getMetadata("$stack"), context);
      }
    }
  }

  private void addNextEntryPoints(final Object nextStep, final int depth, final List<Identifiable> path, final List<Identifiable> stack,
      final CommandContext context) {
    switch (nextStep) {
      case Identifiable identifiable -> addNextEntryPoint(identifiable, depth, path, stack, context);
      case Iterable iterable -> addNextEntryPoints(iterable.iterator(), depth, path, stack, context);
      case Map map -> addNextEntryPoints(map.values().iterator(), depth, path, stack, context);
      case Result result -> addNextEntryPoint(result, depth, path, stack, context);
      case null, default -> {}
    }
  }

  private void addNextEntryPoints(final Iterator nextStep, final int depth, final List<Identifiable> path, final List<Identifiable> stack,
      final CommandContext context) {
    while (nextStep.hasNext())
      addNextEntryPoints(nextStep.next(), depth, path, stack, context);
  }

  private void addNextEntryPoint(final Identifiable nextStep, final int depth, final List<Identifiable> path, final List<Identifiable> stack,
      final CommandContext context) {
    if (!improvesDepth(nextStep.getIdentity(), depth))
      return;

    final TraverseResult res = new TraverseResult((Document) nextStep);
    res.depth = depth;
    res.setMetadata("$depth", depth);

    final List<Identifiable> newPath = new ArrayList<>(path);
    newPath.add(res.getIdentity().get());
    res.setMetadata("$path", newPath);

    final List newStack = new ArrayList();
    newStack.add(res.getIdentity().get());
    newStack.addAll(stack);
    //    for (int i = 0; i < newPath.size(); i++) {
    //      newStack.offerLast(newPath.get(i));
    //    }
    res.setMetadata("$stack", newStack);

    tryAddEntryPoint(res, context);
  }

  private void addNextEntryPoint(final Result nextStep, final int depth, final List<Identifiable> path, final List<Identifiable> stack,
      final CommandContext context) {
    if (!nextStep.isElement())
      return;

    if (!improvesDepth(nextStep.getElement().get().getIdentity(), depth))
      return;

    if (nextStep instanceof TraverseResult result) {
      result.depth = depth;
      result.setMetadata("$depth", depth);
      final List<Identifiable> newPath = new ArrayList<>(path);
      nextStep.getIdentity().ifPresent(x -> newPath.add(x.getIdentity()));
      result.setMetadata("$path", newPath);

      final List reverseStack = new ArrayList(newPath);
      Collections.reverse(reverseStack);
      final List newStack = new ArrayList(reverseStack);
      result.setMetadata("$stack", newStack);

      tryAddEntryPoint(nextStep, context);
    } else {
      final TraverseResult res = new TraverseResult(nextStep.getElement().get());
      res.depth = depth;
      res.setMetadata("$depth", depth);
      final List<Identifiable> newPath = new ArrayList<>(path);
      nextStep.getIdentity().ifPresent(x -> newPath.add(x.getIdentity()));
      res.setMetadata("$path", newPath);

      final List reverseStack = new ArrayList(newPath);
      Collections.reverse(reverseStack);
      final List newStack = new ArrayList(reverseStack);
      res.setMetadata("$stack", newStack);
      tryAddEntryPoint(res, context);
    }
  }

  /**
   * Records {@code depth} as the new best (smallest) depth for {@code rid} when it strictly improves on any previously seen depth, returning true so the caller
   * queues the node for (re-)expansion. Returns false when a path of equal or smaller depth was already recorded, leaving the queue untouched. This relaxation is
   * what keeps MAXDEPTH monotonic: a node first reached through a long path is re-queued (and its subtree re-expanded) once a shorter path arrives.
   */
  private boolean improvesDepth(final RID rid, final int depth) {
    final Integer prev = bestDepthByRid.get(rid);
    if (prev != null && prev <= depth)
      return false;
    bestDepthByRid.put(rid, depth);
    return true;
  }

  private void tryAddEntryPoint(final Result res, final CommandContext context) {
    if (whileClause == null || whileClause.matchesFilters(res, context)) {
      this.entryPoints.addFirst(res);
    }

    if (res.isElement())
      traversed.add(res.getElement().get().getIdentity());
    else if (res.getProperty(RID_PROPERTY) instanceof Identifiable)
      traversed.add(((Identifiable) res.getProperty(RID_PROPERTY)).getIdentity());
  }

  private void tryAddEntryPointAtTheEnd(final Result res, final CommandContext context) {
    if (whileClause == null || whileClause.matchesFilters(res, context))
      this.entryPoints.add(res);

    if (res.isElement())
      traversed.add(res.getElement().get().getIdentity());
    else if (res.getProperty(RID_PROPERTY) instanceof Identifiable)
      traversed.add(((Identifiable) res.getProperty(RID_PROPERTY)).getIdentity());
  }

  @Override
  public String prettyPrint(final int depth, final int indent) {
    final String spaces = ExecutionStepInternal.getIndent(depth, indent);
    final StringBuilder result = new StringBuilder();
    result.append(spaces);
    result.append("+ DEPTH-FIRST TRAVERSE \n");
    result.append(spaces);
    result.append("  ").append(projections.toString());
    if (whileClause != null) {
      result.append("\n");
      result.append(spaces);
      result.append("WHILE ").append(whileClause);
    }
    return result.toString();
  }
}
