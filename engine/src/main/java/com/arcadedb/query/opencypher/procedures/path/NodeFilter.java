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
package com.arcadedb.query.opencypher.procedures.path;

import com.arcadedb.database.RID;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.utility.RidHashSet;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * The node lists of the {@code path.*} procedures, in APOC's terms: {@code endNodes}, {@code terminatorNodes},
 * {@code allowlistNodes} (also {@code whitelistNodes}) and {@code denylistNodes} (also {@code blacklistNodes}). They are the
 * operators {@code >}, {@code /}, {@code +} and {@code -} of the label filter, naming nodes instead of labels:
 * <ul>
 *   <li>{@code denylistNodes}: no node of the path may be one of them;</li>
 *   <li>{@code terminatorNodes}: only paths ending at one of them are returned, and the walk stops there;</li>
 *   <li>{@code endNodes}: only paths ending at one of them are returned, and the walk goes on past it;</li>
 *   <li>{@code allowlistNodes}: every node of the path must be one of them, the end and terminator nodes being allowed too.</li>
 * </ul>
 * The start node is left out of the denylist and the allowlist, and cannot be an end or terminator node, unless
 * {@code filterStartNode} asks for it. A node closer to the start than {@code minLevel} is never an end or a terminator
 * node, and so does not stop the walk.
 * <p>
 * Each list answers like an evaluator of APOC's traversal, and the answers are combined with the label filter's - and with
 * each other - the way those are: a node is an answer only if every filter includes it, and the walk goes on past it only if
 * every filter lets it. {@link Verdict} carries both. That is {@link Filters#evaluate}'s job; this class answers for the lists.
 * <p>
 * The lists are sets of RIDs, tested by RID and so without loading a record. A list may be given as a collection of nodes, or
 * as a single node; a list that is empty, or null, filters nothing.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class NodeFilter {
  /** No list: the shared instance is safe because it holds no set. */
  static final NodeFilter NONE = new NodeFilter(null, null, null, null, false, 0);

  private final RidHashSet end;
  private final RidHashSet terminator;
  // The allowlist, completed with the end and terminator nodes: APOC allows them even when the list does not name them
  private final RidHashSet allow;
  private final RidHashSet deny;
  private final boolean    filterStartNode;
  private final int        minLevel;

  private NodeFilter(final RidHashSet end, final RidHashSet terminator, final RidHashSet allow, final RidHashSet deny,
      final boolean filterStartNode, final int minLevel) {
    this.end = end;
    this.terminator = terminator;
    this.allow = allow;
    this.deny = deny;
    this.filterStartNode = filterStartNode;
    this.minLevel = minLevel;
  }

  /**
   * Reads the four lists out of a procedure's configuration map.
   *
   * @param filterStartNode whether the lists apply to the start node too (APOC's {@code filterStartNode})
   * @param minLevel        the walk's minimum level
   *
   * @throws IllegalArgumentException when a list holds something that is not a node
   */
  static NodeFilter parse(final String procedureName, final Map<String, Object> config, final boolean filterStartNode,
      final int minLevel) {
    final List<RID> end = nodes(procedureName, config, "endNodes");
    final List<RID> terminator = nodes(procedureName, config, "terminatorNodes");
    List<RID> allow = nodes(procedureName, config, "allowlistNodes");
    if (allow == null)
      allow = nodes(procedureName, config, "whitelistNodes");
    List<RID> deny = nodes(procedureName, config, "denylistNodes");
    if (deny == null)
      deny = nodes(procedureName, config, "blacklistNodes");

    if (end == null && terminator == null && allow == null && deny == null)
      return NONE;

    final RidHashSet allowed = allow != null ? toSet(allow) : null;
    if (allowed != null) {
      if (end != null)
        addAll(allowed, end);
      if (terminator != null)
        addAll(allowed, terminator);
    }
    return new NodeFilter(end != null ? toSet(end) : null, terminator != null ? toSet(terminator) : null, allowed,
        deny != null ? toSet(deny) : null, filterStartNode, minLevel);
  }

  boolean isEmpty() {
    return this == NONE;
  }

  /** The verdict of the lists on the node {@code id} names, reached {@code depth} hops from the start node. */
  LabelFilter.Verdict evaluate(final RID id, final int depth) {
    final boolean unfiltered = depth == 0 && !filterStartNode;
    if (!unfiltered) {
      if (deny != null && deny.contains(id))
        return LabelFilter.Verdict.EXCLUDE_AND_PRUNE;
      if (allow != null && !allow.contains(id))
        return LabelFilter.Verdict.EXCLUDE_AND_PRUNE;
    }

    if (end == null && terminator == null)
      return LabelFilter.Verdict.INCLUDE_AND_CONTINUE;
    if (unfiltered || depth < minLevel)
      return LabelFilter.Verdict.EXCLUDE_AND_CONTINUE;

    final boolean terminates = terminator != null && terminator.contains(id);
    final boolean includes = terminates || (end != null && end.contains(id));
    return LabelFilter.Verdict.of(includes, !terminates);
  }

  private static RidHashSet toSet(final List<RID> nodes) {
    final RidHashSet set = new RidHashSet(Math.max(16, nodes.size() * 2));
    addAll(set, nodes);
    return set;
  }

  private static void addAll(final RidHashSet target, final List<RID> source) {
    for (final RID rid : source)
      target.add(rid);
  }

  /** The nodes a list names, or null when it names none. */
  private static List<RID> nodes(final String procedureName, final Map<String, Object> config, final String key) {
    final Object value = config.get(key);
    if (value == null)
      return null;

    final List<RID> nodes = new ArrayList<>();
    if (value instanceof Collection<?> collection) {
      for (final Object element : collection)
        nodes.add(nodeId(procedureName, key, element, "must contain nodes"));
    } else
      nodes.add(nodeId(procedureName, key, value, "must be a list of nodes"));
    return nodes.isEmpty() ? null : nodes;
  }

  private static RID nodeId(final String procedureName, final String key, final Object element, final String requirement) {
    if (element instanceof Vertex vertex)
      return vertex.getIdentity();
    if (element instanceof RID rid)
      return rid;
    if (element instanceof Result result && result.isVertex())
      return result.getVertex().get().getIdentity();
    throw new IllegalArgumentException(procedureName + "(): " + key + " " + requirement + ", got "
        + (element == null ? "null" : element.getClass().getSimpleName()));
  }
}
