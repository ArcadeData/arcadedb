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
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.EdgeIdentityMultiset;
import com.arcadedb.graph.EdgeIdentitySet;
import com.arcadedb.graph.LightEdgeRID;
import com.arcadedb.utility.RidHashSet;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * The {@code uniqueness} option of the {@code path.*} procedures, with the nine values of APOC's: what a walk may visit more
 * than once. A walk records the node, or the relationship, a branch reaches the moment it creates the branch - before the
 * label and node filters have their say - so an entity a filter turns away is spent all the same, as in APOC.
 * <ul>
 *   <li>{@link #NODE_GLOBAL}, {@link #RELATIONSHIP_GLOBAL}: once in the whole walk;</li>
 *   <li>{@link #NODE_LEVEL}, {@link #RELATIONSHIP_LEVEL}: once at each distance from the start;</li>
 *   <li>{@link #NODE_PATH}, {@link #RELATIONSHIP_PATH}: once in a path, which is every node, or every trail, there is;</li>
 *   <li>{@link #NODE_RECENT}, {@link #RELATIONSHIP_RECENT}: not again while still among the last {@value #RECENT_WINDOW}
 *   visited;</li>
 *   <li>{@link #NONE}: anything goes.</li>
 * </ul>
 * Whatever the value, a walk never leaves a node by the relationship it arrived by: APOC's expansion drops it, which is why
 * {@link #NONE} does not bounce between two nodes.
 * <p>
 * {@code path.expandConfig} and {@code path.expand} default to {@link #RELATIONSHIP_PATH}, as APOC's do. The walks of
 * {@code path.spanningTree}, {@code path.subgraphNodes} and {@code path.subgraphAll} are {@link #NODE_GLOBAL} whatever is
 * asked, again as APOC's: the option is read and checked there, and does not change the answer.
 * <p>
 * An {@link Admission} is one walk's state, so it is created per call and never shared.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
enum PathUniqueness {
  NODE_GLOBAL(false, Scope.GLOBAL),
  NODE_LEVEL(false, Scope.LEVEL),
  NODE_PATH(false, Scope.PATH),
  NODE_RECENT(false, Scope.RECENT),
  RELATIONSHIP_GLOBAL(true, Scope.GLOBAL),
  RELATIONSHIP_LEVEL(true, Scope.LEVEL),
  RELATIONSHIP_PATH(true, Scope.PATH),
  RELATIONSHIP_RECENT(true, Scope.RECENT),
  NONE(false, null);

  /** How many of the latest visited entities the {@code RECENT} values remember, measured against APOC 5.26. */
  static final int RECENT_WINDOW = 10_000;

  private enum Scope {
    GLOBAL, LEVEL, PATH, RECENT
  }

  private static final String VALUES;

  static {
    final StringBuilder values = new StringBuilder();
    for (final PathUniqueness uniqueness : values())
      values.append(values.isEmpty() ? "" : ", ").append(uniqueness.name());
    VALUES = values.toString();
  }

  private final boolean relationships;
  private final Scope   scope;

  PathUniqueness(final boolean relationships, final Scope scope) {
    this.relationships = relationships;
    this.scope = scope;
  }

  /**
   * Reads the option, case-insensitively as APOC does. APOC falls back to its default for a value it does not know; a walk
   * that quietly answers something other than what was asked is worse than a refusal, so this refuses it.
   */
  static PathUniqueness parse(final String procedureName, final Object value, final PathUniqueness defaultValue) {
    if (value == null)
      return defaultValue;
    if (!(value instanceof String text))
      throw new IllegalArgumentException(procedureName + "(): uniqueness must be a string, got " + value.getClass().getSimpleName());

    final String name = text.trim();
    for (final PathUniqueness uniqueness : values())
      if (uniqueness.name().equalsIgnoreCase(name))
        return uniqueness;
    throw new IllegalArgumentException(procedureName + "(): uniqueness '" + text + "' is not one of " + VALUES);
  }

  /**
   * The state of one walk.
   *
   * @param depthFirst whether the walk is depth-first: it then backs out of branches, and gives them back through
   *                   {@link Admission#release}
   * @param startNode  the node the walk starts from, which counts as visited by the node values; null for none
   */
  Admission newAdmission(final boolean depthFirst, final RID startNode) {
    if (scope == null)
      return new Admission();
    return switch (scope) {
      case GLOBAL -> new Global(relationships, startNode);
      case LEVEL -> new Level(relationships, startNode);
      case PATH -> depthFirst ? new PathStack(relationships, startNode) : new PathScan(relationships);
      case RECENT -> new Recent(relationships, startNode);
    };
  }

  /** Whether the entity a branch is identified by is the relationship or the node it reaches. */
  private static RID keyOf(final boolean relationships, final Edge edge, final RID neighborId) {
    return relationships ? edge.getIdentity() : neighborId;
  }

  /** The base state, which lets every branch through: {@link #NONE}. */
  static class Admission {
    /**
     * Whether the walk may create the branch that reaches {@code neighborId} through {@code edge}, {@code depth} hops from
     * the start; when it may, the branch is recorded.
     *
     * @param parent the branch the new one extends
     */
    boolean admit(final PathExpansion.Branch parent, final Edge edge, final RID neighborId, final int depth) {
      return true;
    }

    /** A depth-first walk backs out of a branch it created: the values scoped to a path forget it. */
    void release(final Edge edge, final RID neighborId) {
    }
  }

  /** What the global and level values keep: a set of nodes, or of relationships, whichever the value is about. */
  private interface Seen {
    boolean add(RID key);
  }

  private static Seen newSeen(final boolean relationships) {
    if (relationships) {
      final EdgeIdentitySet edges = new EdgeIdentitySet();
      return edges::add;
    }
    final RidHashSet nodes = new RidHashSet();
    return nodes::add;
  }

  private static final class Global extends Admission {
    private final boolean relationships;
    private final Seen    seen;

    Global(final boolean relationships, final RID startNode) {
      this.relationships = relationships;
      this.seen = newSeen(relationships);
      if (!relationships && startNode != null)
        seen.add(startNode);
    }

    @Override
    boolean admit(final PathExpansion.Branch parent, final Edge edge, final RID neighborId, final int depth) {
      return seen.add(keyOf(relationships, edge, neighborId));
    }
  }

  private static final class Level extends Admission {
    private final boolean    relationships;
    private final List<Seen> levels = new ArrayList<>();

    Level(final boolean relationships, final RID startNode) {
      this.relationships = relationships;
      if (!relationships && startNode != null)
        levelOf(0).add(startNode);
    }

    private Seen levelOf(final int depth) {
      while (levels.size() <= depth)
        levels.add(newSeen(relationships));
      return levels.get(depth);
    }

    @Override
    boolean admit(final PathExpansion.Branch parent, final Edge edge, final RID neighborId, final int depth) {
      return levelOf(depth).add(keyOf(relationships, edge, neighborId));
    }
  }

  /** A breadth-first walk holds many paths at once, so a path is checked by looking along it, branch by branch. */
  private static final class PathScan extends Admission {
    private final boolean relationships;

    PathScan(final boolean relationships) {
      this.relationships = relationships;
    }

    @Override
    boolean admit(final PathExpansion.Branch parent, final Edge edge, final RID neighborId, final int depth) {
      if (relationships) {
        final RID edgeId = edge.getIdentity();
        for (PathExpansion.Branch branch = parent; branch.edgeId != null; branch = branch.parent)
          if (LightEdgeRID.isSameEdge(branch.edgeId, edgeId))
            return false;
        return true;
      }
      for (PathExpansion.Branch branch = parent; branch != null; branch = branch.parent)
        if (branch.vertexId.equals(neighborId))
          return false;
      return true;
    }
  }

  /**
   * A depth-first walk holds the one path it is on, and takes entities in and out of it as it goes. The set is not a
   * primitive one: nothing in the engine removes from those, and a path is as long as the walk is deep.
   */
  private static final class PathStack extends Admission {
    private final boolean               relationships;
    private final Set<RID>              nodes;
    private final EdgeIdentityMultiset  edges;

    PathStack(final boolean relationships, final RID startNode) {
      this.relationships = relationships;
      if (relationships) {
        nodes = null;
        edges = new EdgeIdentityMultiset();
      } else {
        nodes = new HashSet<>();
        edges = null;
        if (startNode != null)
          nodes.add(startNode);
      }
    }

    @Override
    boolean admit(final PathExpansion.Branch parent, final Edge edge, final RID neighborId, final int depth) {
      if (relationships) {
        final RID edgeId = edge.getIdentity();
        if (edges.contains(edgeId))
          return false;
        edges.add(edgeId);
        return true;
      }
      return nodes.add(neighborId);
    }

    @Override
    void release(final Edge edge, final RID neighborId) {
      if (relationships)
        edges.remove(edge.getIdentity());
      else
        nodes.remove(neighborId);
    }
  }

  /** The last {@link #RECENT_WINDOW} entities admitted: one that is not among them again is new. */
  private static final class Recent extends Admission {
    private final boolean              relationships;
    private final EdgeIdentityMultiset window = new EdgeIdentityMultiset();
    private final ArrayDeque<RID>      order  = new ArrayDeque<>();

    Recent(final boolean relationships, final RID startNode) {
      this.relationships = relationships;
      if (!relationships && startNode != null)
        remember(startNode);
    }

    private void remember(final RID key) {
      window.add(key);
      order.addLast(key);
      if (order.size() > RECENT_WINDOW)
        window.remove(order.removeFirst());
    }

    @Override
    boolean admit(final PathExpansion.Branch parent, final Edge edge, final RID neighborId, final int depth) {
      final RID key = keyOf(relationships, edge, neighborId);
      if (window.contains(key))
        return false;
      remember(key);
      return true;
    }
  }
}
