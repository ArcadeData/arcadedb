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

import com.arcadedb.database.Database;
import com.arcadedb.database.RID;
import com.arcadedb.query.opencypher.procedures.ProcedureSchemaTypes;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;

import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;

/**
 * The {@code labelFilter} of the {@code path.*} procedures, in APOC's syntax. Entries are separated by {@code |}, and each
 * one carries its own operator:
 * <ul>
 *   <li>{@code -Label} (denylist): no node of the path may carry the label;</li>
 *   <li>{@code /Label} (termination): only paths ending at a node with the label are returned, and the walk stops
 *   there;</li>
 *   <li>{@code >Label} (end node): only paths ending at a node with the label are returned, and the walk goes on past it
 *   when the node is allowed by the allowlist;</li>
 *   <li>{@code +Label}, or a label with no operator (allowlist): every node of the path must carry one of the labels.</li>
 * </ul>
 * They take precedence in that order, whatever order they are written in. {@code *} matches any label, and {@code A:B}
 * matches a node carrying both. A {@code ,} separates the filters of a repeating sequence, the one applied to a node being
 * picked by the node's depth. Every label must be a vertex type of the schema, and a node carries a label when its type
 * is, or extends, that vertex type - which is how a node with several labels is stored.
 * <p>
 * Before issue #10025 the operators were not understood: {@code '+Person'} was read as the label {@code "+Person"},
 * matched no node and answered nothing but the start node, and a label was compared with the node's type name, so a node
 * with two labels matched neither of them. The semantics below, the start-node rule included, were checked against
 * APOC 5.26 rather than read off its documentation.
 * <p>
 * The verdict is answered from the RID alone - a bucket belongs to exactly one type - and cached per type and step, so a
 * filter costs one schema lookup per neighbour and never loads a record it rejects.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class LabelFilter {
  /** What to do with a node reached by a walk: whether its path is an answer, and whether the walk goes on past it. */
  enum Verdict {
    INCLUDE_AND_CONTINUE(true, true),
    INCLUDE_AND_PRUNE(true, false),
    EXCLUDE_AND_CONTINUE(false, true),
    EXCLUDE_AND_PRUNE(false, false),
    /**
     * A termination node, resolved by {@link #evaluate} against the minimum level and never handed to a walk: below it
     * the walk goes on (APOC does the same), at or above it the path ends here.
     */
    TERMINATE(true, false);

    final boolean includes;
    final boolean continues;

    Verdict(final boolean includes, final boolean continues) {
      this.includes = includes;
      this.continues = continues;
    }

    /** The verdict that includes and continues as asked. Never {@link #TERMINATE}, which only {@link #evaluate} resolves. */
    static Verdict of(final boolean includes, final boolean continues) {
      if (includes)
        return continues ? INCLUDE_AND_CONTINUE : INCLUDE_AND_PRUNE;
      return continues ? EXCLUDE_AND_CONTINUE : EXCLUDE_AND_PRUNE;
    }

    /**
     * The verdict of two filters that both have their say, as the evaluators of an APOC traversal combine: a node is an
     * answer only if both include it, and the walk goes on past it only if both let it.
     */
    Verdict and(final Verdict other) {
      return of(includes && other.includes, continues && other.continues);
    }
  }

  /**
   * No filter: every node is part of the answer and the walk goes on past it. The one instance shared across calls, which
   * is safe only because it holds no steps and so no verdict cache: {@link #evaluate} returns before touching either.
   */
  static final LabelFilter NONE = new LabelFilter(null, false, true, 0);

  // A zero-length conjunction is the '*' wildcard: a conjunction of no labels holds for every node
  private static final String[] WILDCARD = new String[0];

  private final Step[]                         steps;
  private final boolean                        filterStartNode;
  private final boolean                        beginSequenceAtStart;
  private final int                            minLevel;
  private final boolean                        endNodesOnly;
  // Plain maps, no synchronization: a filter is parsed per call and walked by that call's thread only, so it must never
  // be cached across calls or shared by a parallel walk. Keyed by the type instance, which a single call cannot see
  // replaced: a schema change during the call would have to drop a type the walk is reading
  private final IdentityHashMap<DocumentType, Verdict>[] verdictsByType;

  @SuppressWarnings("unchecked")
  private LabelFilter(final Step[] steps, final boolean filterStartNode, final boolean beginSequenceAtStart, final int minLevel) {
    this.steps = steps;
    this.filterStartNode = filterStartNode;
    this.beginSequenceAtStart = beginSequenceAtStart;
    this.minLevel = minLevel;

    boolean endNodes = false;
    if (steps != null)
      for (final Step step : steps)
        endNodes |= step.termination.length > 0 || step.endNode.length > 0;
    this.endNodesOnly = endNodes;
    this.verdictsByType = steps != null ? new IdentityHashMap[steps.length] : null;
  }

  /**
   * Parses a filter given as a string (APOC's form) or as a list of strings, each of which is one entry of the single
   * step: the list form never forms a sequence, a comma inside one of its elements is part of a label name.
   *
   * @param filterStartNode      whether the filter applies to the start node too (APOC's {@code filterStartNode})
   * @param beginSequenceAtStart whether the first step of a sequence applies to the start node, or to the node one hop
   *                             away from it (APOC's {@code beginSequenceAtStart})
   * @param minLevel             the walk's minimum level: a termination node closer than that does not stop the walk
   */
  static LabelFilter parse(final String procedureName, final Object filter, final Schema schema, final boolean filterStartNode,
      final boolean beginSequenceAtStart, final int minLevel) {
    final List<String> steps = FilterSteps.split(procedureName, "labelFilter", filter);
    return steps == null ? NONE : ofSteps(procedureName, steps, schema, filterStartNode, beginSequenceAtStart, minLevel);
  }

  /** Builds the filter from its steps, one string per step: the {@code sequence} configuration hands them over already split. */
  static LabelFilter ofSteps(final String procedureName, final List<String> steps, final Schema schema,
      final boolean filterStartNode, final boolean beginSequenceAtStart, final int minLevel) {
    if (FilterSteps.isBlank(steps))
      return NONE;

    final Step[] parsed = new Step[steps.size()];
    for (int i = 0; i < parsed.length; i++)
      parsed[i] = Step.parse(procedureName, steps.get(i), schema);
    return new LabelFilter(parsed, filterStartNode, beginSequenceAtStart, minLevel);
  }

  /** Whether the filter lets every node through, so a walk can skip asking. */
  boolean isUnfiltered() {
    return steps == null;
  }

  /**
   * The verdict on the node {@code vertexId} names, reached at {@code depth} hops from the start node. Never answers
   * {@link Verdict#TERMINATE}.
   */
  Verdict evaluate(final Database database, final RID vertexId, final int depth) {
    if (steps == null)
      return Verdict.INCLUDE_AND_CONTINUE;

    // The start node is not filtered unless asked to be (and a sequence that begins one hop away has no step for it):
    // it is still an answer of its own, the zero-length path, unless the filter only accepts paths ending at end nodes
    if (depth == 0 && (!filterStartNode || !beginSequenceAtStart))
      return endNodesOnly ? Verdict.EXCLUDE_AND_CONTINUE : Verdict.INCLUDE_AND_CONTINUE;

    final DocumentType type = database.getSchema().getTypeByBucketId(vertexId.getBucketId());
    if (type == null)
      // A RID naming a bucket no type owns: an edge left pointing into a bucket that was dropped. There is no node to
      // report there, and the walk cannot go past it
      return Verdict.EXCLUDE_AND_PRUNE;

    final int stepIndex = (beginSequenceAtStart ? depth : depth - 1) % steps.length;
    IdentityHashMap<DocumentType, Verdict> cache = verdictsByType[stepIndex];
    if (cache == null)
      verdictsByType[stepIndex] = cache = new IdentityHashMap<>();

    Verdict verdict = cache.get(type);
    if (verdict == null) {
      verdict = steps[stepIndex].evaluate(type, endNodesOnly);
      cache.put(type, verdict);
    }

    if (verdict == Verdict.TERMINATE)
      return depth < minLevel ? Verdict.EXCLUDE_AND_CONTINUE : Verdict.INCLUDE_AND_PRUNE;
    return verdict;
  }

  /** The filter of one step of the sequence: each group is a list of alternatives, each alternative a conjunction of labels. */
  private static final class Step {
    final String[][] allowlist;
    final String[][] denylist;
    final String[][] termination;
    final String[][] endNode;

    private Step(final String[][] allowlist, final String[][] denylist, final String[][] termination, final String[][] endNode) {
      this.allowlist = allowlist;
      this.denylist = denylist;
      this.termination = termination;
      this.endNode = endNode;
    }

    static Step parse(final String procedureName, final String step, final Schema schema) {
      final List<String[]> allow = new ArrayList<>();
      final List<String[]> deny = new ArrayList<>();
      final List<String[]> terminate = new ArrayList<>();
      final List<String[]> end = new ArrayList<>();

      for (final String rawEntry : step.split("\\|")) {
        final String entry = rawEntry.trim();
        if (entry.isEmpty())
          continue;

        final List<String[]> group = switch (entry.charAt(0)) {
          case '-' -> deny;
          case '/' -> terminate;
          case '>' -> end;
          case '+' -> allow;
          default -> null;
        };
        final String labels = (group != null ? entry.substring(1) : entry).trim();
        if (labels.isEmpty())
          throw new IllegalArgumentException(procedureName + "(): label filter entry '" + entry + "' names no label");

        (group != null ? group : allow).add(parseConjunction(procedureName, labels, schema));
      }
      return new Step(toArray(allow), toArray(deny), toArray(terminate), toArray(end));
    }

    private static String[] parseConjunction(final String procedureName, final String labels, final Schema schema) {
      if ("*".equals(labels))
        return WILDCARD;
      final String[] names = labels.split(":", -1);
      for (int i = 0; i < names.length; i++) {
        names[i] = names[i].trim();
        if (names[i].isEmpty())
          throw new IllegalArgumentException(procedureName + "(): label filter entry '" + labels + "' has an empty label");
        if ("*".equals(names[i]))
          throw new IllegalArgumentException(
              procedureName + "(): label filter entry '" + labels + "' uses '*' inside a compound label: '*' stands alone");
        final char last = names[i].charAt(names[i].length() - 1);
        if ((last == '>' || last == '<') && schema.getTypeOrNull(names[i]) == null)
          throw new IllegalArgumentException(procedureName + "(): label filter entry '" + labels
              + "' ends with a direction marker: a label filter's operator ('+', '-', '/' or '>') goes before the label");
      }
      ProcedureSchemaTypes.requireVertexTypes(procedureName, schema, names);
      return names;
    }

    private static String[][] toArray(final List<String[]> group) {
      return group.toArray(new String[0][]);
    }

    /**
     * The verdict of this step for a node of {@code type}. May answer {@link Verdict#TERMINATE}, which only
     * {@link LabelFilter#evaluate} sees: it resolves it against the minimum level before returning to the walk.
     */
    Verdict evaluate(final DocumentType type, final boolean endNodesOnly) {
      if (matches(type, denylist))
        return Verdict.EXCLUDE_AND_PRUNE;
      if (matches(type, termination))
        return Verdict.TERMINATE;
      final boolean allowed = allowlist.length == 0 || matches(type, allowlist);
      if (matches(type, endNode))
        // An end node is an answer whatever the allowlist says, but the walk goes past it only if the allowlist lets it
        return allowed ? Verdict.INCLUDE_AND_CONTINUE : Verdict.INCLUDE_AND_PRUNE;
      if (allowed)
        return endNodesOnly ? Verdict.EXCLUDE_AND_CONTINUE : Verdict.INCLUDE_AND_CONTINUE;
      return Verdict.EXCLUDE_AND_PRUNE;
    }

    private static boolean matches(final DocumentType type, final String[][] alternatives) {
      for (final String[] conjunction : alternatives) {
        boolean all = true;
        for (final String label : conjunction)
          if (!type.instanceOf(label)) {
            all = false;
            break;
          }
        if (all)
          return true;
      }
      return false;
    }
  }
}
