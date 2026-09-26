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

import com.arcadedb.database.Database;
import com.arcadedb.database.RID;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.EdgeType;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Identity of a relationship an anonymous hop bound through a Graph Analytical View, which holds adjacency ids and no
 * edge records (issue #8394). Cypher forbids binding one relationship twice in a MATCH clause, and an anonymous hop
 * never exposes the edge it walked, so any bijection between the relationships and their labels answers that question
 * as well as the record identity would: two relationships of the same type joining the same ordered pair of vertices
 * are interchangeable to every observer of the query.
 * <p>
 * The label is {@code (type, out, in, occurrence)}, where {@code occurrence} ranks this relationship among the parallel
 * ones of the same type and orientation in the adjacency list it was read from. Every list a hop can walk holds the
 * {@code m} parallel relationships of a pair as exactly {@code m} entries - the out-list of {@code out}, the in-list of
 * {@code in} - so each walk hands out the occurrences {@code 0..m-1}, and two hops conflict exactly when they carry the
 * same number. The rank costs a scan of the list, so it is taken only when another label already has the same endpoints.
 * <p>
 * <b>An occurrence is not the identity of a physical edge.</b> The out-list and the in-list are sorted independently,
 * so the k-th parallel entry of one need not be the same edge as the k-th of the other. It does not have to be: each
 * list numbers the {@code m} relationships bijectively, and N hops accepting pairwise distinct numbers yield exactly
 * {@code m!/(m-N)!} rows, the count of assignments of distinct edges, however the two numberings align. That holds only
 * because nothing observes which edge a label stands for: a hop that exposes its relationship, or filters on its
 * properties, must bind the edge record instead, which is why only anonymous relationships are tracked this way.
 * <p>
 * Labels compare only with labels. The planner puts a MATCH clause on labels only when every relationship of the clause
 * that could collide with another one is walked through a view; otherwise all of them bind edge records.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class GAVEdgeRef {
  private final String type;
  private final RID    out;
  private final RID    in;
  // Lazily ranked labels read the rank off the adjacency slice: the entries equal to slice[index] that precede it
  private final int[]   slice;
  private final int     index;
  private final boolean sorted;
  private       int     occurrence;

  private GAVEdgeRef(final String type, final RID out, final RID in, final int[] slice, final int index,
      final boolean sorted, final int occurrence) {
    this.type = type;
    this.out = out;
    this.in = in;
    this.slice = slice;
    this.index = index;
    this.sorted = sorted;
    this.occurrence = occurrence;
  }

  /**
   * A label whose occurrence is the rank of {@code slice[index]} among the equal entries before it, taken on demand.
   *
   * @param sorted whether the slice is sorted (see {@link #isSorted}), which lets the rank scan the run of equal entries
   *               only
   */
  public static GAVEdgeRef inSlice(final String type, final RID out, final RID in, final int[] slice, final int index,
      final boolean sorted) {
    return new GAVEdgeRef(type, out, in, slice, index, sorted, -1);
  }

  /** A label whose occurrence is already known. */
  public static GAVEdgeRef ranked(final String type, final RID out, final RID in, final int occurrence) {
    return new GAVEdgeRef(type, out, in, null, -1, false, occurrence);
  }

  /**
   * Whether an adjacency slice is sorted. A Graph Analytical View always hands out sorted per-type slices, overlay
   * included, but the provider SPI does not promise it: checked once per slice, in the walk that reads it anyway.
   */
  public static boolean isSorted(final int[] slice) {
    for (int i = 1; i < slice.length; i++)
      if (slice[i] < slice[i - 1])
        return false;
    return true;
  }

  /**
   * The edge types a tracked hop walks one by one: its own, or every edge type of the schema for an untyped hop. Read
   * from the schema rather than from the view's snapshot, which holds no slice for a type that had no edges when it was
   * built: edges of that type committed since live in the view's overlay, and a walk skipping the type would miss them.
   */
  public static String[] trackedEdgeTypes(final Database database, final String[] hopTypes) {
    if (hopTypes != null && hopTypes.length > 0)
      return hopTypes;
    final List<String> names = new ArrayList<>();
    for (final DocumentType type : database.getSchema().getTypes())
      if (type instanceof EdgeType)
        names.add(type.getName());
    return names.toArray(new String[0]);
  }

  /**
   * Ranks {@code slice[index]} among the equal entries of the slice that precede it. In a sorted slice they form one
   * contiguous run ending at {@code index}, so only that run is read: the cost is the number of parallel relationships,
   * not the degree of the vertex.
   */
  public static int rankInSlice(final int[] slice, final int index, final boolean sorted) {
    final int value = slice[index];
    int rank = 0;
    if (sorted) {
      for (int i = index - 1; i >= 0 && slice[i] == value; i--)
        ++rank;
      return rank;
    }
    for (int i = 0; i < index; i++)
      if (slice[i] == value)
        ++rank;
    return rank;
  }

  /**
   * Collects the labels bound in {@code row} by the given relationship variables, or {@code null} when there are none.
   * A variable not bound yet - it belongs to a hop further up the plan - is skipped.
   */
  public static GAVEdgeRef[] collect(final Result row, final Set<String> variables) {
    if (variables == null || variables.isEmpty())
      return null;
    // Counted first, so the per-row array is allocated once and at its size
    int count = 0;
    for (final String variable : variables)
      if (row.getProperty(variable) instanceof GAVEdgeRef)
        ++count;
    if (count == 0)
      return null;
    final GAVEdgeRef[] refs = new GAVEdgeRef[count];
    int i = 0;
    for (final String variable : variables)
      if (row.getProperty(variable) instanceof GAVEdgeRef ref)
        refs[i++] = ref;
    return refs;
  }

  /** True when one of {@code bound} is the relationship {@code slice[index]} labels. The rank is taken only on a match. */
  public static boolean conflicts(final GAVEdgeRef[] bound, final String type, final RID out, final RID in,
      final int[] slice, final int index, final boolean sorted) {
    if (bound == null)
      return false;
    int rank = -1;
    for (final GAVEdgeRef ref : bound)
      if (ref.sameEndpoints(type, out, in)) {
        if (rank < 0)
          rank = rankInSlice(slice, index, sorted);
        if (ref.occurrence() == rank)
          return true;
      }
    return false;
  }

  /** True when one of {@code bound} is the relationship labelled {@code (type, out, in, occurrence)}. */
  public static boolean conflicts(final GAVEdgeRef[] bound, final String type, final RID out, final RID in,
      final int occurrence) {
    if (bound == null)
      return false;
    for (final GAVEdgeRef ref : bound)
      if (ref.sameEndpoints(type, out, in) && ref.occurrence() == occurrence)
        return true;
    return false;
  }

  public int occurrence() {
    int o = occurrence;
    if (o < 0) {
      // Idempotent: a concurrent reader computes the same value
      o = rankInSlice(slice, index, sorted);
      occurrence = o;
    }
    return o;
  }

  public boolean sameEndpoints(final String type, final RID out, final RID in) {
    return this.type.equals(type) && this.out.equals(out) && this.in.equals(in);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o)
      return true;
    if (!(o instanceof GAVEdgeRef other))
      return false;
    return other.sameEndpoints(type, out, in) && other.occurrence() == occurrence();
  }

  @Override
  public int hashCode() {
    // The occurrence stays out of the hash so hashing never pays for the rank
    return Objects.hash(type, out, in);
  }

  @Override
  public String toString() {
    return "[:" + type + " " + out + "->" + in + " #" + occurrence() + "]";
  }
}
