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
package com.arcadedb.index.vector;

import com.arcadedb.database.RID;
import com.arcadedb.query.sql.executor.WorkGuard;
import io.github.jbellis.jvector.util.Bits;

import java.util.Set;

/**
 * Tells JVector which graph ordinals may enter a search result: the ones that still map to a live vector, optionally
 * narrowed to an allow-list of RIDs.
 * <p>
 * The liveness half is not an optimisation, it is what keeps a delete from emptying a result set (issue #5558). A
 * deleted vector stays in the HNSW graph until the next rebuild, and JVector's beam stops as soon as it holds
 * {@code rerankK} <i>acceptable</i> candidates. Accepting everything therefore lets a query aimed at a deleted
 * neighbourhood fill its beam with tombstones, declare itself finished, and hand back nodes that the caller's
 * post-filter then removes one by one - down to nothing. Rejecting them here instead makes the same beam keep walking
 * until it has found live neighbours: the tombstones are still traversed (JVector expands a rejected node, it just
 * does not collect it), so the surviving vectors behind the hole stay reachable.
 * <p>
 * The predicate is deliberately identical to the post-filter applied to the search output, so nothing that would have
 * survived it is dropped here.
 * <p>
 * <b>Deliberately not memoized.</b> A filter whose verdict consumes a budget would have to cache it, so that a repeat
 * call within one search cannot spend the budget twice - that is what the withdrawn {@code GroupedRIDBitsFilter} did
 * before #5761 moved the {@code groupBy} cap out of the traversal. This one has no state to protect, so it reads the
 * location map live and a delete committed mid-traversal takes effect immediately. The freshness is worth more than
 * the stability here, and it is safe because the result loop re-checks the same predicate before emitting - an
 * ordinal admitted just before its delete lands is dropped at the output rather than returned.
 * <p>
 * <b>This rests on one JVector behaviour.</b> "The tombstones are still traversed" is true because
 * {@code GraphSearcher.searchOneLayer} consults {@code acceptOrds} only to decide whether a popped node joins the
 * result heap, and expands its neighbours either way. A JVector upgrade that started pruning the neighbours of a
 * rejected node would silently make the vectors behind a deleted region unreachable again - the same bug this class
 * exists to fix, with the filter now causing it. If you are upgrading JVector, check that first;
 * {@code Issue5558DeletedRegionSearchTest} is what will tell you.
 * <p>
 * <b>Cost.</b> This runs on every search, not only on the RID-restricted ones that used to need a filter, so it is
 * worth knowing what it costs an index with no deletions at all. JVector calls {@code acceptOrds.get()} once per
 * <i>popped</i> candidate, not once per scored neighbour, so it does not scale with beam width times graph fan-out:
 * measured at 28.7 calls per query on a 50k-vector, 128-dimension INT8 index at {@code k=10}. One call is a location
 * lookup at 7.5 ns, so the whole filter adds 0.22 us to a 172 us query - 0.13%. That is why there is no "no
 * tombstones, use Bits.ALL" fast path here: it would buy nothing measurable and would cost a second, subtly
 * different definition of which nodes a search may return.
 * <p>
 * Since issue #5588 the location index holds no resident objects, so the unrestricted case asks it for the single
 * presence bit rather than for a location tuple it would have to materialize. Only the allow-list case pays for a
 * {@link RID}, and it pays for it because {@code Set<RID>} membership needs the object.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class LiveVectorBitsFilter implements Bits {
  private final Set<RID>            allowedRIDs;
  private final int[]               ordinalToVectorIdSnapshot;
  // NOT a snapshot, unlike the ordinal map: this is the live location map, read at traversal time so the filter
  // answers exactly what the post-filter on the search output will.
  private final VectorLocationIndex vectorIndex;
  /**
   * RIDs whose committed vectors the calling transaction has superseded - removed, or rewritten with a vector the
   * overlay contributes instead (issue #7378). {@code null} whenever there is no such transaction, which is every
   * read-only query and therefore the case this filter's measured cost was established on: the field is a null
   * check away from the fast path, and only a transaction that has actually written to this index makes the walk
   * pay for a {@link RID} it would not otherwise have materialized.
   */
  private final Set<RID>            supersededRIDs;
  /**
   * The bound of the statement running the search (issue #9896). JVector calls this filter on every candidate the walk
   * considers, so it is the one place inside the walk that ArcadeDB code runs often enough to stop it: the walk can read a
   * record per candidate, and its length is the graph's to decide. Null for a walk nothing can stop.
   */
  private final WorkGuard           guard;
  /**
   * The walk of a predicate whose candidates an index can list (issue #10921): the members the walk needs before it ends (its
   * beam), and the vectors of the index, or 0 when the walk is never given up for the listing.
   */
  private       int                 listedWalkBeam;
  private final int                 listedWalkVectors;
  private final int                 listedWalkMaxCandidates;
  private       int                 calls;
  private       int                 accepted;

  /**
   * What a candidate the walk considers costs against a candidate the pre-filter plan scores (issue #10921): the walk loads
   * the candidate's record to test it and scores its neighbours, the plan reads one vector. Measured by
   * Issue10921WherePrefilterBenchmark on 100,000 vectors of 128 dimensions: about 3.8 us against 0.45 us.
   */
  static final int WALK_TO_PLAN_COST_RATIO = 8;

  /** How many candidates the walk considers between two estimates of the predicate's selectivity. */
  private static final int ESTIMATE_EVERY = 32;

  /**
   * Thrown by {@link #get} once the walk has found its predicate selective enough that scoring what an index lists for it
   * costs less than the rest of the walk: the search abandons the walk and scores the listing instead, if it holds no more
   * than {@link #listingCap} candidates. Carries no stack trace, it is a plan decision and not an error.
   */
  static final class SelectiveWalkException extends RuntimeException {
    final int listingCap;

    SelectiveWalkException(final int listingCap) {
      super("The filtered walk costs more than scoring the listed candidates", null, false, false);
      this.listingCap = listingCap;
    }
  }

  /**
   * @param allowedRIDs               optional RID allow-list; {@code null} or empty means "every live vector"
   * @param ordinalToVectorIdSnapshot the ordinal map captured together with the vectors snapshot by the caller
   * @param vectorIndex               the live location map that answers whether a vector id is still live
   */
  LiveVectorBitsFilter(final Set<RID> allowedRIDs, final int[] ordinalToVectorIdSnapshot,
      final VectorLocationIndex vectorIndex) {
    this(allowedRIDs, ordinalToVectorIdSnapshot, vectorIndex, null);
  }

  /**
   * @param allowedRIDs               optional RID allow-list; {@code null} or empty means "every live vector"
   * @param ordinalToVectorIdSnapshot the ordinal map captured together with the vectors snapshot by the caller
   * @param vectorIndex               the live location map that answers whether a vector id is still live
   * @param supersededRIDs            optional set of RIDs the calling transaction has superseded; {@code null} or
   *                                  empty means "no uncommitted work to account for"
   */
  LiveVectorBitsFilter(final Set<RID> allowedRIDs, final int[] ordinalToVectorIdSnapshot,
      final VectorLocationIndex vectorIndex, final Set<RID> supersededRIDs) {
    this(allowedRIDs, ordinalToVectorIdSnapshot, vectorIndex, supersededRIDs, null);
  }

  LiveVectorBitsFilter(final Set<RID> allowedRIDs, final int[] ordinalToVectorIdSnapshot,
      final VectorLocationIndex vectorIndex, final Set<RID> supersededRIDs, final WorkGuard guard) {
    this(allowedRIDs, ordinalToVectorIdSnapshot, vectorIndex, supersededRIDs, guard, 0, 0, 0);
  }

  /**
   * @param listedWalkBeam          for a predicate an index can list, the members the walk needs before it ends: the walk is
   *                                then given up with a {@link SelectiveWalkException} once the listing costs less than the
   *                                rest of it. 0 for a walk never given up
   * @param listedWalkVectors       the vectors of the index, of which the selectivity the walk measures is a fraction
   * @param listedWalkMaxCandidates the most candidates the pre-filter plan takes (its selectivity threshold): a predicate
   *                                expected to keep more is walked
   */
  LiveVectorBitsFilter(final Set<RID> allowedRIDs, final int[] ordinalToVectorIdSnapshot,
      final VectorLocationIndex vectorIndex, final Set<RID> supersededRIDs, final WorkGuard guard, final int listedWalkBeam,
      final int listedWalkVectors, final int listedWalkMaxCandidates) {
    this.guard = guard;
    this.listedWalkBeam = listedWalkMaxCandidates > 0 ? listedWalkBeam : 0;
    this.listedWalkVectors = listedWalkVectors;
    this.listedWalkMaxCandidates = listedWalkMaxCandidates;
    this.allowedRIDs = allowedRIDs != null && !allowedRIDs.isEmpty() ? allowedRIDs : null;
    this.ordinalToVectorIdSnapshot = ordinalToVectorIdSnapshot;
    this.vectorIndex = vectorIndex;
    this.supersededRIDs = supersededRIDs != null && !supersededRIDs.isEmpty() ? supersededRIDs : null;
  }

  /**
   * Whether the walk may be given up for the listing of its predicate (issue #10921).
   */
  boolean givesUpForTheListing() {
    return listedWalkBeam > 0;
  }

  /**
   * The walk that follows (a resumed pass) is no longer given up for the listing.
   */
  void disarm() {
    listedWalkBeam = 0;
  }

  /**
   * A graph ordinal may be answered with unless it is outside the ordinal map, no longer live, or outside the
   * allow-list.
   */
  @Override
  public boolean get(final int ordinal) {
    ++calls;
    if (guard != null)
      guard.checkPeriodically(calls);
    if (listedWalkBeam == 0)
      return accepts(ordinal);
    final boolean accept = accepts(ordinal);
    if (accept)
      ++accepted;
    if (calls % ESTIMATE_EVERY == 0)
      giveUpForTheListingIfCheaper();
    return accept;
  }

  /**
   * Gives the walk up when its predicate proves selective (issue #10921). The walk ends once the predicate has accepted a beam
   * of candidates, so at the rate it has accepted them so far it still has {@code (beam - accepted) / rate} candidates to
   * consider, while the predicate keeps about {@code rate * vectors} records, which the plan would score instead. The rate is
   * the walk's own, measured where the walk goes, which is what its cost depends on; counting one accepted candidate more than
   * seen keeps it from reading zero, and leans towards walking on. The walk is given up when the listing it expects costs no
   * more than half of the rest of the walk, nor holds more than half of the candidates the plan takes, and the listing it then
   * asks for is capped at both, so a predicate that keeps more records than the walk met where it went is walked after all.
   */
  private void giveUpForTheListingIfCheaper() {
    final double rate = (accepted + 1d) / calls;
    final double listingCap = Math.min(listedWalkMaxCandidates,
        Math.max(0, listedWalkBeam - accepted) / rate * WALK_TO_PLAN_COST_RATIO);
    if (rate * listedWalkVectors * 2 <= listingCap)
      throw new SelectiveWalkException((int) listingCap);
  }

  private boolean accepts(final int ordinal) {
    if (ordinal < 0 || ordinal >= ordinalToVectorIdSnapshot.length)
      return false;

    final int vectorId = ordinalToVectorIdSnapshot[ordinal];
    if (allowedRIDs == null && supersededRIDs == null)
      // The common case, and the one every unrestricted search takes: liveness alone, answered by one presence bit
      // with nothing materialized (issue #5588). The constructor already collapsed an empty allow-list to null.
      return vectorIndex.isLive(vectorId);

    final RID rid = vectorIndex.getRid(vectorId);
    if (rid == null)
      return false;
    if (allowedRIDs != null && !allowedRIDs.contains(rid))
      return false;
    // Rejected during the walk rather than only at the output, for the same reason a tombstone is (issue #5558):
    // a beam that collects rows the caller's own transaction has superseded stops as soon as it holds rerankK of
    // them, and the post-filter then removes them one by one - down to an answer shorter than the one the same
    // query returns after the commit.
    return supersededRIDs == null || !supersededRIDs.contains(rid);
  }
}
