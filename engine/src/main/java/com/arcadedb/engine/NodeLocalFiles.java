/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
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
package com.arcadedb.engine;

import com.arcadedb.index.vector.LSMVectorIndexGraphFile;
import com.arcadedb.index.vector.LSMVectorIndexGraphManifest;
import com.arcadedb.index.vector.LSMVectorIndexOrdinalMapFile;
import com.arcadedb.index.vector.LSMVectorIndexPQFile;
import com.arcadedb.schema.LocalSchema;

/**
 * The files the engine writes into a database directory that belong to the NODE holding the copy rather than to the
 * database itself (issue #9638): two nodes holding the same data can each have a different one, or one can have none at
 * all, and neither is wrong. They are declared here, next to the names their writers build them from, so that anything
 * comparing two copies of a database - the HA {@code /checksums} endpoint first of all - can leave them out without
 * keeping its own list of names that drifts every time a writer is added.
 * <ul>
 * <li>{@link LocalSchema#STATISTICS_FILE_NAME} and its pre-25.2.1 predecessor
 * {@link LocalSchema#CACHED_COUNT_FILE_NAME_LEGACY}: cached record counts and page free-space hints, written by
 * {@code LocalSchema.close()} at whatever moment this node last closed, and not shipped by an HA snapshot.</li>
 * <li>{@link #DATABASE_LOCK_FILE_NAME}: the exclusive-open marker. Empty today, which is the only reason it never
 * showed as a difference.</li>
 * <li>{@code gav-<view>.csr} and its {@code gav-<view>.csr.<weight>[.<edgeType>]*.cchorder} companions: a Graph
 * Analytical View's persisted CSR and contraction-hierarchy orders. A cache of data the database already holds, written
 * at close and - in a replicated database - only on the leader, so a cluster has one on the current leader, a stale one
 * wherever leadership used to be and none elsewhere.</li>
 * <li>An LSM vector index's proximity graph and everything derived from it (issue #9751): the {@code .vecgraph}
 * component, its {@code .vecgraphfp} manifest and {@code .vecordmap} ordinal map, and the {@code .vecpq} product
 * quantization. Every node builds its own graph, on the inner database instance and without the WAL (issues #8292,
 * #8905), from the vectors the index pages DO replicate, so the graph pages carry different bytes on every node - the
 * JVector build is parallel and not deterministic - and the PQ codebooks are trained from the same node-local pass.
 * The three plain side files are not shipped by a snapshot at all; the {@code .vecgraph} component is shipped EMPTY
 * (issue #9758), so the follower keeps it registered under the same file id and builds its own graph, with its own
 * manifest, on first use - rather than reusing the leader's pages with nothing beside them to vouch for them. A
 * divergence in any of them is a cache that differs, never data that does: the vectors themselves live in the index's
 * own pages, which ARE compared.</li>
 * <li>{@link TransactionManager#LAST_TX_ID_FILE_NAME}: the recency marker, holding the highest transaction id this
 * node had assigned the last time it rotated its WAL or closed (issue #5277). A snapshot ships a synthesized copy, but
 * on a live node the file is rewritten at moments that are this node's own, so two healthy peers at the same commit
 * routinely hold different values - or one holds none (issue #9751). Node-local here means "not comparable", not
 * "never shipped".</li>
 * </ul>
 * A new engine side file that is not replicated content belongs here. {@code Issue9638NodeLocalFilesChecksumTest} in
 * {@code ha-raft} drives a real database through its writers and fails on a file that is neither shipped by a snapshot
 * nor declared node-local.
 */
public final class NodeLocalFiles {
  public static final String DATABASE_LOCK_FILE_NAME  = "database.lck";
  public static final String GAV_CSR_FILE_PREFIX      = "gav-";
  public static final String GAV_CSR_FILE_EXTENSION   = ".csr";
  public static final String CCH_ORDER_FILE_EXTENSION = ".cchorder";

  private NodeLocalFiles() {
  }

  /**
   * True when {@code fileName} (a name, never a path) is one of the node-local files above. Matched by exact name or by
   * the full prefix-plus-extension shape, never by a substring, so a component file of a type named {@code gav-x} or
   * {@code statistics} is still data: a component file's name always ends with its own extension, and the only component
   * extension matched below is the vector graph's own {@code .vecgraph}. The shapes describe the files the engine
   * itself creates; a user file dropped into the directory that happens to take one of them (say {@code gav-notes.csr})
   * is left out too, which is harmless because nothing in the database reads or ships it.
   * <p>
   * <b>"Node-local" answers "may two healthy copies differ here?", not "is it shipped?".</b> The {@code .vecgraph}
   * component (as an empty file, issue #9758) and {@code last-tx-id.bin} are both node-local and both carried by an HA
   * snapshot, so a caller deciding what to SHIP or BACK UP must not use this method to skip files.
   */
  public static boolean isNodeLocalFileName(final String fileName) {
    return fileName.equals(LocalSchema.STATISTICS_FILE_NAME)
        || fileName.equals(LocalSchema.CACHED_COUNT_FILE_NAME_LEGACY)
        || fileName.equals(DATABASE_LOCK_FILE_NAME)
        || fileName.equals(TransactionManager.LAST_TX_ID_FILE_NAME)
        || isVectorGraphFileName(fileName)
        || (fileName.startsWith(GAV_CSR_FILE_PREFIX)
        && (fileName.endsWith(GAV_CSR_FILE_EXTENSION) || fileName.endsWith(CCH_ORDER_FILE_EXTENSION)));
  }

  /**
   * True when {@code fileName} is an LSM vector index's node-locally built graph or one of the files derived from it:
   * the {@code .vecgraph} component, the {@code .vecgraphfp} manifest, the {@code .vecordmap} ordinal map and the
   * {@code .vecpq} product quantization (issue #9751). Matched by the final extension, which is how every one of their
   * writers names them ({@code <base>.<ext>}); a write in flight ends in {@code .tmp} instead and is not matched here.
   */
  public static boolean isVectorGraphFileName(final String fileName) {
    return endsWithExtension(fileName, LSMVectorIndexGraphFile.FILE_EXT)
        || endsWithExtension(fileName, LSMVectorIndexGraphManifest.FILE_EXT)
        || endsWithExtension(fileName, LSMVectorIndexOrdinalMapFile.FILE_EXT)
        || endsWithExtension(fileName, LSMVectorIndexPQFile.FILE_EXT);
  }

  /**
   * True when {@code fileName} is an LSM vector index's {@code .vecgraph} page component alone - the one file of
   * {@link #isVectorGraphFileName} that is registered, and so the one an HA snapshot and a full backup reach (both carry
   * it empty, issues #9758 and #9888). Matched by the final extension, like the rest of this class.
   */
  public static boolean isVectorGraphComponentFileName(final String fileName) {
    return endsWithExtension(fileName, LSMVectorIndexGraphFile.FILE_EXT);
  }

  private static boolean endsWithExtension(final String fileName, final String extension) {
    final int dot = fileName.length() - extension.length() - 1;
    return dot > 0 && fileName.charAt(dot) == '.' && fileName.endsWith(extension);
  }
}
