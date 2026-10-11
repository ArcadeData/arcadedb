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
   * {@code statistics} is still data: a component file's name always ends with its own extension, which is neither of
   * the two below. The shapes describe the files the engine itself creates; a user file dropped into the directory that
   * happens to take one of them (say {@code gav-notes.csr}) is left out too, which is harmless because nothing in the
   * database reads or ships it.
   */
  public static boolean isNodeLocalFileName(final String fileName) {
    return fileName.equals(LocalSchema.STATISTICS_FILE_NAME)
        || fileName.equals(LocalSchema.CACHED_COUNT_FILE_NAME_LEGACY)
        || fileName.equals(DATABASE_LOCK_FILE_NAME)
        || (fileName.startsWith(GAV_CSR_FILE_PREFIX)
        && (fileName.endsWith(GAV_CSR_FILE_EXTENSION) || fileName.endsWith(CCH_ORDER_FILE_EXTENSION)));
  }
}
