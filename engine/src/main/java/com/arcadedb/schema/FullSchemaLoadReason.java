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
package com.arcadedb.schema;

/**
 * Why a replica applying a schema entry ran a full {@link LocalSchema#load} instead of the incremental refresh
 * {@link LocalSchema#loadIncremental} (issue #10020).
 * <p>
 * A full load re-instantiates every component of the database on the single Ratis apply thread, which is what an
 * operator looking at an apply-lag spike or at searches rebuilding after a DDL needs to be able to tie back to its cause.
 * The set is closed on purpose: {@link #getTag()} is a metric tag, so a reason is never built from a file name or a
 * message, which would make its cardinality unbounded.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public enum FullSchemaLoadReason {
  /** {@code arcadedb.ha.schemaIncrementalApply} is off, so every entry takes the full load. Decided by the caller. */
  INCREMENTAL_DISABLED("incremental_disabled", "the incremental refresh is switched off"),

  /** The entry renames component files, whose components the incremental refresh would look up by their old names. */
  FILE_RENAMED("file_renamed", "the entry renames component files"),

  /** Nothing was ever loaded in this lifecycle, so there is no baseline to add to. */
  NOT_LOADED("not_loaded", "no schema was loaded yet, so there is nothing to refresh"),

  /** The entry retires a file: its stale entry stays in the lookup maps and the index that superseded it re-reads page 0. */
  FILE_RETIRED("file_retired", "the entry retires component files"),

  /** The entry brings a file with no component yet that cannot be registered alone: a dictionary, a compacted index or a bloom filter. */
  NON_INCREMENTAL_NEW_FILE("non_incremental_new_file",
      "the entry adds a dictionary, a compacted index or a bloom filter file"),

  /** The entry wrote pages into a compacted index or a bloom filter, whose in-memory state no load hook re-reads. */
  NON_INCREMENTAL_TOUCHED_FILE("non_incremental_touched_file",
      "the entry wrote pages into a compacted index or a bloom filter file"),

  /** The entry wrote pages into a file the file manager does not hold, so the incremental refresh cannot rebuild its component. */
  TOUCHED_FILE_MISSING("touched_file_missing", "the entry wrote pages into a file this node does not have");

  private final String tag;
  private final String description;

  FullSchemaLoadReason(final String tag, final String description) {
    this.tag = tag;
    this.description = description;
  }

  /** The value of the {@code reason} tag on the full-load metrics: lower snake case, stable across releases. */
  public String getTag() {
    return tag;
  }

  /** A short phrase an operator can read in a log line. */
  public String getDescription() {
    return description;
  }
}
