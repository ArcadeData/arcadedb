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

/** Selects how an index is populated when it is created over existing records. */
public enum IndexBuildMode {
  /**
   * Lets the engine choose: the record-at-a-time mutable LSM build, except for an LSM_TREE index over a type holding at least
   * {@code arcadedb.index.buildSortedUniqueMinRecords} records (UNIQUE, issue #10334) or
   * {@code arcadedb.index.buildSortedNonUniqueMinRecords} records (NOTUNIQUE, issue #10267), which takes the {@link #SORTED} build
   * when it can run.
   */
  DEFAULT,

  /** Uses a bounded external sort and writes the ordered stream into compacted LSM pages. */
  SORTED
}
