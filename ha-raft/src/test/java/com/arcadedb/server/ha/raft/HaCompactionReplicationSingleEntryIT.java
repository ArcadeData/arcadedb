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
package com.arcadedb.server.ha.raft;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Issue #5443: an incremental compaction appends a new series to the ALREADY EXISTING compacted file, and those pages
 * are written without WAL and outside {@code addFiles}, so nothing replicated them. A follower ended up holding fewer
 * index entries than the leader while every record replicated normally, which makes the keys in the missing range
 * unfindable on that node.
 * <p>
 * Sibling of {@link HaCompactionReplicationIT}, renamed from {@code Issue5443FollowerIndexGapIT}. It keeps a fresh
 * cluster per test ({@link BaseCompactionIndexCompletenessTest}): three servers at the STOCK append buffer, so the
 * compaction's schema change travels in one Raft entry, while {@link HaCompactionReplicationIT} shrinks the buffer to
 * split it (the split variant of the same guarantee lives there). The scenario and its assertions live in
 * {@link BaseCompactionIndexCompletenessTest}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class HaCompactionReplicationSingleEntryIT extends BaseCompactionIndexCompletenessTest {

  /** Issue #5443, from {@code Issue5443FollowerIndexGapIT}. */
  @Test
  @Tag("slow")
  void everyNodeHoldsTheWholeIndexAfterACompaction() throws Exception {
    assertEveryNodeHoldsTheWholeIndexAfterACompaction();
  }
}
