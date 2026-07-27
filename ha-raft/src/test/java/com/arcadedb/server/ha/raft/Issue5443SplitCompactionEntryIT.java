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

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Regression test for issue #5443, SPLIT variant: the same guarantee as {@link Issue5443FollowerIndexGapIT},
 * but with the compaction's schema change split across many Raft entries - which is what a large enough
 * index does at the stock ceiling.
 * <p>
 * That path used to fail differently: the first chunk carries {@code filesToAdd} and no schema JSON, so
 * the follower reloaded its schema from a half-delivered state, could not resolve the compacted
 * sub-index and detached it. The detach is sticky, so the follower never re-attached and ended up
 * serving only its mutable pages.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue5443SplitCompactionEntryIT extends BaseCompactionIndexCompletenessTest {

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    super.onServerConfiguration(config);
    // Force the compaction's schema change to be SPLIT across many Raft entries (#4743), which is what a
    // large enough index does at the stock 32MB ceiling.
    config.setValue(GlobalConfiguration.HA_APPEND_BUFFER_SIZE, "512KB");
  }

  @Test
  @Tag("slow")
  void everyNodeHoldsTheWholeIndexAfterASplitCompaction() throws Exception {
    assertEveryNodeHoldsTheWholeIndexAfterACompaction();
  }
}
