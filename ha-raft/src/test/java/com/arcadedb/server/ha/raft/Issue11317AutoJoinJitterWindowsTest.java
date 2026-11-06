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
package com.arcadedb.server.ha.raft;

import com.arcadedb.server.ha.raft.KubernetesAutoJoin.JitterWindow;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11317: the jitter of the first cluster probe was documented as non-overlapping windows per pod ordinal, while only the
 * lower bound moved with the ordinal: the windows were nested, and every pod from ordinal 6 up drew from one 100 ms slot.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue11317AutoJoinJitterWindowsTest {
  /** The number of slots the range holds: 3000 ms of range, 500 ms per slot. */
  private static final int  SLOTS   = 6;
  private static final long SLOT_MS = 500L;
  private static final long MAX_MS  = 3_000L;

  private static JitterWindow windowOf(final int ordinal) {
    return KubernetesAutoJoin.computeJitterWindow("arcadedb-" + ordinal);
  }

  private static boolean overlap(final JitterWindow a, final JitterWindow b) {
    return a.minMs() < b.maxMs() && b.minMs() < a.maxMs();
  }

  @Test
  void ordinalsLessThanALapApartNeverDrawTheSameInstant() {
    for (int a = 0; a < 4 * SLOTS; a++)
      for (int b = a + 1; b < a + SLOTS; b++)
        assertThat(overlap(windowOf(a), windowOf(b)))
            .as("pod-%d %s and pod-%d %s must not overlap", a, windowOf(a), b, windowOf(b)).isFalse();
  }

  @Test
  void aStatefulSetOfAnySizeKeepsFullWidthWindowsInsideTheRange() {
    for (final int ordinal : new int[] { 0, 1, 5, 6, 7, 8, 9, 100, 1_001, Integer.MAX_VALUE }) {
      final JitterWindow window = windowOf(ordinal);
      assertThat(window.maxMs() - window.minMs()).as("width of the window of pod-%d", ordinal).isEqualTo(SLOT_MS);
      assertThat(window.minMs()).as("start of the window of pod-%d", ordinal).isGreaterThanOrEqualTo(0L);
      assertThat(window.maxMs()).as("end of the window of pod-%d", ordinal).isLessThanOrEqualTo(MAX_MS);
    }
  }

  @Test
  void theHighOrdinalsNoLongerCollapseOntoOneSlot() {
    assertThat(windowOf(6)).isNotEqualTo(windowOf(7));
    assertThat(windowOf(7)).isNotEqualTo(windowOf(8));
    assertThat(windowOf(6)).as("a lap later the slot comes round again").isEqualTo(windowOf(0));
  }

  @Test
  void withoutAnOrdinalThePodDrawsFromTheWholeRange() {
    final JitterWindow whole = new JitterWindow(0, MAX_MS);
    assertThat(KubernetesAutoJoin.computeJitterWindow(null)).isEqualTo(whole);
    assertThat(KubernetesAutoJoin.computeJitterWindow("")).isEqualTo(whole);
    assertThat(KubernetesAutoJoin.computeJitterWindow("arcadedb")).isEqualTo(whole);
    assertThat(KubernetesAutoJoin.computeJitterWindow("arcadedb-")).isEqualTo(whole);
    assertThat(KubernetesAutoJoin.computeJitterWindow("arcadedb-primary")).isEqualTo(whole);
    assertThat(KubernetesAutoJoin.computeJitterWindow("arcadedb-99999999999")).as("an ordinal an int cannot hold").isEqualTo(whole);
  }

  /** The wait of a probe round is drawn from the window, whatever this machine's HOSTNAME reads. */
  @Test
  void aProbeRoundWaitsInsideTheWindowOfThisHost() {
    final RaftPeer self = RaftPeer.newBuilder().setId(RaftPeerId.valueOf("self")).setAddress("localhost:1").build();
    final KubernetesAutoJoin autoJoin = new KubernetesAutoJoin(null, RaftGroup.valueOf(RaftGroupId.randomId(), self), self.getId(),
        new RaftProperties());

    final List<Long> waits = new ArrayList<>();
    // INTERRUPTED ENDS THE ROUND BEFORE ANY PEER IS PROBED, SO NO CLUSTER IS NEEDED TO READ THE WAIT
    autoJoin.setSleeperForTesting(ms -> {
      waits.add(ms);
      throw new InterruptedException();
    });

    final JitterWindow window = KubernetesAutoJoin.computeJitterWindow(System.getenv("HOSTNAME"));
    try {
      for (int i = 0; i < 200; i++)
        assertThat(autoJoin.tryAutoJoin()).isEqualTo(KubernetesAutoJoin.Outcome.INTERRUPTED);
    } finally {
      // THE ROUND RESTORES THE INTERRUPT FLAG, WHICH MUST NOT LEAK INTO THE NEXT TEST ON THIS THREAD
      Thread.interrupted();
    }

    assertThat(waits).hasSize(200).allSatisfy(ms -> assertThat(ms).isBetween(window.minMs(), window.maxMs() - 1));
  }
}
