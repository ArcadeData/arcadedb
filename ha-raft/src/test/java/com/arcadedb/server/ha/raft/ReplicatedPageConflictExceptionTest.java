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

import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.exception.NeedRetryException;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The refusal the leader sends back travels through Ratis by class name and message only (issue #6965): the page and
 * cluster version the replica waits for must survive that round trip.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class ReplicatedPageConflictExceptionTest {

  @Test
  void theFieldsSurviveTheMessageRoundTrip() {
    final ReplicatedPageConflictException original = new ReplicatedPageConflictException("graph", 28, 3, 1136, 1137);
    assertThat(original).isInstanceOf(ConcurrentModificationException.class).isInstanceOf(NeedRetryException.class);
    assertThat(original.getMessage()).contains("28/3").contains("'graph'").contains("version 1136").contains("version 1137")
        .contains("retry");

    // What the Ratis client rebuilds on the originating node.
    final ReplicatedPageConflictException rebuilt = new ReplicatedPageConflictException(original.getMessage());
    assertThat(rebuilt.getDatabaseName()).isEqualTo("graph");
    assertThat(rebuilt.getFileId()).isEqualTo(28);
    assertThat(rebuilt.getPageNumber()).isEqualTo(3);
    assertThat(rebuilt.getClusterVersion()).isEqualTo(1137);
  }

  @Test
  void anUnparsableMessageDegradesToAPlainConflict() {
    final ReplicatedPageConflictException rebuilt = new ReplicatedPageConflictException("something else entirely");
    assertThat(rebuilt.getClusterVersion()).isEqualTo(-1);
    assertThat(rebuilt.getFileId()).isEqualTo(-1);
    assertThat(rebuilt.getDatabaseName()).isNull();

    assertThat(new ReplicatedPageConflictException((String) null).getClusterVersion()).isEqualTo(-1);
  }
}
