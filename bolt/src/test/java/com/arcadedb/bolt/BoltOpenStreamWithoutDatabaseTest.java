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
package com.arcadedb.bolt;

import com.arcadedb.bolt.message.BoltMessage;
import com.arcadedb.bolt.message.DiscardMessage;
import com.arcadedb.bolt.message.PullMessage;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A PULL or a DISCARD on a connection that has no database bound reads nothing: it is answered with a FAILURE and the
 * connection is failed, whatever state it was in.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class BoltOpenStreamWithoutDatabaseTest {

  @Test
  void aPullWithoutADatabaseIsRefused() throws Exception {
    for (final String state : new String[] { "STREAMING", "TX_STREAMING" }) {
      final BoltHandlerProbe.Dispatch dispatch = BoltHandlerProbe.dispatch(new PullMessage(Map.of("n", -1L)), state);

      assertThat(dispatch.reply()).as("PULL in %s", state).isEqualTo(BoltMessage.FAILURE);
      assertThat(dispatch.code()).as("PULL in %s", state).isEqualTo(BoltErrorCodes.FORBIDDEN_ERROR);
      assertThat(dispatch.stateAfter()).as("PULL in %s", state).isEqualTo("FAILED");
    }
  }

  @Test
  void aDiscardWithoutADatabaseIsRefused() throws Exception {
    for (final String state : new String[] { "STREAMING", "TX_STREAMING" }) {
      final BoltHandlerProbe.Dispatch dispatch = BoltHandlerProbe.dispatch(new DiscardMessage(Map.of("n", -1L)), state);

      assertThat(dispatch.reply()).as("DISCARD in %s", state).isEqualTo(BoltMessage.FAILURE);
      assertThat(dispatch.code()).as("DISCARD in %s", state).isEqualTo(BoltErrorCodes.FORBIDDEN_ERROR);
      assertThat(dispatch.stateAfter()).as("DISCARD in %s", state).isEqualTo("FAILED");
    }
  }
}
