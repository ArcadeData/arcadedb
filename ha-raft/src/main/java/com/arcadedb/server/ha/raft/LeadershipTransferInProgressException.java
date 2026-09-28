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

import com.arcadedb.exception.ConfigurationException;

/**
 * Thrown by a targeted leadership transfer that Ratis refused because another transfer, to a different peer, is
 * already pending on this leader (issue #8557).
 * <p>
 * A distinct type because it says nothing about the candidate: every other candidate would be refused the same way
 * until the pending transfer completes. A caller walking a list of candidates must stop at it, and must above all not
 * fall back to the bare no-target step-down, which Ratis does not hold back for the pending transfer and which would
 * leave the cluster leaderless for an election timeout. It stays a {@link ConfigurationException} so every caller that
 * only reports a failed transfer maps it exactly as before.
 */
public class LeadershipTransferInProgressException extends ConfigurationException {

  public LeadershipTransferInProgressException(final String targetPeerId, final Exception cause) {
    super("Failed to transfer leadership to " + targetPeerId + ": another leadership transfer is already in progress on "
        + "this node (" + cause.getMessage() + ")", cause);
  }
}
