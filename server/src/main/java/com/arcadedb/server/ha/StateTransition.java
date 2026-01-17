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
package com.arcadedb.server.ha;

/**
 * Records a state transition for historical tracking.
 */
public class StateTransition {
  private final Leader2ReplicaNetworkExecutor.STATUS fromStatus;
  private final Leader2ReplicaNetworkExecutor.STATUS toStatus;
  private final long timestampMs;

  public StateTransition(Leader2ReplicaNetworkExecutor.STATUS fromStatus,
                         Leader2ReplicaNetworkExecutor.STATUS toStatus,
                         long timestampMs) {
    this.fromStatus = fromStatus;
    this.toStatus = toStatus;
    this.timestampMs = timestampMs;
  }

  public Leader2ReplicaNetworkExecutor.STATUS getFromStatus() {
    return fromStatus;
  }

  public Leader2ReplicaNetworkExecutor.STATUS getToStatus() {
    return toStatus;
  }

  public long getTimestampMs() {
    return timestampMs;
  }

  @Override
  public String toString() {
    return fromStatus + " -> " + toStatus + " at " + timestampMs;
  }
}
