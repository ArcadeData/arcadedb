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

import com.arcadedb.exception.ArcadeDBException;

/**
 * Exception thrown when an operation is attempted on a fenced leader.
 *
 * <p>A leader is fenced when a newer leader has been elected in the cluster.
 * Once fenced, the old leader must reject all write operations to prevent
 * split-brain data corruption.</p>
 *
 * <p>Clients receiving this exception should:</p>
 * <ol>
 *   <li>Stop sending requests to this server</li>
 *   <li>Refresh cluster topology to find the new leader</li>
 *   <li>Retry the operation on the new leader</li>
 * </ol>
 */
public class LeaderFencedException extends ArcadeDBException {

  /**
   * Creates a new LeaderFencedException with the specified message.
   *
   * @param message Description of the fencing condition
   */
  public LeaderFencedException(final String message) {
    super(message);
  }

  /**
   * Creates a new LeaderFencedException with the specified message and cause.
   *
   * @param message Description of the fencing condition
   * @param cause   The underlying cause
   */
  public LeaderFencedException(final String message, final Throwable cause) {
    super(message, cause);
  }
}
