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

import java.io.IOException;

/**
 * The leader answered {@code 404} for a database's snapshot: it does not hold that database REGISTERED (issue #8559).
 * {@link SnapshotHttpHandler} answers so for every name {@code server.existsDatabase} rejects, which covers a database
 * the leader never had, one the cluster dropped, and one closed on the leader too.
 * <p>
 * Kept apart from every other download failure because it is a verdict about the cluster rather than about the
 * transfer: retrying against the same leader answers the same, so a caller that treats it as a failed install to be
 * retried - a quarantine that holds the node out of the ready set until an install succeeds - waits for good. It is
 * the same fact the auto-acquire reconcile reports as {@link DatabaseReconciler.AcquireState#LEADER_MISSING}.
 */
class LeaderDoesNotHoldDatabaseException extends IOException {
  LeaderDoesNotHoldDatabaseException(final String message) {
    super(message);
  }

  LeaderDoesNotHoldDatabaseException(final String message, final Throwable cause) {
    super(message, cause);
  }
}
