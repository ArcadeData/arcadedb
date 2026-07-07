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
package com.arcadedb.exception;

/**
 * Thrown when a transaction IS durably committed cluster-wide (the replication quorum accepted it) but the
 * local apply failed afterwards (#5064). The data exists on the cluster and will be present locally after
 * reconciliation or restart.
 * <p>
 * <b>Do NOT retry the transaction</b>: a retry would apply the changes a second time (for inserts, creating
 * duplicates). Treat this as a commit that succeeded remotely and continue; reloading held records is
 * optional (for freshness) - their in-memory content already matches what the cluster committed. The
 * identities of records created in the failed-locally transaction remain valid - they are the identities the
 * cluster committed.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class TransactionCommittedRemotelyException extends TransactionException {
  /** Message-only form, used when reconstructing the exception from a wire response (forwarded writes). */
  public TransactionCommittedRemotelyException(final String message) {
    super(message);
  }

  public TransactionCommittedRemotelyException(final String message, final Throwable cause) {
    super(message, cause);
  }
}
