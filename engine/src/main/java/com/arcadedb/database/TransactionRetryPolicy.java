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
package com.arcadedb.database;

import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.exception.NeedRetryException;

/**
 * The one decision every transaction retry loop takes after a failed attempt: is the failure worth another attempt? The
 * loops are {@code LocalDatabase.transaction}, {@code RemoteDatabase.transaction}, {@code db.async().transaction()}, the
 * async executor's batch commit and the SQL {@code COMMIT RETRY} step. Each one used to answer it with a catch clause of its
 * own, and two of them caught only {@code ConcurrentModificationException}, so a {@code LockTimeoutException} from the
 * commit's own file locks - a {@link NeedRetryException} like every other contention error - was never retried there
 * although both javadocs promised it (issue #10759). A loop now asks {@link #isRetryable} which failures enter its retry
 * path and {@link #mayRetry} whether one more attempt may follow, so a new retryable exception is retried by every loop or
 * by none.
 * <p>
 * The family is {@link NeedRetryException} and its subclasses, plus {@link DuplicatedKeyException}. The rules on top:
 * <ul>
 *   <li>a duplicated key is retried ONCE (#4959): a genuine duplicate fails identically on every attempt, and only a commit
 *   racing in between attempts can turn it into a success, which one retry is enough to tell;</li>
 *   <li>on the node that refused it, a {@link NeedRetryException} that holds there until something outside the node
 *   changes ({@link NeedRetryException#isRetryableOnThisNode()} false, a follower naming the leader) is not retried
 *   (#9995). A client that can go elsewhere still retries it there.</li>
 * </ul>
 * One instance per run of a loop: it remembers the duplicated-key retry. Not thread-safe, a loop runs on one thread.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class TransactionRetryPolicy {
  private final boolean onThisNode;
  private       boolean duplicatedKeyRetried;

  private TransactionRetryPolicy(final boolean onThisNode) {
    this.onThisNode = onThisNode;
  }

  /** The policy of a loop that runs the work on the node that refused it: the embedded and server-side loops. */
  public static TransactionRetryPolicy onThisNode() {
    return new TransactionRetryPolicy(true);
  }

  /** The policy of a client loop, which can send the work to another node: a node-bound refusal is retried there. */
  public static TransactionRetryPolicy fromClient() {
    return new TransactionRetryPolicy(false);
  }

  /** Whether {@code e} belongs to the family a transaction retry loop retries. */
  public static boolean isRetryable(final Throwable e) {
    return e instanceof NeedRetryException || e instanceof DuplicatedKeyException;
  }

  /**
   * Whether another attempt may follow the one that failed with {@code e}, attempts left permitting. Records the one retry a
   * duplicated key is given, so call it once per failed attempt, and only when the loop is about to retry otherwise.
   */
  public boolean mayRetry(final Throwable e) {
    if (e instanceof NeedRetryException retryable)
      return !onThisNode || retryable.isRetryableOnThisNode();
    if (e instanceof DuplicatedKeyException) {
      if (duplicatedKeyRetried)
        return false;
      duplicatedKeyRetried = true;
      return true;
    }
    return false;
  }
}
