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
 * Thrown when an operation could not acquire a lock within its timeout (e.g. file locks during a
 * commit, or the per-session HTTP lock). This is a <b>contention</b> timeout, not a deadline/budget
 * timeout: the work itself never started because another thread held the lock, so the operation is
 * transient and safe to retry once the contention clears. It therefore extends {@link NeedRetryException}
 * so the engine's existing retry loops (transaction retry, HTTP 503 retry-after, Raft apply retry)
 * pick it up automatically.
 * <p>
 * Do NOT use this for query/iteration deadline timeouts ({@code TimeoutStep}, {@code MultiIterator},
 * the SQL {@code TIMEOUT} clause): those mean "this took too long, stop" and must not be retried -
 * keep using {@link TimeoutException} for them.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class LockTimeoutException extends NeedRetryException {
  public LockTimeoutException(final String message) {
    super(message);
  }

  public LockTimeoutException(final String message, final Throwable cause) {
    super(message, cause);
  }
}
