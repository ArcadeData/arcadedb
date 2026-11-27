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
package com.arcadedb.index;

import com.arcadedb.exception.ConcurrentModificationException;

/**
 * An index read failed because a concurrent DDL dropped or rebuilt the index under it, or is changing it right now: the
 * request was fine and the same request run again succeeds. An {@link IndexException}, so every reader that catches one to
 * read again without the index (a SELECT or MATCH planned again, issues #9331 and #9615, a lookup that falls back to a scan)
 * keeps doing so. A write cannot be planned again halfway through, as it may already have written: it answers with
 * {@link #asWriteConflict()} instead, the retryable conflict a retry loop runs again whole (issue #11835).
 * <p>
 * The other {@link IndexException}s (a key too large for a page, a property indexed BY ITEM that is not a list) say
 * something about the request or the data, which a retry does not change, and are not of this class.
 */
public class StaleIndexException extends IndexException {
  public StaleIndexException(final String message) {
    super(message);
  }

  public StaleIndexException(final String message, final Throwable cause) {
    super(message, cause);
  }

  /**
   * The {@link ConcurrentModificationException} a write that read through the stale index answers with: HTTP 503, gRPC
   * ABORTED, and run again by {@code Database.transaction(..., retries)} and {@code COMMIT RETRY}.
   */
  public ConcurrentModificationException asWriteConflict() {
    return new ConcurrentModificationException(getMessage() + ": the index was changed by a concurrent DDL, run the statement again",
        this);
  }
}
