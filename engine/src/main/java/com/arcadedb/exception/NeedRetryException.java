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

public class NeedRetryException extends ArcadeDBException {
  // Issue #8617: how long the side that refused asked the caller to wait before retrying, 0 when it did not say
  private long retryAfterMs;

  public NeedRetryException(final String s) {
    super(s);
  }

  public NeedRetryException(final String s, final Throwable e) {
    super(s, e);
  }

  /**
   * How long, in milliseconds, the side that refused the operation asked the caller to wait before retrying it, or 0 when
   * it did not say - a server answering {@code 503} with a {@code Retry-After} header, such as a node installing a
   * snapshot. A retry loop waits at least this long, bounded by its own cap, instead of retrying at once and spending its
   * whole budget before the refusing side is ready (issue #8617).
   */
  public long getRetryAfterMs() {
    return retryAfterMs;
  }

  public void setRetryAfterMs(final long retryAfterMs) {
    this.retryAfterMs = Math.max(0L, retryAfterMs);
  }
}
