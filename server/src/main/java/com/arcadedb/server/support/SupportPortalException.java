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
package com.arcadedb.server.support;

/**
 * A failure of the support portal (or of the way to it), with a clear user-facing message. The message never carries the
 * Client key. {@link #getCode()} is the portal's error code ({@code invalid_key}, {@code client_mismatch},
 * {@code scope_denied}, {@code support_not_active}, {@code not_found}, {@code too_large}, {@code rate_limited},
 * {@code bad_request}) or one of ours: {@code portal_unreachable}, {@code portal_error}.
 */
public class SupportPortalException extends SupportException {
  private final int  portalStatus;
  private final long retryAfterSeconds;

  public SupportPortalException(final String code, final int portalStatus, final String message, final long retryAfterSeconds) {
    super(code, message);
    this.portalStatus = portalStatus;
    this.retryAfterSeconds = retryAfterSeconds;
  }

  /** The HTTP status the portal answered with, 0 when it did not answer. */
  public int getPortalStatus() {
    return portalStatus;
  }

  /** From the {@code Retry-After} header of a {@code rate_limited} answer, otherwise 0. */
  @Override
  public long getRetryAfterSeconds() {
    return retryAfterSeconds;
  }
}
