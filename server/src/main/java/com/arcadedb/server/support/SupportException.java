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
 * A failure of the support feature with a code Studio can switch on and a clear user-facing message (never carrying the
 * Client key): {@code not_registered} (409), {@code preview_not_found} (404), {@code bundle_too_large} (413),
 * {@code bad_request} (400), {@code preview_busy} (409), {@code support_stopped} (503), {@code internal_error} (500), and the ones of {@link SupportPortalException}.
 */
public class SupportException extends RuntimeException {
  private final String code;

  public SupportException(final String code, final String message) {
    super(message);
    this.code = code;
  }

  public String getCode() {
    return code;
  }

  /**
   * The HTTP status Studio's browser receives. 401 and 403 are never used: Studio reads a 401 as "the session of the user
   * expired" and logs out, while these failures are about the key held by the server, not about the user.
   */
  public int getStudioStatus() {
    return switch (code) {
      case "not_registered", "registered_by_settings", "config_not_writable", "preview_busy" -> 409;
      case "preview_not_found" -> 404;
      case "bundle_too_large", "too_large" -> 413;
      case "bad_request" -> 400;
      case "support_not_active" -> 402;
      case "not_found" -> 404;
      case "rate_limited" -> 429;
      case "portal_unreachable", "support_stopped" -> 503;
      case "internal_error" -> 500;
      default -> 502;
    };
  }

  public long getRetryAfterSeconds() {
    return 0L;
  }
}
