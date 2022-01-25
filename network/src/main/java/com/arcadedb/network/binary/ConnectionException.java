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
package com.arcadedb.network.binary;

public class ConnectionException extends RuntimeException {
  private final String url;
  private final String reason;

  public ConnectionException(final String url, final Throwable e) {
    super("Error on connecting to server '" + url + "' (cause=" + e.toString() + ")", e);
    this.url = url;
    this.reason = e.toString();
  }

  public ConnectionException(final String url, final String reason) {
    super("Error on connecting to server '" + url + "' (cause=" + reason + ")");
    this.url = url;
    this.reason = reason;
  }

  public String getUrl() {
    return url;
  }

  public String getReason() {
    return reason;
  }
}
