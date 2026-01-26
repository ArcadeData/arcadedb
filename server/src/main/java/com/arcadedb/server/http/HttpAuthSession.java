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
package com.arcadedb.server.http;

import com.arcadedb.server.security.ServerSecurityUser;

/**
 * Represents an authenticated HTTP session. Unlike {@link HttpSession} which manages a transaction,
 * this session represents a logged-in user and can be used for token-based authentication.
 * <p>
 * The session token can be used in place of username/password for subsequent HTTP requests.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 * @see <a href="https://github.com/ArcadeData/arcadedb/issues/1691">GitHub Issue #1691</a>
 */
public class HttpAuthSession {
  public final  String             token;
  public final  ServerSecurityUser user;
  private final long               createdAt;
  private volatile long            lastUpdate;
  private final String             sourceIp;
  private final String             userAgent;
  private final String             country;
  private final String             city;

  public HttpAuthSession(final ServerSecurityUser user, final String token) {
    this(user, token, null, null, null, null);
  }

  public HttpAuthSession(final ServerSecurityUser user, final String token, final String sourceIp,
      final String userAgent, final String country, final String city) {
    this.user = user;
    this.token = token;
    this.createdAt = System.currentTimeMillis();
    this.lastUpdate = this.createdAt;
    this.sourceIp = sourceIp;
    this.userAgent = userAgent;
    this.country = country;
    this.city = city;
  }

  public long elapsedFromLastUpdate() {
    return System.currentTimeMillis() - lastUpdate;
  }

  public void touch() {
    this.lastUpdate = System.currentTimeMillis();
  }

  public ServerSecurityUser getUser() {
    return user;
  }

  public String getToken() {
    return token;
  }

  public long getCreatedAt() {
    return createdAt;
  }

  public long getLastUpdate() {
    return lastUpdate;
  }

  public String getSourceIp() {
    return sourceIp;
  }

  public String getUserAgent() {
    return userAgent;
  }

  public String getCountry() {
    return country;
  }

  public String getCity() {
    return city;
  }
}
