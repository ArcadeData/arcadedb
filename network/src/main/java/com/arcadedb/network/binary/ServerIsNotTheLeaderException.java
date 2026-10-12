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

import com.arcadedb.exception.CauseChain;
import com.arcadedb.exception.ErrorCategory;
import com.arcadedb.exception.NeedRetryException;

public class ServerIsNotTheLeaderException extends NeedRetryException {

  private final String leaderURL;

  public ServerIsNotTheLeaderException(final String message, final String leaderURL) {
    super(message);
    this.leaderURL = leaderURL;
  }

  public String getLeaderAddress() {
    return leaderURL;
  }

  /**
   * Whether this refusal names a leader a client could be sent to. One raised mid-election names none ({@code null} or
   * blank), and is the ordinary retryable condition its {@link NeedRetryException} ancestry says it is: the same request
   * succeeds once the election settles.
   */
  public boolean namesLeader() {
    return leaderURL != null && !leaderURL.isBlank();
  }

  /**
   * The leader refusal a wire protocol must answer as a redirect rather than as a retryable conflict, or {@code null}
   * when {@code error} is anything else (issue #9845).
   * <p>
   * Found anywhere along the cause chain, because the engine wraps what it raises (a commit failure, a planner
   * failure). Only when {@link ErrorCategory#of} reads the chain as {@link ErrorCategory#RETRY}, so a chain the
   * category ranks above it - a statement terminated on request - keeps that verdict. And only when the refusal names
   * a leader: without an address there is nowhere to redirect to, and the retryable answer every transport already
   * gives is the right one, which is what the HTTP handler does for that case too.
   */
  public static ServerIsNotTheLeaderException redirectIn(final Throwable error) {
    if (ErrorCategory.of(error) != ErrorCategory.RETRY)
      return null;
    final ServerIsNotTheLeaderException refusal = CauseChain.find(error, ServerIsNotTheLeaderException.class);
    return refusal != null && refusal.namesLeader() ? refusal : null;
  }

  /**
   * {@code "The leader is server '<address>'"}, the sentence a wire protocol appends to its error text for a refusal
   * {@link #redirectIn} found, so every transport words it the same way. Control characters in the address are replaced
   * by spaces: the text goes into one-line (RESP) and NUL-terminated (Postgres) fields, and an address must not end
   * either early.
   */
  public String leaderSentence() {
    final char[] chars = String.valueOf(leaderURL).toCharArray();
    for (int i = 0; i < chars.length; i++)
      if (Character.isISOControl(chars[i]))
        chars[i] = ' ';
    return "The leader is server '" + new String(chars) + "'";
  }

  @Override
  public String toString() {
    return super.toString() + ". The leader is server " + leaderURL;
  }
}
