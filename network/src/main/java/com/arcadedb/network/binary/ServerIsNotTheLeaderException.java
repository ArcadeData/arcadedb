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

  @Override
  public String toString() {
    return super.toString() + ". The leader is server " + leaderURL;
  }
}
