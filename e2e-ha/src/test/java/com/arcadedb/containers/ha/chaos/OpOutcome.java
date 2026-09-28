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

package com.arcadedb.containers.ha.chaos;

import java.io.IOException;

/**
 * Maps an HTTP answer to a ledger outcome, conservatively: only answers that prove the write never reached the Raft
 * log are FAILED. Any 4xx/5xx other than authentication, and any error after the request was sent, is UNKNOWN,
 * because the server may have appended the entry before failing.
 */
public final class OpOutcome {
  private OpOutcome() {
  }

  public static byte fromStatus(final int status) {
    if (status >= 200 && status < 300)
      return Ledger.ACKED;
    if (status == 401 || status == 403)
      return Ledger.FAILED;
    return Ledger.UNKNOWN;
  }

  public static byte fromException(final IOException exception) {
    return exception instanceof ChaosHttp.NotSentException ? Ledger.FAILED : Ledger.UNKNOWN;
  }
}
