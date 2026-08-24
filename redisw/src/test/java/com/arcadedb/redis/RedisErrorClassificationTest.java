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
package com.arcadedb.redis;

import com.arcadedb.exception.ArithmeticErrorException;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.exception.LockTimeoutException;
import com.arcadedb.exception.TransactionException;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * RESP error replies used to go out as a bare {@code -<message>} with no kind word, so a client had nothing to
 * branch on and a retryable conflict - the one failure worth repeating the command for - was indistinguishable
 * from a permanent one. See issue #5628.
 */
class RedisErrorClassificationTest {

  @Test
  void aConflictTellsTheClientToTryAgain() {
    assertThat(RedisNetworkExecutor.respErrorPrefix(new ConcurrentModificationException("page version changed")))
        .isEqualTo("TRYAGAIN");
    assertThat(RedisNetworkExecutor.respErrorPrefix(new LockTimeoutException("lock timeout"))).isEqualTo("TRYAGAIN");
    assertThat(RedisNetworkExecutor.respErrorPrefix(
        new TransactionException("commit failed", new ConcurrentModificationException("conflict")))).isEqualTo("TRYAGAIN");
  }

  @Test
  void aSecurityDenialUsesRedisPermissionKind() {
    assertThat(RedisNetworkExecutor.respErrorPrefix(new SecurityException("denied"))).isEqualTo("NOPERM");
  }

  @Test
  void everythingElseKeepsTheGenericKind() {
    // RESP has no vocabulary for the client-error categories Postgres and Bolt distinguish, so they share ERR.
    assertThat(RedisNetworkExecutor.respErrorPrefix(new ArithmeticErrorException("/ by zero"))).isEqualTo("ERR");
    assertThat(RedisNetworkExecutor.respErrorPrefix(new CommandExecutionException("something broke"))).isEqualTo("ERR");
    assertThat(RedisNetworkExecutor.respErrorPrefix(new IOException("disk gone"))).isEqualTo("ERR");
  }

  @Test
  void anExplicitKindWinsOverTheDefaultErrCategory() {
    // Issue #6560: a RedisException whose message already baked in a kind word (e.g. "WRONGPASS ...") used to be
    // reported with the generic "ERR" kind anyway, since none of WRONGPASS/NOAUTH/NOPROTO/NOPERM's call sites
    // raise a real java.lang.SecurityException that ErrorCategory.of() would recognise - the wire reply came out
    // as "-ERR WRONGPASS ..." (or "-ERR NOAUTH ...", "-ERR NOPROTO ...", "-ERR NOPERM ..."), so the RESP error
    // *kind* (the token right after '-') a client can branch on was always ERR, never the specific one.
    assertThat(RedisNetworkExecutor.respErrorPrefix(RedisException.withKind("WRONGPASS", "bad credentials")))
        .isEqualTo("WRONGPASS");
    assertThat(RedisNetworkExecutor.respErrorPrefix(RedisException.withKind("NOAUTH", "Authentication required.")))
        .isEqualTo("NOAUTH");
    assertThat(RedisNetworkExecutor.respErrorPrefix(RedisException.withKind("NOPROTO", "unsupported protocol version")))
        .isEqualTo("NOPROTO");
    assertThat(RedisNetworkExecutor.respErrorPrefix(RedisException.withKind("NOPERM", "no permission")))
        .isEqualTo("NOPERM");
  }

  @Test
  void aPlainRedisExceptionWithoutAnExplicitKindKeepsTheDefault() {
    // A RedisException that never called withKind() (e.g. "syntax error", "Key 'x' is not a number") still falls
    // back to the ErrorCategory-derived default, exactly as before this exception carried a kind at all.
    assertThat(RedisNetworkExecutor.respErrorPrefix(new RedisException("syntax error"))).isEqualTo("ERR");
  }

  @Test
  void anEmptyMessageStillNamesTheFailure() {
    // A bare `-` reply, or `-null`, tells the client nothing at all.
    assertThat(RedisNetworkExecutor.respErrorMessage(new IllegalStateException())).isEqualTo("IllegalStateException");
    assertThat(RedisNetworkExecutor.respErrorMessage(new IllegalStateException(""))).isEqualTo("IllegalStateException");
  }

  @Test
  void aMultiLineMessageCannotEndTheReplyEarly() {
    // A RESP simple error is one line: an embedded CR or LF would terminate the reply and leave the remainder to
    // be read as the start of the next one, desynchronising the connection.
    final String flattened = RedisNetworkExecutor.respErrorMessage(
        new CommandExecutionException("first line\r\nsecond line\nthird"));

    assertThat(flattened).doesNotContain("\r").doesNotContain("\n");
    assertThat(flattened).isEqualTo("first line  second line third");
  }
}
