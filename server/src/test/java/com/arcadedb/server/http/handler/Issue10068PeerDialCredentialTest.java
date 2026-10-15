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
package com.arcadedb.server.http.handler;

import com.arcadedb.server.PeerRequest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.net.http.HttpRequest;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10068: the batch forward builds its request through {@link PeerRequest}, so it presents the credential the way every
 * other peer dial does and owes the operator the same notice when it goes out in clear text on an SSL cluster. It spelled the pair
 * by hand, and was silent about the fallback.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10068PeerDialCredentialTest {

  private static final String PLAIN_URL     = "http://leader:2480/api/v1/batch/db";
  private static final String ENCRYPTED_URL = "https://leader:2490/api/v1/batch/db";

  @BeforeEach
  @AfterEach
  void rearmTheNotice() {
    PeerRequest.rearmForTests();
  }

  private static HttpRequest batchForward(final String url, final String token, final boolean sslEnabled) {
    return PostBatchHandler.buildForwardRequest(url, "application/json", token, "reader", 0L, new ByteArrayInputStream(new byte[0]),
        null, null, "peer-1", sslEnabled);
  }

  @Test
  void theBatchForwardPresentsTheTokenWithThePrincipalItActsFor() {
    final HttpRequest request = batchForward(PLAIN_URL, "s3cr3t", false);

    assertThat(request.headers().firstValue(PeerRequest.CLUSTER_TOKEN_HEADER)).hasValue("s3cr3t");
    assertThat(request.headers().firstValue(PeerRequest.FORWARDED_USER_HEADER)).hasValue("reader");
    assertThat(request.headers().firstValue("Content-Type")).hasValue("application/json");
  }

  @Test
  void aPlainBatchForwardOnAnSslClusterSaysTheNotice() {
    batchForward(PLAIN_URL, "s3cr3t", true);

    assertThat(PeerRequest.saidForTests()).isTrue();
  }

  @Test
  void anEncryptedOrNonSslBatchForwardSaysNothing() {
    batchForward(ENCRYPTED_URL, "s3cr3t", true);
    batchForward(PLAIN_URL, "s3cr3t", false);

    assertThat(PeerRequest.saidForTests()).isFalse();
  }
}
