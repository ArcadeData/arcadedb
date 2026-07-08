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
package com.arcadedb.server.grpc;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * SEC-7: the concurrent-transaction reservation logic that bounds per-transaction executor allocation. Verified
 * directly against the reserve/release accounting so the global and per-principal caps are exercised without a server.
 */
class ArcadeDbGrpcServiceTransactionLimitTest {

  private ArcadeDbGrpcService service(final int maxGlobal, final int maxPerPrincipal) {
    // Reaper disabled (idle/age/period all zero) so no background thread is started.
    return new ArcadeDbGrpcService("/tmp/notused", null, 0L, 0L, 0L, maxGlobal, maxPerPrincipal);
  }

  @Test
  void globalCapRejectsBeyondLimitAndAdmitsAfterRelease() {
    final ArcadeDbGrpcService service = service(2, 0);
    try {
      assertThat(service.tryReserveTransactionSlot("a")).isTrue();
      assertThat(service.tryReserveTransactionSlot("b")).isTrue();
      // Global limit (2) reached: a third slot for any principal is refused.
      assertThat(service.tryReserveTransactionSlot("c")).isFalse();

      // Releasing one slot admits the next reservation.
      service.releaseTransactionSlot("a");
      assertThat(service.tryReserveTransactionSlot("c")).isTrue();
    } finally {
      service.close();
    }
  }

  @Test
  void perPrincipalCapIsIndependentPerOwner() {
    final ArcadeDbGrpcService service = service(0, 2);
    try {
      assertThat(service.tryReserveTransactionSlot("alice")).isTrue();
      assertThat(service.tryReserveTransactionSlot("alice")).isTrue();
      // alice reached her per-principal cap (2).
      assertThat(service.tryReserveTransactionSlot("alice")).isFalse();
      assertThat(service.getTransactionCountForPrincipal("alice")).isEqualTo(2);

      // A different principal is unaffected by alice's usage.
      assertThat(service.tryReserveTransactionSlot("bob")).isTrue();
      assertThat(service.getTransactionCountForPrincipal("bob")).isEqualTo(1);

      // A rejected reservation must not leak into the per-principal count.
      service.tryReserveTransactionSlot("alice");
      assertThat(service.getTransactionCountForPrincipal("alice")).isEqualTo(2);
    } finally {
      service.close();
    }
  }

  @Test
  void releaseDecrementsPerPrincipalCount() {
    final ArcadeDbGrpcService service = service(0, 5);
    try {
      service.tryReserveTransactionSlot("alice");
      service.tryReserveTransactionSlot("alice");
      assertThat(service.getTransactionCountForPrincipal("alice")).isEqualTo(2);

      service.releaseTransactionSlot("alice");
      assertThat(service.getTransactionCountForPrincipal("alice")).isEqualTo(1);
    } finally {
      service.close();
    }
  }

  @Test
  void nonPositiveCapsDisableBounds() {
    final ArcadeDbGrpcService service = service(0, 0);
    try {
      for (int i = 0; i < 50; i++)
        assertThat(service.tryReserveTransactionSlot("flooder")).isTrue();
      assertThat(service.getTransactionCountForPrincipal("flooder")).isEqualTo(50);
    } finally {
      service.close();
    }
  }

  @Test
  void anonymousPrincipalIsCappedUnderNullOwner() {
    final ArcadeDbGrpcService service = service(0, 1);
    try {
      assertThat(service.tryReserveTransactionSlot(null)).isTrue();
      assertThat(service.tryReserveTransactionSlot(null)).isFalse();
      assertThat(service.getTransactionCountForPrincipal(null)).isEqualTo(1);
    } finally {
      service.close();
    }
  }
}
