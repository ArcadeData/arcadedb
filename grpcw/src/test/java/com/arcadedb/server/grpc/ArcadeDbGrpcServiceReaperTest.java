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
 * Unit test for the idle-transaction reaper configuration wiring of {@link ArcadeDbGrpcService} (issue #4802).
 * No server is required: the constructor only decides whether to start the reaper thread based on the thresholds.
 */
class ArcadeDbGrpcServiceReaperTest {

  @Test
  void reaperDisabledWhenBothBoundsNonPositive() {
    final ArcadeDbGrpcService service = new ArcadeDbGrpcService("/tmp/notused", null, 0L, 0L, 30_000L);
    try {
      assertThat(service.isIdleReaperActive()).isFalse();
      assertThat(service.getActiveTransactionCount()).isZero();
    } finally {
      service.close();
    }
  }

  @Test
  void reaperDisabledWhenPeriodNonPositive() {
    final ArcadeDbGrpcService service = new ArcadeDbGrpcService("/tmp/notused", null, 1_000L, 1_000L, 0L);
    try {
      assertThat(service.isIdleReaperActive()).isFalse();
    } finally {
      service.close();
    }
  }

  @Test
  void reaperEnabledWithIdleBoundOnly() {
    final ArcadeDbGrpcService service = new ArcadeDbGrpcService("/tmp/notused", null, 1_000L, 0L, 500L);
    try {
      assertThat(service.isIdleReaperActive()).isTrue();
    } finally {
      service.close();
    }
  }

  @Test
  void reaperEnabledWithAgeBoundOnly() {
    final ArcadeDbGrpcService service = new ArcadeDbGrpcService("/tmp/notused", null, 0L, 1_000L, 500L);
    try {
      assertThat(service.isIdleReaperActive()).isTrue();
    } finally {
      service.close();
    }
  }

  @Test
  void defaultConstructorEnablesReaper() {
    final ArcadeDbGrpcService service = new ArcadeDbGrpcService("/tmp/notused", null);
    try {
      assertThat(service.isIdleReaperActive()).isTrue();
    } finally {
      service.close();
    }
  }
}
