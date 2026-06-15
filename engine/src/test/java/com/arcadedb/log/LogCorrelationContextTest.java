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
package com.arcadedb.log;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class LogCorrelationContextTest {

  @AfterEach
  void clear() {
    LogManager.instance().clearCorrelation();
    LogManager.instance().setTraceContextSupplier(null);
  }

  @Test
  void storesAndReadsCorrelationFields() {
    LogManager.instance().setCorrelation("req-1", "mydb", "trace-abc", "span-xyz");
    assertThat(LogManager.instance().getRequestId()).isEqualTo("req-1");
    assertThat(LogManager.instance().getDatabaseContext()).isEqualTo("mydb");
    assertThat(LogManager.instance().getTraceId()).isEqualTo("trace-abc");
    assertThat(LogManager.instance().getSpanId()).isEqualTo("span-xyz");
  }

  @Test
  void clearRemovesAllFields() {
    LogManager.instance().setCorrelation("req-1", "mydb", "trace-abc", "span-xyz");
    LogManager.instance().clearCorrelation();
    assertThat(LogManager.instance().getRequestId()).isNull();
    assertThat(LogManager.instance().getDatabaseContext()).isNull();
    assertThat(LogManager.instance().getTraceId()).isNull();
    assertThat(LogManager.instance().getSpanId()).isNull();
    assertThat(LogManager.instance().getCorrelation()).isNull();
  }

  @Test
  void accessorsAreNullWhenNoCorrelationSet() {
    assertThat(LogManager.instance().getCorrelation()).isNull();
    assertThat(LogManager.instance().getRequestId()).isNull();
    assertThat(LogManager.instance().getTraceId()).isNull();
  }

  @Test
  void traceContextSupplierFeedsCurrentTraceContext() {
    LogManager.instance().setTraceContextSupplier(() -> new String[] { "tid", "sid" });
    final String[] ctx = LogManager.instance().currentTraceContext();
    assertThat(ctx).containsExactly("tid", "sid");
  }

  @Test
  void currentTraceContextIsNullWhenNoSupplier() {
    assertThat(LogManager.instance().currentTraceContext()).isNull();
  }

  @Test
  void currentTraceContextSwallowsSupplierFailure() {
    LogManager.instance().setTraceContextSupplier(() -> {
      throw new RuntimeException("boom");
    });
    assertThat(LogManager.instance().currentTraceContext()).isNull();
  }
}
