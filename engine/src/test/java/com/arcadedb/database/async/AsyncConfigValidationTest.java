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
package com.arcadedb.database.async;

import com.arcadedb.TestHelper;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for #4961: {@code setCommitEvery(0)} was accepted and made every subsequent task
 * fail with an {@code ArithmeticException} on {@code count % commitEvery}. The setter must reject
 * non-positive values.
 */
class AsyncConfigValidationTest extends TestHelper {

  @Test
  void setCommitEveryRejectsNonPositiveValues() {
    assertThatThrownBy(() -> database.async().setCommitEvery(0))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> database.async().setCommitEvery(-5))
        .isInstanceOf(IllegalArgumentException.class);

    database.async().setCommitEvery(100);
    assertThat(database.async().getCommitEvery()).isEqualTo(100);
  }
}
