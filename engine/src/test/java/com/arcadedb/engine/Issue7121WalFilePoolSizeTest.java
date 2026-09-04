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
package com.arcadedb.engine;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #7121: {@code arcadedb.txWalFiles} documents {@code 0 = available cores}, but nothing translated 0. Setting
 * the value the description advertises produced a zero-length WAL file pool, and the first commit then died on
 * {@code threadId() % 0}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7121WalFilePoolSizeTest {
  private static final int CORES = Math.max(Runtime.getRuntime().availableProcessors(), 1);

  @Test
  void zeroResolvesToAvailableCores() {
    final ContextConfiguration configuration = new ContextConfiguration();
    configuration.setValue(GlobalConfiguration.TX_WAL_FILES, 0);
    assertThat(TransactionManager.walFilePoolSize(configuration))
        .as("`0 = available cores` is what the setting documents")
        .isEqualTo(CORES);
  }

  @Test
  void aNegativeValueResolvesTheSameWayInsteadOfThrowing() {
    final ContextConfiguration configuration = new ContextConfiguration();
    configuration.setValue(GlobalConfiguration.TX_WAL_FILES, -3);
    assertThat(TransactionManager.walFilePoolSize(configuration))
        .as("must not reach `new WALFile[-3]`")
        .isEqualTo(CORES);
  }

  @Test
  void anExplicitPositiveValueIsHonoured() {
    final ContextConfiguration configuration = new ContextConfiguration();
    configuration.setValue(GlobalConfiguration.TX_WAL_FILES, 3);
    assertThat(TransactionManager.walFilePoolSize(configuration)).isEqualTo(3);
  }
}
