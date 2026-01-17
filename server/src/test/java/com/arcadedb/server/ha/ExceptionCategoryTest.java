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
package com.arcadedb.server.ha;

import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;

class ExceptionCategoryTest {

  @Test
  void testEnumValues() {
    ExceptionCategory[] categories = ExceptionCategory.values();

    assertThat(categories).hasSize(4);
    assertThat(categories).contains(
        ExceptionCategory.TRANSIENT_NETWORK,
        ExceptionCategory.LEADERSHIP_CHANGE,
        ExceptionCategory.PROTOCOL_ERROR,
        ExceptionCategory.UNKNOWN
    );
  }

  @Test
  void testEnumHasDisplayName() {
    assertThat(ExceptionCategory.TRANSIENT_NETWORK.getDisplayName())
        .isEqualTo("Transient Network Failure");
    assertThat(ExceptionCategory.LEADERSHIP_CHANGE.getDisplayName())
        .isEqualTo("Leadership Change");
  }
}
