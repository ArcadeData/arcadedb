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
package com.arcadedb.index.columnar;

import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class RowBufferTest {

  /**
   * A present ORDINAL_LIST with count 0 is valid for {@link RowValues#check}, and {@link RowBuffer#put} allocates no array for it:
   * the remap of a dictionary compaction must skip it rather than sort a null list.
   */
  @Test
  void remapOrdinalsSkipsAPresentEmptyList() {
    final ColumnDefinition tags = new ColumnDefinition(7, "tags", Type.STRING, ColumnKind.ORDINAL_LIST, true, null);
    final RowBuffer rows = new RowBuffer(64, new ColumnDefinition[] { tags });
    final RowValues row = new RowValues(1);
    row.present[0] = true;
    row.listCount[0] = 0;
    rows.put(3, row);

    rows.remapOrdinals(0, ordinal -> ordinal + 1);

    assertThat(rows.present(0, 3)).isTrue();
    assertThat(rows.listCount(0, 3)).isZero();
  }
}
