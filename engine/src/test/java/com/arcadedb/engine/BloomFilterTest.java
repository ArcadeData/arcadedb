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

import com.arcadedb.database.Binary;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class BloomFilterTest {
  @Test
  public void testBloomFilter() {
    testValidity(new BufferBloomFilter(new Binary(1024 * 10), 8 * 8, 23), 100);
    testValidity(new BufferBloomFilter(new Binary(1024 * 10), 8 * 8, 23), 1000);
    testValidity(new BufferBloomFilter(new Binary(1024 * 1024), 8 * 8, 23), 100000);
  }

  private void testValidity(final BufferBloomFilter bf, final int count) {
    int might = 0;
    for (int i = 0; i < count; i++)
      if (bf.mightContain(i))
        ++might;

    assertThat(might < count).isTrue();

    for (int i = 0; i < count; i++)
      bf.add(i);

    for (int i = 0; i < count; i++)
      assertThat(bf.mightContain(i)).isTrue();
  }
}
