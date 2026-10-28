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
package com.arcadedb.engine.block;

import com.arcadedb.GlobalConfiguration;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class BlockCacheConfigurationTest {

  @Test
  void defaultScalesWithTheHeapWithAFloor() {
    GlobalConfiguration.BLOCK_CACHE_MAX_BYTES.reset();
    final long floor = 16L * 1024 * 1024;
    final long maxHeap = Runtime.getRuntime().maxMemory();
    final long expected = maxHeap == Long.MAX_VALUE ? floor : Math.max(floor, maxHeap / 32);
    assertThat(GlobalConfiguration.BLOCK_CACHE_MAX_BYTES.getValueAsLong()).isEqualTo(expected);
  }

  @Test
  void instanceIsASingletonReadingTheSetting() {
    try {
      assertThat(BlockCache.instance()).isSameAs(BlockCache.instance());
      final BlockCache cache = BlockCache.instance();
      cache.clear();
      GlobalConfiguration.BLOCK_CACHE_MAX_BYTES.setValue(0L);
      final long before = cache.usedBytes();
      cache.get(cache.newFileId(), 1, 0, () -> "v", v -> 10L);
      assertThat(cache.usedBytes()).isEqualTo(before);
    } finally {
      GlobalConfiguration.BLOCK_CACHE_MAX_BYTES.reset();
      BlockCache.instance().clear();
    }
  }

  @Test
  void settingResolvesByKeyAndIsJvmScoped() {
    assertThat(GlobalConfiguration.findByKey("arcadedb.blockCache.maxBytes")).isEqualTo(GlobalConfiguration.BLOCK_CACHE_MAX_BYTES);
    assertThat(GlobalConfiguration.BLOCK_CACHE_MAX_BYTES.getScope()).isEqualTo(GlobalConfiguration.SCOPE.JVM);
    assertThat(GlobalConfiguration.BLOCK_CACHE_MAX_BYTES.getType()).isEqualTo(Long.class);
  }
}
