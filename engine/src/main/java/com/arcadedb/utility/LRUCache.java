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
package com.arcadedb.utility;

import java.io.*;
import java.util.*;

/**
 * The simplest LRU cache implementation in Java. Not thread safe, wrap it in Collections.synchronizedMap() for thread-safety.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class LRUCache<K, V> extends LinkedHashMap<K, V> {

  @Serial
  private static final long serialVersionUID = 0;

  final private int cacheSize;

  public LRUCache(final int iCacheSize) {
    super(Math.max(16, (int) (iCacheSize / 0.75) + 1), 0.75f, true);
    this.cacheSize = iCacheSize;
  }

  protected boolean removeEldestEntry(final Map.Entry<K, V> eldest) {
    return size() >= cacheSize;
  }
}
