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
package com.arcadedb.gremlin.io;

import com.arcadedb.database.BasicDatabase;
import com.arcadedb.database.RID;
import org.apache.tinkerpop.gremlin.structure.io.AbstractIoRegistry;

import java.util.Map;

@SuppressWarnings("serial")
public class ArcadeIoRegistry extends AbstractIoRegistry {
  public static final  String           BUCKET_ID       = "bucketId";
  public static final  String           BUCKET_POSITION = "bucketPosition";
  private static final ArcadeIoRegistry INSTANCE        = new ArcadeIoRegistry();

  private final BasicDatabase database;

  public ArcadeIoRegistry() {
    this(null);
  }

  public ArcadeIoRegistry(final BasicDatabase database) {
    this.database = database;
  }

  public BasicDatabase getDatabase() {
    return database;
  }

  public RID newRID(final Object obj) {
    return newRID(this.database, obj);
  }

  public static RID newRID(final BasicDatabase database, final Object obj) {
    return switch (obj) {
      case null -> null;
      case RID rid -> rid;
      case String s -> new RID(database, s);
      case Map map -> {
        final Map<String, Number> map2 = map;
        yield new RID(database, map2.get(BUCKET_ID).intValue(), map2.get(BUCKET_POSITION).longValue());
      }
      default -> throw new IllegalArgumentException("Unable to convert unknown (" + obj.getClass() + ") type to RID");
    };

  }

  public static boolean isRID(final Object result) {
    if (!(result instanceof Map))
      return false;

    @SuppressWarnings("unchecked")
    final Map<String, Number> map = (Map<String, Number>) result;
    return map.containsKey(BUCKET_ID) && map.containsKey(BUCKET_POSITION);
  }

  public static ArcadeIoRegistry instance() {
    return INSTANCE;
  }
}
