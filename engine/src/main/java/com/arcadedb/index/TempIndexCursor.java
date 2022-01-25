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
package com.arcadedb.index;

import com.arcadedb.database.Identifiable;
import com.arcadedb.serializer.BinaryComparator;

import java.util.Collection;
import java.util.Iterator;

public class TempIndexCursor implements IndexCursor {
  private final Iterator<IndexCursorEntry> iterator;
  private final long                       size;
  private       IndexCursorEntry           current;

  public TempIndexCursor(final Collection<IndexCursorEntry> list) {
    this.iterator = list.iterator();
    this.size = list.size();
  }

  @Override
  public Object[] getKeys() {
    return current.keys;
  }

  @Override
  public Identifiable getRecord() {
    return current.record;
  }

  @Override
  public boolean hasNext() {
    return iterator.hasNext();
  }

  @Override
  public Identifiable next() {
    current = iterator.next();
    return current.record;
  }

  @Override
  public int getScore() {
    return current.score;
  }

  @Override
  public void close() {
  }

  @Override
  public String dumpStats() {
    return "no-stats";
  }

  @Override
  public BinaryComparator getComparator() {
    return null;
  }

  @Override
  public byte[] getKeyTypes() {
    return new byte[0];
  }

  @Override
  public long size() {
    return size;
  }

  @Override
  public Iterator<Identifiable> iterator() {
    return this;
  }
}
