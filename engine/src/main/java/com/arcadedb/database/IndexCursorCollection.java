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
package com.arcadedb.database;

import com.arcadedb.index.IndexCursor;
import com.arcadedb.serializer.BinaryComparator;

import java.util.*;

public class IndexCursorCollection implements IndexCursor {
  private final Collection<Identifiable> collection;
  private final Iterator<Identifiable>   iterator;
  private       Identifiable             last = null;

  public IndexCursorCollection(final Collection<Identifiable> collection) {
    this.collection = collection;
    this.iterator = collection.iterator();
  }

  @Override
  public Object[] getKeys() {
    return new Object[0];
  }

  @Override
  public Identifiable getRecord() {
    return last;
  }

  @Override
  public BinaryComparator getComparator() {
    return null;
  }

  @Override
  public byte[] getBinaryKeyTypes() {
    return new byte[0];
  }

  @Override
  public long estimateSize() {
    return collection.size();
  }

  @Override
  public Iterator<Identifiable> iterator() {
    return iterator;
  }

  @Override
  public boolean hasNext() {
    return iterator.hasNext();
  }

  @Override
  public Identifiable next() {
    last = iterator.next();
    return last;
  }
}
