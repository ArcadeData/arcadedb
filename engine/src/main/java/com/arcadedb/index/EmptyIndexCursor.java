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
import com.arcadedb.database.RID;
import com.arcadedb.serializer.BinaryComparator;

import java.util.Iterator;
import java.util.NoSuchElementException;

public class EmptyIndexCursor implements IndexCursor {
  @Override
  public Object[] getKeys() {
    return null;
  }

  @Override
  public RID getRecord() {
    return null;
  }

  @Override
  public boolean hasNext() {
    return false;
  }

  @Override
  public Identifiable next() {
    throw new NoSuchElementException();
  }

  @Override
  public int getScore() {
    return 0;
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
    return 0l;
  }

  @Override
  public Iterator<Identifiable> iterator() {
    return this;
  }
}
