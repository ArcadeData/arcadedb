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
package com.arcadedb.index.lsm;

import com.arcadedb.database.RID;
import com.arcadedb.engine.PageId;
import com.arcadedb.serializer.BinarySerializer;

public abstract class LSMTreeIndexUnderlyingAbstractCursor {
  protected final LSMTreeIndexAbstract index;
  /**
   * The index's STORAGE key types ({@link LSMTreeIndexAbstract#storageKeyTypes}), because a cursor's first job is to
   * decode page bytes. Subclasses then reuse this same array as the type argument of
   * {@link LSMTreeIndexMutable#compareKeys}, which is only correct while {@link com.arcadedb.serializer.BinaryComparator}
   * orders {@code TYPE_COMPRESSED_RID} and {@code TYPE_RID} identically - it does, they share one branch. Should that
   * ever stop being true, these cursors need the declared types threaded in alongside, the way
   * {@link LSMTreeIndexAbstract#compareKey} already keeps the two apart (#5703).
   */
  protected final byte[]               keyTypes;
  protected final BinarySerializer     serializer;
  protected final int                  totalKeys;
  protected final boolean              ascendingOrder;

  public LSMTreeIndexUnderlyingAbstractCursor(final LSMTreeIndexAbstract index, final byte[] keyTypes, final int totalKeys, final boolean ascendingOrder) {
    this.index = index;
    this.keyTypes = keyTypes;
    this.serializer = index.getDatabase().getSerializer();
    this.totalKeys = totalKeys;
    this.ascendingOrder = ascendingOrder;
  }

  public abstract boolean hasNext();

  public abstract void next();

  public abstract Object[] getKeys();

  public abstract RID[] getValue();

  public abstract PageId getCurrentPageId();

  public abstract int getCurrentPositionInPage();

  public void close() {
    // EMPTY METHOD
  }
}
