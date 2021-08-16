/*
 * Copyright 2021 Arcade Data Ltd
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.arcadedb.index.lsm;

import com.arcadedb.database.RID;
import com.arcadedb.engine.PageId;
import com.arcadedb.serializer.BinarySerializer;

public abstract class LSMTreeIndexUnderlyingAbstractCursor {
  protected final LSMTreeIndexAbstract index;
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
  }
}
