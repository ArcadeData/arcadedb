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
package com.arcadedb.remote.grpc;

import java.util.concurrent.atomic.AtomicLong;

final class TxDebug {
  final    long                                   id          = System.nanoTime(); // local correlation id
  final    Thread                                 ownerThread = Thread.currentThread();
  final    String                                 dbName;
  volatile String                                 txLabel; // optional
  final    Exception                              beginSite   = new Exception("begin site"); // capture stack
  final    AtomicLong rpcSeq      = new AtomicLong();
  volatile boolean                                beginRpcSent, committed, rolledBack;

  @SuppressWarnings("unused")
  TxDebug(String db, String label) {
    this.dbName = db;
    this.txLabel = label;
  }
}
