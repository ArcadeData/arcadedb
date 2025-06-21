package com.arcadedb.database;

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
 */

/**
 * Interface for explicit locks on a transaction to lock buckets and types in a pessimistic way. This avoids the retry mechanism of default implicit locking.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public interface TransactionExplicitLock {
  TransactionExplicitLock bucket(String bucketName);

  TransactionExplicitLock type(String typeName);

  void lock();
}
