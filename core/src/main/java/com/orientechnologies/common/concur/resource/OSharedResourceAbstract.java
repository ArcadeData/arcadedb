/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://orientdb.com
 *
 */
package com.orientechnologies.common.concur.resource;

import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Shared resource abstract class. Sub classes can acquire and release shared and exclusive locks.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public abstract class OSharedResourceAbstract {
  protected final ReadWriteLock lock = new ReentrantReadWriteLock();

  protected void acquireSharedLock() {
    lock.readLock().lock();
  }

  protected void releaseSharedLock() {
    lock.readLock().unlock();
  }

  protected void acquireExclusiveLock() {
    lock.writeLock().lock();
  }

  protected void releaseExclusiveLock() {
    lock.writeLock().unlock();
  }
}
