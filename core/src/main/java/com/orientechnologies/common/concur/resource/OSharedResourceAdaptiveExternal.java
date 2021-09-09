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

/**
 * Optimize locks since they are enabled only when the engine runs in MULTI-THREADS mode.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class OSharedResourceAdaptiveExternal extends OSharedResourceAdaptive
    implements OSharedResource {
  public OSharedResourceAdaptiveExternal(
      final boolean iConcurrent, final int iTimeout, final boolean ignoreThreadInterruption) {
    super(iConcurrent, iTimeout, ignoreThreadInterruption);
  }

  @Override
  public void acquireExclusiveLock() {
    super.acquireExclusiveLock();
  }

  public boolean tryAcquireExclusiveLock() {
    return super.tryAcquireExclusiveLock();
  }

  @Override
  public void acquireSharedLock() {
    super.acquireSharedLock();
  }

  public boolean tryAcquireSharedLock() {
    return super.tryAcquireSharedLock();
  }

  @Override
  public void releaseExclusiveLock() {
    super.releaseExclusiveLock();
  }

  @Override
  public void releaseSharedLock() {
    super.releaseSharedLock();
  }
}
