/*
 * Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.arcadedb.lucene;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// This class might serve as the main plugin class listed in plugin.json for initialization purposes,
// or handle lifecycle events if ArcadeDB's plugin API expects a specific class for that.
// For now, it's minimal.
public class ArcadeLuceneLifecycleManager {
  private static final Logger logger = LoggerFactory.getLogger(ArcadeLuceneLifecycleManager.class);

  // This constant might be better placed in ArcadeLuceneIndexFactoryHandler or a shared constants class.
  public static final String LUCENE_ALGORITHM = "LUCENE";

  public ArcadeLuceneLifecycleManager() {
    this(false);
  }

  public ArcadeLuceneLifecycleManager(boolean manual) {
     if (!manual) {
         logger.info("ArcadeLuceneLifecycleManager initialized (manual: {}).", manual);
         // Further initialization or listener registration logic specific to ArcadeDB's plugin system
         // would go here if this class is the entry point.
     }
  }

  // Any necessary lifecycle methods (e.g., from a specific ArcadeDB plugin interface) would be here.
  // For now, assuming it does not need to implement DatabaseListener directly.
  // Drop logic for indexes of this type should be handled by the Index.drop() method.
}
