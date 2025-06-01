/*
 * Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
 * Copyright 2023 Arcade Data Ltd
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
package com.arcadedb.lucene.functions;

import com.arcadedb.query.sql.SQLFunctionRegistry; // Assuming this is the ArcadeDB equivalent

// FIXME: The actual function class (ArcadeLuceneCrossClassSearchFunction) will need to be created/refactored separately.

public class ArcadeLuceneCrossClassFunctionsFactory { // Changed class name

  public static void onStartup() { // Changed to a static method for registration
    SQLFunctionRegistry.INSTANCE.register(new ArcadeLuceneCrossClassSearchFunction()); // FIXME: Placeholder for refactored class
  }
}
