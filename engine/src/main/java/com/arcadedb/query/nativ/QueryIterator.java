package com.arcadedb.query.nativ;/*
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

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Document;
import com.arcadedb.database.Record;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.Vertex;
import com.arcadedb.schema.DocumentType;

import java.util.*;

/**
 * Query iterator returned from queries. Extends the base Java iterator with convenient methods.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public abstract class QueryIterator<T extends Document> implements Iterator<T> {
  public T nextOrNull() {
    return hasNext() ? next() : null;
  }
}
