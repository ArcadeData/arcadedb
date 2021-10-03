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
package org.apache.tinkerpop.gremlin.arcadedb.structure;

import org.opencypher.gremlin.translation.TranslationFacade;

import java.util.*;

/**
 * Cypher Expression builder. Transform a cypher expression into Gremlin.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */

public class ArcadeCypher extends ArcadeGremlin {
  private static final HashMap<String, DatabaseCache> STATEMENT_CACHE = new HashMap<>();

  private static class DatabaseCache {
    public final HashMap<String, CachedStatement> CACHE = new HashMap<>();
  }

  // TODO: ADD SQL IF THE GREMLIN QUERY IS CONVERTIBLE TO SQL
  private static class CachedStatement {
    public final String cypher;
    public final String gremlin;
    public       int    used = 0;

    private CachedStatement(final String cypher, final String gremlin) {
      this.cypher = cypher;
      this.gremlin = gremlin;
    }
  }

  protected ArcadeCypher(final ArcadeGraph graph, final String cypherQuery) {
    super(graph, compileToGremlin(graph, cypherQuery));
  }

  public static String compileToGremlin(final ArcadeGraph graph, final String cypher) {
    String gremlin = null;

    synchronized (STATEMENT_CACHE) {
      final String db = graph.getDatabase().getDatabasePath();
      DatabaseCache databaseCache = STATEMENT_CACHE.get(db);
      if (databaseCache == null) {
        databaseCache = new DatabaseCache();
        STATEMENT_CACHE.put(db, databaseCache);
      }

      final CachedStatement cached = databaseCache.CACHE.get(cypher);
      if (cached != null) {
        // FOUND: USE THE CACHED GREMLIN STATEMENT
        ++cached.used;
        gremlin = cached.gremlin;
      }

      if (gremlin == null) {
        // TRANSLATE TO GREMLIN AND CACHE THE STATEMENT FOR FURTHER USAGE
        gremlin = new TranslationFacade().toGremlinGroovy(cypher);
        databaseCache.CACHE.put(cypher, new CachedStatement(cypher, gremlin));
      }
    }

    return gremlin;
  }

  public static void closeDatabase(final ArcadeGraph graph) {
    synchronized (STATEMENT_CACHE) {
      STATEMENT_CACHE.remove(graph.getDatabase().getDatabasePath());
    }
  }
}
