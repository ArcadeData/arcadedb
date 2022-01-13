package com.arcadedb.mongo.query;

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.exception.QueryParsingException;
import com.arcadedb.log.LogManager;
import com.arcadedb.mongo.MongoDBDatabaseWrapper;
import com.arcadedb.query.QueryEngine;

import java.util.logging.*;

public class MongoQueryEngineFactory implements QueryEngine.QueryEngineFactory {
  @Override
  public String getLanguage() {
    return "mongo";
  }

  @Override
  public QueryEngine getInstance(final DatabaseInternal database) {
    Object engine = database.getWrappers().get(MongoQueryEngine.ENGINE_NAME);
    if (engine != null)
      return (MongoQueryEngine) engine;

    try {

      engine = new MongoQueryEngine(MongoDBDatabaseWrapper.open(database));
      database.setWrapper(MongoQueryEngine.ENGINE_NAME, engine);
      return (MongoQueryEngine) engine;

    } catch (Exception e) {
      LogManager.instance().log(this, Level.SEVERE, "Error on initializing Mongo query engine", e);
      throw new QueryParsingException("Error on initializing Mongo query engine", e);
    }
  }
}
