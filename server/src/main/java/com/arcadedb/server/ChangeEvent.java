package com.arcadedb.server;

import com.arcadedb.database.Record;
import org.json.JSONObject;

public class ChangeEvent {
  private final TYPE   type;
  private final Record record;
  private final String database;

  public enum TYPE {CREATE, UPDATE, DELETE}

  public ChangeEvent(TYPE type, Record record, String database) {
    this.type = type;
    this.record = record;
    this.database = database;
  }

  public String getDatabase() {
    return database;
  }

  public Record getRecord() {
    return record;
  }

  public TYPE getType() {
    return type;
  }

  public String toJSON() {
    var jsonObject = new JSONObject();
    jsonObject.put("changeType", this.type.toString().toLowerCase());
    jsonObject.put("record", this.record.toJSON().toString());
    jsonObject.put("database", this.database);
    return jsonObject.toString();
  }
}
