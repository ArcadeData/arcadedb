package com.arcadedb.server.http.ws;

import com.arcadedb.database.Record;
import org.json.JSONObject;

public class ChangeEvent {
  private final TYPE   type;
  private final Record record;

  public enum TYPE {CREATE, UPDATE, DELETE}

  public ChangeEvent(final TYPE type, final Record record) {
    this.type = type;
    this.record = record;
  }

  public Record getRecord() {
    return record;
  }

  public TYPE getType() {
    return type;
  }

  public String toJSON() {
    final var jsonObject = new JSONObject();
    jsonObject.put("changeType", this.type.toString().toLowerCase());
    jsonObject.put("record", this.record.toJSON());
    jsonObject.put("database", this.record.getDatabase().getName());
    return jsonObject.toString();
  }
}
