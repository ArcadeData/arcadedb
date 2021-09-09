package com.orientechnologies.orient.server.distributed.impl;

import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.OrientDBDistributed;
import com.orientechnologies.orient.core.db.viewmanager.ViewManager;
import com.orientechnologies.orient.core.metadata.schema.OView;
import java.util.List;

public class ViewManagerDistributed extends ViewManager {
  public ViewManagerDistributed(OrientDBDistributed orientDB, String name) {
    super(orientDB, name);
  }

  @Override
  protected boolean buildOnThisNode(ODatabase db, String viewName) {
    OView view = db.getMetadata().getSchema().getView(viewName);

    List<String> nodesForView = view.getNodes();
    if (nodesForView == null || nodesForView.contains("*")) {
      return true;
    }
    String localNode = ((ODatabaseDocumentDistributed) db).getLocalNodeName();
    return nodesForView.contains(localNode);
  }
}
