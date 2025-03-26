package com.arcadedb.remote;

import com.arcadedb.graph.Edge;
import com.arcadedb.graph.MutableEdge;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class RemoteDatabaseJavaApiIT extends BaseGraphServerTest {
  private static final String DATABASE_NAME = "remote-database";

  @Override
  protected boolean isCreateDatabases() {
    return false;
  }

  @Test
  void createUnidirectionalEdge() {
    assertThat(
        new RemoteServer("127.0.0.1", 2480, "root", BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).exists(DATABASE_NAME)).isTrue();

    final RemoteDatabase database = new RemoteDatabase("127.0.0.1", 2480, DATABASE_NAME, "root",
        BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);

    database.command("sql", "create vertex type Person");
    database.command("sql", "create edge type FriendOf");

    MutableVertex me = database.newVertex("Person").set("name", "me") .save();
    MutableVertex you = database.newVertex("Person").set("name", "you") .save();

    MutableEdge friendOf = me.newEdge("FriendOf", you).save();

    assertThat(friendOf.getOut()).isEqualTo(me);
    assertThat(friendOf.getIn()).isEqualTo(you);

    assertThat(me.getVertices(Vertex.DIRECTION.OUT).iterator().hasNext()).isTrue();
    assertThat(me.getVertices(Vertex.DIRECTION.OUT).iterator().next()).isEqualTo(you);
    assertThat(me.getVertices(Vertex.DIRECTION.IN).iterator().hasNext()).isFalse();

    assertThat(you.getVertices(Vertex.DIRECTION.OUT).iterator().hasNext()).isFalse();
    assertThat(you.getVertices(Vertex.DIRECTION.IN).iterator().hasNext()).isFalse();

    Iterable<Edge> friends = me.getEdges(Vertex.DIRECTION.IN, "FriendOf");
    assertThat(friends).containsExactly(friendOf);

  }

  @BeforeEach
  public void beginTest() {
    super.beginTest();
    final RemoteServer server = new RemoteServer("127.0.0.1", 2480, "root", BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);
    if (!server.exists(DATABASE_NAME))
      server.create(DATABASE_NAME);
  }

  @AfterEach
  public void endTest() {
    final RemoteServer server = new RemoteServer("127.0.0.1", 2480, "root", BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);
    if (server.exists(DATABASE_NAME))
      server.drop(DATABASE_NAME);
    super.endTest();
  }

}
