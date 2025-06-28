package com.arcadedb.remote;

import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

public class RemoteBulk {
  protected Supplier<Integer> idSupplier = new Supplier<>() {

    private final AtomicInteger id = new AtomicInteger();

    @Override
    public Integer get() {
      return id.getAndIncrement();
    }
  };

  @Test
  void bulkload() {
    RemoteServer server = new RemoteServer("127.0.0.1", 2480, "root", "pippopluto");
    if (server.exists("testBulk"))
      server.drop("testBulk");
    server.create("testBulk");

    final RemoteDatabase db = new RemoteDatabase("127.0.0.1", 2480, "testBulk", "root",
        "pippopluto");

    db.command("sqlscript",
        """
            CREATE VERTEX TYPE User;
            CREATE PROPERTY User.id INTEGER;
            CREATE INDEX ON User (id) UNIQUE;

            CREATE VERTEX TYPE Photo;
            CREATE PROPERTY Photo.id INTEGER;
            CREATE INDEX ON Photo (id) UNIQUE;

            CREATE EDGE TYPE HasUploaded;

            CREATE EDGE TYPE FriendOf;

            CREATE EDGE TYPE Likes;
            """);

    for (int userIndex = 1; userIndex <= 1000000; userIndex++) {
      Integer userId = idSupplier.get();
      try {
        db.transaction(() ->
            {
                db.acquireLock()
                    .type("User")
                    .lock();
              db.command("sql", "CREATE VERTEX User SET id = ?", userId);
            }
            , true, 10);

        if (userIndex % 1000 == 0)
          System.out.println("userId = " + userId);
      } catch (Exception e) {
        System.out.printf("Error creating user %s: %s", userId, e.getMessage());
        e.printStackTrace();
//        System.exit(1);
      }
    }
    db.close();
  }

}
