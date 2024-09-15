package com.arcadedb.database;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.engine.Bucket;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;

public class BucketSQLTest extends TestHelper {
  @Test
  public void testPopulate() {
    // CALL POPULATE
  }

  @Override
  protected void beginTest() {
    database.getConfiguration().setValue(GlobalConfiguration.TX_WAL_FLUSH, 2);

    database.transaction(() -> {
      database.command("sql", "CREATE BUCKET Customer_Europe if not exists");
      database.command("sql", "CREATE BUCKET Customer_Europe if not exists");

      database.command("sql", "CREATE BUCKET Customer_Americas");
      database.command("sql", "CREATE BUCKET Customer_Asia");
      database.command("sql", "CREATE BUCKET Customer_Other");

      database.command("sql", "CREATE DOCUMENT TYPE Customer BUCKET Customer_Europe,Customer_Americas,Customer_Asia,Customer_Other");

      final DocumentType customer = database.getSchema().getType("Customer");
      final List<Bucket> buckets = customer.getBuckets(true);
      assertThat(buckets.size()).isEqualTo(4);

      ResultSet resultset = database.command("sql", "INSERT INTO BUCKET:Customer_Europe CONTENT { firstName: 'Enzo', lastName: 'Ferrari' }");
      assertThat(resultset.hasNext()).isTrue();
      final Document enzo = resultset.next().getRecord().get().asDocument();
      assertThat(resultset.hasNext()).isFalse();
      assertThat(enzo.getIdentity().bucketId).isEqualTo(database.getSchema().getBucketByName("Customer_Europe").getFileId());

      resultset = database.command("sql", "INSERT INTO BUCKET:Customer_Americas CONTENT { firstName: 'Jack', lastName: 'Tramiel' }");
      assertThat(resultset.hasNext()).isTrue();
      final Document jack = resultset.next().getRecord().get().asDocument();
      assertThat(resultset.hasNext()).isFalse();
      assertThat(jack.getIdentity().bucketId).isEqualTo(database.getSchema().getBucketByName("Customer_Americas").getFileId());

      resultset = database.command("sql", "INSERT INTO BUCKET:Customer_Asia CONTENT { firstName: 'Bruce', lastName: 'Lee' }");
      assertThat(resultset.hasNext()).isTrue();
      final Document bruce = resultset.next().getRecord().get().asDocument();
      assertThat(resultset.hasNext()).isFalse();
      assertThat(bruce.getIdentity().bucketId).isEqualTo(database.getSchema().getBucketByName("Customer_Asia").getFileId());

      resultset = database.command("sql", "INSERT INTO BUCKET:Customer_Other CONTENT { firstName: 'Penguin', lastName: 'Hungry' }");
      assertThat(resultset.hasNext()).isTrue();
      final Document penguin = resultset.next().getRecord().get().asDocument();
      assertThat(resultset.hasNext()).isFalse();
      assertThat(penguin.getIdentity().bucketId).isEqualTo(database.getSchema().getBucketByName("Customer_Other").getFileId());
    });
  }
}
