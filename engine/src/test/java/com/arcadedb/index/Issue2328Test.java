package com.arcadedb.index;

import com.arcadedb.database.BasicDatabase;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class Issue2328Test {

  @Test
  void notUniqueShouldReturnDistinctValues() throws IOException {
    final File tmp = File.createTempFile("ArcadeTmp", "");
    tmp.delete();
    System.out.println("Tmp Database: " + tmp);

    try(final DatabaseFactory fact = new DatabaseFactory(tmp.toString())){
      try(final LocalDatabase local = (LocalDatabase) fact.create()){
        local.begin();

        final Schema schema = local.getSchema();

        // Create type
        final DocumentType tc = schema.createDocumentType("example");

        // Create props
        tc.createProperty("p1", String.class);
        tc.createProperty("p2", Long.class);

        // Index props
        tc.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "p2");

        // Add dummy data
        final String[] perm = { "a", "b", "c", "d", "e", "f", "g", "h" };
        long idx = 0;
        for(final String l1 : perm) {
          for(final String l2 : perm) {
            for(final String l3 : perm) {
              final MutableDocument doc = local.newDocument("example");
              doc.set("p1", l1 + l2 + l3);
              doc.set("p2", idx);
              doc.save();
            }
          }
          idx++;
        }


        local.commit();


        local.begin();

        // Get data from high water mark
        final ResultSet rs = local.command("sql", "select * from example where p2>?", 5);

        // Output data check for dups
        int count = 0;
        int dup = 0;
        final Set<String> found = new HashSet<>();

        System.out.println();
        while(rs.hasNext()) {
          final Result res = rs.next();
          final String p1 = res.getProperty("p1");
          final boolean add = found.add(p1);
          if(!add) {
            dup++;
          }
          System.out.println(p1 + " - " + (Long)res.getProperty("p2") + " - dup: " + !add);
          count++;
        }

        System.out.println("Found: " + count);
        System.out.println("Duplicates: " + dup);
        if(dup>0) {
          throw new IllegalStateException("Duplicates found: " + dup);
        }
      }
    }

  }
}
