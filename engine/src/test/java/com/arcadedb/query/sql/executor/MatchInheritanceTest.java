package com.arcadedb.query.sql.executor;

import com.arcadedb.TestHelper;
import com.arcadedb.graph.Vertex;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class MatchInheritanceTest extends TestHelper {
  @Test
  public void testInheritance() {
    ResultSet result = null;

    String sql = "SELECT FROM Services";

    result = database.command("SQL", sql);
    int selectFromServices = 0;
    while (result.hasNext()) {
      Result record = result.next();
      ++selectFromServices;
    }
    assertThat(selectFromServices).isEqualTo(4);

    sql = "SELECT FROM Attractions";
    result = database.command("SQL", sql);
    int selectFromAttractions = 0;
    while (result.hasNext()) {
      Result record = result.next();
      ++selectFromAttractions;
    }
    assertThat(selectFromAttractions).isEqualTo(4);

    sql = "SELECT FROM Locations";

    result = database.command("SQL", sql);
    int selectFromLocations = 0;
    while (result.hasNext()) {
      Result record = result.next();
      ++selectFromLocations;
    }
    assertThat(selectFromLocations).isEqualTo(8);

    sql = "MATCH {type: Customers, as: customer, where: (OrderedId=1)}--{type: Monuments} " + "RETURN $pathelements";
    result = database.query("SQL", sql);

    assertThat(result.stream().count()).isEqualTo(2);

    sql = "MATCH {type: Customers, as: customer, where: (OrderedId=1)}--{type: Services} " + "RETURN $pathelements";
    result = database.query("SQL", sql);

    assertThat(result.stream().count()).isEqualTo(8);

    sql = "MATCH {type: Customers, as: customer, where: (OrderedId=1)}--{type: Attractions} " + "RETURN $pathelements";
    result = database.query("SQL", sql);

    assertThat(result.stream().count()).isEqualTo(8);

    sql = "MATCH {type: Customers, as: customer, where: (OrderedId=1)}--{type: Locations} " + "RETURN $pathelements";
    result = database.query("SQL", sql);

    assertThat(result.stream().count()).isEqualTo(16);
  }

  @Override
  public void beginTest() {
    StringBuilder sb = new StringBuilder();

    sb.append("BEGIN;");
/*
			-- Locations
			--    + Services
			--        + Hotels
			--        + Restaurants
			--    + Attractions
			--        + Monuments
			--        + Castles
			--        + Theatres
			--        + Archaeological Sites
*/
    sb.append("CREATE VERTEX TYPE Locations;");
    sb.append("CREATE PROPERTY Locations.Id LONG;");
    sb.append("CREATE PROPERTY Locations.Type STRING;");
    sb.append("CREATE PROPERTY Locations.Name STRING;");

    sb.append("CREATE INDEX ON Locations (Type) NOTUNIQUE;");
    sb.append("CREATE INDEX ON Locations (Name) FULL_TEXT;");

    sb.append("CREATE VERTEX TYPE Services EXTENDS Locations;");
    sb.append("CREATE VERTEX TYPE Hotels EXTENDS Services;");
    sb.append("CREATE INDEX ON Hotels (Id) UNIQUE;");

    sb.append("CREATE VERTEX TYPE Restaurants EXTENDS Services;\n");
    sb.append("CREATE INDEX ON Restaurants(Id) UNIQUE;\n");

    sb.append("CREATE VERTEX TYPE Attractions EXTENDS Locations;\n");
    sb.append("CREATE VERTEX TYPE Monuments EXTENDS Attractions;\n");
    sb.append("CREATE INDEX ON Monuments (Id) UNIQUE;\n");

    sb.append("CREATE VERTEX TYPE Castles EXTENDS Attractions;\n");
    sb.append("CREATE INDEX ON Castles(Id) UNIQUE;\n");

    sb.append("CREATE VERTEX TYPE Theatres EXTENDS Attractions;\n");
    sb.append("CREATE INDEX ON Theatres(Id) UNIQUE;\n");

    sb.append("CREATE VERTEX TYPE ArchaeologicalSites EXTENDS Attractions;\n");
    sb.append("CREATE INDEX ON ArchaeologicalSites(Id) UNIQUE;\n");

    sb.append("CREATE VERTEX TYPE Customers;");
    sb.append("CREATE PROPERTY Customers.OrderedId LONG;");

    sb.append("CREATE VERTEX TYPE Orders;");
    sb.append("CREATE PROPERTY Orders.Id LONG;");
    sb.append("CREATE PROPERTY Orders.Amount LONG;");
    sb.append("CREATE PROPERTY Orders.OrderDate DATE;");

    sb.append("CREATE INDEX ON Customers(OrderedId) UNIQUE;");

    sb.append("CREATE INDEX ON Orders(Id) UNIQUE;");

    sb.append("CREATE EDGE TYPE HasUsedService;");
    sb.append("CREATE PROPERTY HasUsedService.out LINK OF Customers;");

    sb.append("CREATE EDGE TYPE HasStayed EXTENDS HasUsedService;");
    sb.append("CREATE PROPERTY HasStayed.in LINK OF Hotels;");

    sb.append("CREATE EDGE TYPE HasEaten EXTENDS HasUsedService;");
    sb.append("CREATE PROPERTY HasEaten.in LINK OF Restaurants;");

    sb.append("CREATE EDGE TYPE HasVisited;");
    sb.append("CREATE PROPERTY HasVisited.out LINK OF Customers;");
    sb.append("CREATE PROPERTY HasVisited.in LINK;");
    sb.append("CREATE INDEX ON HasVisited (`in`, `out`) UNIQUE;");

    sb.append("CREATE EDGE TYPE HasCustomer;");
    sb.append("CREATE PROPERTY HasCustomer.in LINK OF Customers;");
    sb.append("CREATE PROPERTY HasCustomer.out LINK OF Orders ;");

    sb.append("INSERT INTO Customers SET OrderedId = 1, Phone = '+1400844724';");
    sb.append("INSERT INTO Orders SET Id = 1, Amount = 536, OrderDate = '2013-05-23';");

    sb.append("INSERT INTO Hotels SET Id = 730, Name = 'Toules', Type = 'alpine_hut';");

    sb.append("INSERT INTO Restaurants SET Id = 1834, Name = 'Uliassi', Type = 'restaurant';");
    sb.append("INSERT INTO Restaurants SET Id = 1099, Name = 'L\\'Angelo d\\'Oro', Type = 'restaurant';");

    sb.append("INSERT INTO Restaurants SET Id = 1738, Name = 'Johnny Paranza', Type = 'fast_food';");

    sb.append("INSERT INTO Castles SET Id = 127, Name = 'Haselburg', Type = 'castle';");
    sb.append("INSERT INTO ArchaeologicalSites SET Id = 47, Name = 'Villa Romana', Type = 'archaeological_site';");
    sb.append("INSERT INTO Monuments  SET Id = 62, Name = 'Giuseppe Garibaldi', Type = 'monument';");
    sb.append("INSERT INTO Theatres SET Id = 65, Name = 'Teatro Civico', Type = 'theatre';");

    sb.append("CREATE EDGE HasStayed FROM (SELECT FROM Customers WHERE OrderedId=1) TO (SELECT FROM Hotels WHERE Id=730);");

    sb.append("CREATE EDGE HasEaten FROM (SELECT FROM Customers WHERE OrderedId=1) TO (SELECT FROM Restaurants WHERE Id=1834);");
    sb.append("CREATE EDGE HasEaten FROM (SELECT FROM Customers WHERE OrderedId=1) TO (SELECT FROM Restaurants WHERE Id=1099);");
    sb.append("CREATE EDGE HasEaten FROM (SELECT FROM Customers WHERE OrderedId=1) TO (SELECT FROM Restaurants WHERE Id=1738);");

    sb.append("CREATE EDGE HasCustomer FROM (SELECT FROM Orders WHERE Id=1) TO (SELECT FROM Customers WHERE OrderedId=1);");

    sb.append("CREATE EDGE HasVisited FROM (SELECT FROM Customers WHERE OrderedId=1) TO (SELECT FROM Castles WHERE Id=127);");
    sb.append(
        "CREATE EDGE HasVisited FROM (SELECT FROM Customers WHERE OrderedId=1) TO (SELECT FROM ArchaeologicalSites WHERE Id=47);");
    sb.append("CREATE EDGE HasVisited FROM (SELECT FROM Customers WHERE OrderedId=1) TO (SELECT FROM Monuments WHERE Id=62);");
    sb.append("CREATE EDGE HasVisited FROM (SELECT FROM Customers WHERE OrderedId=1) TO (SELECT FROM Theatres WHERE Id=65);");

    sb.append("COMMIT;");

    database.command("SQLScript", sb.toString());
  }
}
