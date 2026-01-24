# SQL Triggers

ArcadeDB supports database triggers that automatically execute SQL statements, JavaScript code, or Java classes in response to record events. Triggers enable you to implement business logic, data validation, audit trails, and automated workflows directly in your database.

## Overview

A **trigger** is a named database object that automatically executes when specific events occur on records of a particular type. Triggers can execute SQL statements, JavaScript code, or Java classes, giving you flexibility in how you implement your logic.

### Key Features

- **Event-driven**: Triggers fire automatically on CREATE, READ, UPDATE, or DELETE operations
- **Timing control**: Execute BEFORE or AFTER the event
- **Multi-language**: Choose between SQL statements, JavaScript code, or Java classes
- **High performance**: Java class triggers offer maximum performance with compiled code
- **Persistent**: Triggers are stored in the schema and survive database restarts
- **Type-specific**: Each trigger applies to a specific document/vertex/edge type

## Syntax

### Creating Triggers

```sql
CREATE TRIGGER [IF NOT EXISTS] trigger_name
  (BEFORE|AFTER) (CREATE|READ|UPDATE|DELETE)
  ON [TYPE] type_name
  EXECUTE (SQL|JAVASCRIPT|JAVA) 'code_or_class_name'
```

**Parameters:**
- `trigger_name` - Unique name for the trigger
- `IF NOT EXISTS` - Optional: skip creation if trigger already exists
- `BEFORE|AFTER` - When to execute (before or after the event)
- `CREATE|READ|UPDATE|DELETE` - Which event to respond to
- `type_name` - The type (document, vertex, or edge type) to monitor
- `SQL|JAVASCRIPT|JAVA` - Language/type of the trigger action
- `code_or_class_name` - SQL statement, JavaScript code, or fully qualified Java class name

### Dropping Triggers

```sql
DROP TRIGGER [IF EXISTS] trigger_name
```

## Trigger Events

Triggers can respond to eight different combinations of timing and events:

| Event | Description |
|-------|-------------|
| `BEFORE CREATE` | Before a new record is created |
| `AFTER CREATE` | After a new record is created |
| `BEFORE READ` | Before a record is read from database |
| `AFTER READ` | After a record is read from database |
| `BEFORE UPDATE` | Before a record is modified |
| `AFTER UPDATE` | After a record is modified |
| `BEFORE DELETE` | Before a record is deleted |
| `AFTER DELETE` | After a record is deleted |

## SQL Triggers

SQL triggers execute SQL statements. They have access to the current record through context variables.

### Context Variables

- `$record` or `record` - The current record being operated on

### Example: Audit Trail

Create an audit log that tracks all user creations:

```sql
-- Create the audit log type
CREATE DOCUMENT TYPE AuditLog

-- Create trigger to log user creations
CREATE TRIGGER user_audit AFTER CREATE ON TYPE User
EXECUTE SQL 'INSERT INTO AuditLog SET action = "user_created",
             userName = $record.name,
             timestamp = sysdate()'
```

Now every time a User is created, an entry is automatically added to the AuditLog:

```sql
-- Create a user
INSERT INTO User SET name = 'Alice', email = 'alice@example.com'

-- Check the audit log
SELECT * FROM AuditLog
-- Returns: {action: "user_created", userName: "Alice", timestamp: ...}
```

### Example: Auto-increment Counter

Automatically set a sequence number on new documents:

```sql
-- Create a counter type
CREATE DOCUMENT TYPE Counter
INSERT INTO Counter SET name = 'order_sequence', value = 1000

-- Create trigger to auto-increment order numbers
CREATE TRIGGER order_number BEFORE CREATE ON TYPE Order
EXECUTE SQL 'UPDATE Counter SET value = value + 1
             WHERE name = "order_sequence";
             UPDATE $record SET orderNumber =
             (SELECT value FROM Counter WHERE name = "order_sequence")'
```

### Example: Cascade Updates

Update related records when the parent changes:

```sql
-- Update all orders when customer email changes
CREATE TRIGGER customer_email_update AFTER UPDATE ON TYPE Customer
EXECUTE SQL 'UPDATE Order SET customerEmail = $record.email
             WHERE customerId = $record.@rid'
```

### Example: Data Validation

Enforce business rules using BEFORE triggers:

```sql
-- Ensure product prices are positive
CREATE TRIGGER validate_price BEFORE CREATE ON TYPE Product
EXECUTE SQL 'SELECT FROM Product WHERE @this = $record AND price > 0'
-- If SELECT returns no results, the trigger fails and creation is aborted
```

## JavaScript Triggers

JavaScript triggers offer more flexibility and can implement complex logic with conditional statements, loops, and calculations.

### Context Variables

- `record` or `$record` - The current record being operated on
- `database` - The database instance

### Return Value

JavaScript triggers can return `false` to abort the operation (for BEFORE triggers only).

### Example: Data Validation

Validate email format before creating users:

```sql
CREATE TRIGGER validate_email BEFORE CREATE ON TYPE User
EXECUTE JAVASCRIPT 'if (!record.email || !record.email.includes("@")) {
  throw new Error("Invalid email address");
}'
```

### Example: Auto-populate Fields

Automatically set timestamps and computed fields:

```sql
CREATE TRIGGER user_defaults BEFORE CREATE ON TYPE User
EXECUTE JAVASCRIPT '
  // Set creation timestamp
  record.createdAt = new Date();

  // Generate username from email if not provided
  if (!record.username && record.email) {
    record.username = record.email.split("@")[0];
  }

  // Set default role
  if (!record.role) {
    record.role = "user";
  }
'
```

### Example: Complex Business Logic

Implement discount rules based on order total:

```sql
CREATE TRIGGER calculate_discount BEFORE CREATE ON TYPE Order
EXECUTE JAVASCRIPT '
  var total = record.total || 0;
  var discount = 0;

  // Apply discount based on order total
  if (total > 1000) {
    discount = 0.15;  // 15% discount
  } else if (total > 500) {
    discount = 0.10;  // 10% discount
  } else if (total > 100) {
    discount = 0.05;  // 5% discount
  }

  record.discountPercent = discount * 100;
  record.discountAmount = total * discount;
  record.finalTotal = total - (total * discount);
'
```

### Example: Conditional Abort

Prevent operations based on business rules:

```sql
CREATE TRIGGER prevent_weekend_orders BEFORE CREATE ON TYPE Order
EXECUTE JAVASCRIPT '
  var day = new Date().getDay();
  if (day === 0 || day === 6) {
    throw new Error("Orders cannot be placed on weekends");
  }
'
```

### Example: Audit with Details

Create detailed audit logs with JavaScript:

```sql
CREATE TRIGGER audit_update AFTER UPDATE ON TYPE Product
EXECUTE JAVASCRIPT '
  database.command("sql",
    "INSERT INTO AuditLog SET " +
    "action = \"product_updated\", " +
    "productId = \"" + record["@rid"] + "\", " +
    "productName = \"" + record.name + "\", " +
    "timestamp = sysdate()"
  );
'
```

## Java Triggers

Java triggers offer maximum performance by executing compiled Java code. They require implementing the `JavaTrigger` interface and must be available in the classpath.

### Creating a Java Trigger Class

First, create a Java class that implements the `com.arcadedb.schema.trigger.JavaTrigger` interface:

```java
package com.example.triggers;

import com.arcadedb.database.Database;
import com.arcadedb.database.Document;
import com.arcadedb.database.Record;
import com.arcadedb.schema.trigger.JavaTrigger;

public class EmailValidationTrigger implements JavaTrigger {

  @Override
  public boolean execute(Database database, Record record, Record oldRecord) throws Exception {
    if (record instanceof Document) {
      Document doc = (Document) record;
      String email = doc.getString("email");

      if (email == null || !email.contains("@")) {
        throw new IllegalArgumentException("Invalid email address");
      }
    }
    return true;  // Continue with the operation
  }
}
```

### JavaTrigger Interface

```java
public interface JavaTrigger {
  /**
   * Execute the trigger logic.
   *
   * @param database  The database instance
   * @param record    The current record being operated on
   * @param oldRecord The original record (for UPDATE operations), null otherwise
   * @return true to continue the operation, false to abort (BEFORE triggers only)
   * @throws Exception to abort the operation with an error message
   */
  boolean execute(Database database, Record record, Record oldRecord) throws Exception;
}
```

### Registering Java Triggers

Once your class is compiled and in the classpath, register it using SQL:

```sql
CREATE TRIGGER validate_email BEFORE CREATE ON TYPE User
EXECUTE JAVA 'com.example.triggers.EmailValidationTrigger'
```

### Example: Simple Flag Setter

Set a flag to indicate processing:

```java
package com.example.triggers;

import com.arcadedb.database.Database;
import com.arcadedb.database.Document;
import com.arcadedb.database.Record;
import com.arcadedb.schema.trigger.JavaTrigger;

public class ProcessedFlagTrigger implements JavaTrigger {

  @Override
  public boolean execute(Database database, Record record, Record oldRecord) {
    if (record instanceof Document) {
      ((Document) record).modify().set("processed", true);
    }
    return true;
  }
}
```

Register it:

```sql
CREATE TRIGGER mark_processed BEFORE CREATE ON TYPE Order
EXECUTE JAVA 'com.example.triggers.ProcessedFlagTrigger'
```

### Example: Data Validation

Validate complex business rules with full Java capabilities:

```java
package com.example.triggers;

import com.arcadedb.database.Database;
import com.arcadedb.database.Document;
import com.arcadedb.database.Record;
import com.arcadedb.schema.trigger.JavaTrigger;
import java.math.BigDecimal;

public class PriceValidationTrigger implements JavaTrigger {

  private static final BigDecimal MIN_PRICE = new BigDecimal("0.01");
  private static final BigDecimal MAX_PRICE = new BigDecimal("999999.99");

  @Override
  public boolean execute(Database database, Record record, Record oldRecord) throws Exception {
    if (record instanceof Document) {
      Document doc = (Document) record;
      BigDecimal price = doc.get("price");

      if (price == null) {
        throw new IllegalArgumentException("Price is required");
      }

      if (price.compareTo(MIN_PRICE) < 0) {
        throw new IllegalArgumentException("Price must be at least " + MIN_PRICE);
      }

      if (price.compareTo(MAX_PRICE) > 0) {
        throw new IllegalArgumentException("Price cannot exceed " + MAX_PRICE);
      }
    }
    return true;
  }
}
```

### Example: Abort Operation

Return `false` to silently abort the operation:

```java
package com.example.triggers;

import com.arcadedb.database.Database;
import com.arcadedb.database.Document;
import com.arcadedb.database.Record;
import com.arcadedb.schema.trigger.JavaTrigger;

public class WorkingHoursOnlyTrigger implements JavaTrigger {

  @Override
  public boolean execute(Database database, Record record, Record oldRecord) {
    java.time.LocalTime now = java.time.LocalTime.now();
    java.time.LocalTime start = java.time.LocalTime.of(9, 0);
    java.time.LocalTime end = java.time.LocalTime.of(17, 0);

    // Abort if outside working hours
    return !now.isBefore(start) && !now.isAfter(end);
  }
}
```

### Example: Database Queries

Execute queries within the trigger:

```java
package com.example.triggers;

import com.arcadedb.database.Database;
import com.arcadedb.database.Document;
import com.arcadedb.database.Record;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.trigger.JavaTrigger;

public class StockCheckTrigger implements JavaTrigger {

  @Override
  public boolean execute(Database database, Record record, Record oldRecord) throws Exception {
    if (record instanceof Document) {
      Document doc = (Document) record;
      String productId = doc.getString("productId");
      Integer quantity = doc.getInteger("quantity");

      ResultSet result = database.query("sql",
          "SELECT stock FROM Product WHERE @rid = ?", productId);

      if (result.hasNext()) {
        Document product = result.next().toElement().asDocument();
        Integer stock = product.getInteger("stock");

        if (stock < quantity) {
          throw new IllegalStateException(
              "Insufficient stock. Available: " + stock + ", Requested: " + quantity);
        }
      }
    }
    return true;
  }
}
```

### Example: Modify Related Records

Update related records in the same transaction:

```java
package com.example.triggers;

import com.arcadedb.database.Database;
import com.arcadedb.database.Document;
import com.arcadedb.database.Record;
import com.arcadedb.schema.trigger.JavaTrigger;

public class UpdateInventoryTrigger implements JavaTrigger {

  @Override
  public boolean execute(Database database, Record record, Record oldRecord) {
    if (record instanceof Document) {
      Document orderItem = (Document) record;
      String productId = orderItem.getString("productId");
      Integer quantity = orderItem.getInteger("quantity");

      // Decrement stock
      database.command("sql",
          "UPDATE Product SET stock = stock - ? WHERE @rid = ?",
          quantity, productId);
    }
    return true;
  }
}
```

### Java Trigger Advantages

1. **Performance**: Compiled code executes faster than interpreted JavaScript
2. **Type Safety**: Compile-time type checking prevents runtime errors
3. **Full Java Ecosystem**: Access to all Java libraries and frameworks
4. **IDE Support**: Code completion, refactoring, and debugging
5. **Testability**: Unit test your triggers like any Java class
6. **Reusability**: Share trigger code across projects

### Java Trigger Considerations

1. **Classpath**: Trigger classes must be in the ArcadeDB classpath at runtime
2. **Deployment**: Requires redeployment when trigger logic changes
3. **Error Handling**: Exceptions abort the operation and rollback the transaction
4. **Thread Safety**: Trigger instances may be reused across threads; ensure thread-safety
5. **No State**: Avoid instance variables; triggers should be stateless

## Use Cases

### 1. Audit Trails

Track who changed what and when:

```sql
CREATE TRIGGER audit_all AFTER UPDATE ON TYPE ImportantData
EXECUTE SQL 'INSERT INTO AuditLog SET
             tableName = "ImportantData",
             recordId = $record.@rid,
             modifiedAt = sysdate()'
```

### 2. Data Integrity

Ensure referential integrity and business rules:

```sql
CREATE TRIGGER check_inventory BEFORE CREATE ON TYPE OrderItem
EXECUTE JAVASCRIPT '
  var result = database.query("sql",
    "SELECT stock FROM Product WHERE @rid = ?",
    record.productId
  );

  if (result.hasNext()) {
    var product = result.next();
    if (product.stock < record.quantity) {
      throw new Error("Insufficient stock");
    }
  }
'
```

### 3. Denormalization

Maintain computed or cached values:

```sql
CREATE TRIGGER update_order_total AFTER CREATE ON TYPE OrderItem
EXECUTE SQL 'UPDATE Order SET
             totalAmount = (SELECT sum(price * quantity) FROM OrderItem
                           WHERE orderId = $record.orderId)
             WHERE @rid = $record.orderId'
```

### 4. Notifications

Send alerts or trigger external processes:

```sql
CREATE TRIGGER low_stock_alert AFTER UPDATE ON TYPE Product
EXECUTE JAVASCRIPT '
  if (record.stock < 10) {
    database.command("sql",
      "INSERT INTO Notification SET " +
      "type = \"low_stock\", " +
      "productId = \"" + record["@rid"] + "\", " +
      "message = \"Product stock low: " + record.stock + "\""
    );
  }
'
```

### 5. Workflow Automation

Automatically progress through workflow states:

```sql
CREATE TRIGGER order_workflow AFTER UPDATE ON TYPE Order
EXECUTE JAVASCRIPT '
  // Auto-approve small orders
  if (record.status === "pending" && record.total < 100) {
    record.status = "approved";
    record.approvedAt = new Date();
  }
'
```

## Best Practices

### 1. Keep Triggers Simple

Triggers execute synchronously and can impact performance. Keep logic simple and fast:

```sql
-- Good: Simple, fast operation
CREATE TRIGGER set_timestamp BEFORE CREATE ON TYPE Document
EXECUTE JAVASCRIPT 'record.createdAt = new Date();'

-- Avoid: Complex calculations that could be done elsewhere
```

### 2. Use Meaningful Names

Name triggers clearly to indicate their purpose:

```sql
-- Good naming
CREATE TRIGGER audit_user_creation AFTER CREATE ON TYPE User ...
CREATE TRIGGER validate_email BEFORE CREATE ON TYPE User ...

-- Poor naming
CREATE TRIGGER trigger1 AFTER CREATE ON TYPE User ...
```

### 3. Handle Errors Gracefully

In JavaScript triggers, throw descriptive errors:

```sql
CREATE TRIGGER validate_age BEFORE CREATE ON TYPE User
EXECUTE JAVASCRIPT '
  if (!record.age || record.age < 18) {
    throw new Error("User must be at least 18 years old");
  }
'
```

### 4. Avoid Infinite Loops

Be careful not to create circular trigger dependencies:

```sql
-- DANGER: This could create an infinite loop if not careful
-- Trigger A updates Type B, which triggers B updates Type A, etc.
```

### 5. Test Thoroughly

Test triggers with various scenarios:

- Normal operations
- Edge cases (null values, empty strings, etc.)
- Error conditions
- Performance with large datasets

## Managing Triggers

### List All Triggers

```java
// Java API
Trigger[] triggers = database.getSchema().getTriggers();
for (Trigger trigger : triggers) {
  System.out.println(trigger.getName() + " on " + trigger.getTypeName());
}
```

### Check if Trigger Exists

```java
boolean exists = database.getSchema().existsTrigger("trigger_name");
```

### Get Triggers for a Type

```java
Trigger[] triggers = database.getSchema().getTriggersForType("User");
```

### Remove a Trigger

```sql
DROP TRIGGER user_audit
```

## Limitations

1. **Synchronous Execution**: Triggers execute synchronously within the transaction. Long-running triggers can impact performance.

2. **Type Matching**: Triggers match the exact type name. Polymorphic matching (inheriting triggers from parent types) is not currently supported.

3. **Order of Execution**: When multiple triggers exist for the same event on the same type, they execute in alphabetical order by trigger name.

4. **BEFORE READ Limitation**: BEFORE READ triggers receive only the RID and must load the record, which can cause a double-read.

5. **Transaction Context**: Triggers execute within the same transaction as the triggering operation. If a trigger fails, the entire transaction rolls back.

6. **JavaScript Sandboxing**: JavaScript triggers run in a sandboxed environment with limited access to Java packages for security.

## Performance Considerations

### Benchmark Results

Performance tests measuring trigger execution overhead on document creation with identical operations (100,000 iterations, Java 21, macOS). All triggers perform the same operation: `INSERT INTO [Type]Audit SET triggered = true`.

| Trigger Type | Avg Time (µs) | Overhead (µs) | Overhead (%) | Relative Performance |
|--------------|---------------|---------------|--------------|---------------------|
| No Trigger (Baseline) | 95 | — | — | — |
| **Java Trigger** | **147** | **+52** | **+54.7%** | **Fastest trigger** |
| SQL Trigger | 150 | +55 | +57.9% | 2% slower than Java |
| JavaScript Trigger | 187 | +92 | +96.8% | 27% slower than Java |

**Key Findings:**

1. **Java and SQL triggers have nearly identical performance**: Only 2% difference (147 vs 150 µs), both execute compiled code paths efficiently.

2. **JavaScript triggers are ~27% slower**: GraalVM JavaScript execution adds noticeable overhead compared to native execution.

3. **All triggers add overhead**: Expect ~50-95% overhead depending on trigger type, which is acceptable for most use cases.

4. **Trigger overhead is predictable**: The performance impact is consistent and can be factored into capacity planning.

### Performance Recommendations

- **Minimize Work**: Keep trigger code as lightweight as possible
- **Choose the Right Type**:
  - Use **Java triggers** when you need type safety, IDE support, and debugging capabilities
  - Use **SQL triggers** for database operations - performance is nearly identical to Java
  - Use **JavaScript triggers** for dynamic logic where ~30% slower performance is acceptable
- **Avoid Complex Queries**: Heavy queries in triggers can slow down operations
- **Consider Batch Operations**: Triggers fire for each record, which can be expensive in bulk operations
- **Monitor Impact**: Test performance with realistic data volumes
- **Profile Your Workload**: Measure actual impact in your specific use case

### When to Use Each Type

**Java Triggers** - Best for:
- Complex validation requiring type safety and compile-time checks
- Integration with existing Java libraries
- Code that benefits from IDE support and refactoring
- Unit testing requirements
- Team prefers strongly-typed languages

**SQL Triggers** - Best for:
- Simple database operations (audit logs, denormalization)
- Prototyping and development (no compilation step)
- Deployment simplicity (embedded in schema)
- Operations that are primarily SQL-based
- **Performance-critical paths** (nearly identical performance to Java)

**JavaScript Triggers** - Best for:
- Moderate complexity business logic
- Rapid development and iteration
- Dynamic validation rules that change frequently
- Scenarios where scripting flexibility outweighs performance
- Teams comfortable with JavaScript

## Troubleshooting

### Trigger Not Firing

1. Verify the trigger exists: `SELECT FROM schema:triggers`
2. Check the type name matches exactly
3. Ensure the event type is correct (CREATE/READ/UPDATE/DELETE)
4. Verify trigger timing (BEFORE/AFTER)

### JavaScript Errors

Check logs for detailed error messages. Common issues:
- Syntax errors in JavaScript code
- Accessing undefined properties
- Type mismatches
- Security restrictions

### Java Trigger Errors

Common issues with Java triggers:
- ClassNotFoundException: Trigger class not in classpath
- NoSuchMethodException: Missing no-arg constructor
- ClassCastException: Class doesn't implement JavaTrigger interface
- NullPointerException: Check for null values in record properties
- Thread safety issues: Ensure triggers are stateless

### Performance Issues

- Profile which triggers are executing
- Consider disabling triggers temporarily for bulk operations
- Optimize trigger logic
- Move complex processing to application code

## Examples Summary

For more examples and real-world scenarios, see the test suite in `engine/src/test/java/com/arcadedb/query/sql/TriggerSQLTest.java`.

## Next Steps

- Learn about [Event Listeners](https://docs.arcadedb.com) for programmatic trigger alternatives
- Explore [SQL Commands](https://docs.arcadedb.com) for more database operations
- Review [JavaScript in ArcadeDB](https://docs.arcadedb.com) for advanced scripting
