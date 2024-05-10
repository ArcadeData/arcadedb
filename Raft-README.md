### Remote debug

To halt and be able to attach IDE as remote debug session add/update JAVA_OPTS ENV variable to contain below value. When configuring IDE make sure to use port 8000 after port forwarding on port 8000.

```yaml
- name: JAVA_OPTS
  value: -agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=0.0.0.0:8000 -Darcadedb.server.rootPassword=playwithdata
```

### Info on Raft's modifications to ArcadeDb for the the SOCOM DF usecase.
- Keycloak authentication
- Basic classification/ACCM handling
- Non classification marked data in databases with classification controls enabled are quarantined and only viewable by Data Stewards.
- Daily backup job


#### Access ArcadeDb on DF helm deployments at:
- Studio: https://arcadedb.[localhost|custom df domain]
- API: ~/api/v1/arcadedb

#### Configuration
| EnvVariable | Default                          | Description                                                                                                                                                                                          | 
|---|----------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------| 
| BOOTSTRAP_SERVERS_CONFIG | "kafka-bootstrap.localhost:9092" | Kafka cluster connection string                                                                                                                                                                      | 
| CLIENT_ID_CONFIG | "admin-arcadedb-kafka-client"    | Kafka client id.                                                                                                                                                                                     | 
| SECURITY_PROTOCOL_CONFIG | "PLAINTEXT"                      | Security protocol used. Can be SSL or PLAINTEXT.                                                                                                                                                     |
| SASL_MECHANISM | None                             | SASL auth mechanism. Can be SCRAM-SHA-256 or SCRAM-SHA-512.                                                                                                                                          | 
| SASL_JAAS_CONFIG | None                             | Jaas configuration which will contain credentials.                                                                                                                                                   | 
| DATABASE_SUBSCRIPTION_PATTERN | ".*"                             | Regex which will used to pattern match databases to register kafka events.                                                                                                                           | 
| STREAM_DATABASE_SUBSCRIPTION_SERVICE_TIMEOUT_MILLIS | "500"                            | Frequency in milliseconds at which Kafka event subscription service will check for new databases and if the database should be subscibed to emit events on record create, update or delete to Kafka. | 
 | STREAM_DEFAULT_DATABASE_USERNAME | admin                            | Default username for database under which events  will be sent to Kafka if the database does not have a username set when created.                                                                   |

## 1. Demos

### 1.1 Included by default
Raft's Arcade image includes a food demo dataset that is pre imported for showing basic graph functions in arcade.


To view a good graph result set in the food dataset, run the following query
   ``` select *, bothE() as `@edges` from `FinalFood` ```

### 1.2 Classification handling


Arcade has been extended to optionally require general or source based classification markings on all graph nodes/edges for specific databases.

Classification/ACCM data access enforcements are currently limited to clearance checks and no foreign nationaity checks. More advanced ACCM controls to come.


To demo this functionality....

### Setup, before demo:

1. In keycloak, remove the Data Steward group from the admin user.

2. Create a new user attribute: `clearance-USA`, with value `U`.

3. Create a new user attribute `nationality` with value `USA`.

### Demo

1. Log into arcade http://arcadedb.localhost/ with the keycloak admin credentials. Replace `localhost` with your deployed domain as needed.

2. Create new database in the Arcade GUI. (Click on the database icon, 2nd from the top on the left menu). The create database button is in the upper right.

   a. Set `classification` to `S`.

   b. Leave `import ontology` unchecked.

3. Switch to the newly created database if needed in the database dropdown on the upper middle left of the screen.

4. Switch back to the first tab on the left menu, so you can run commands and see results.

5. Run the following script to create the People vertex type
   ```
   CREATE VERTEX TYPE People
   ```

6. Confirm the following command fails because no clearance markings are on the document
   ```
   INSERT INTO People CONTENT 
   {
     firstName: 'Enzo',
     lastName: 'Ferrari'
   }
   ```
   
7. Confirm the following command fails because TS is higher than the classification of the database (S)
   ```
   INSERT INTO People CONTENT 
   { 
     firstName: 'Enzo', 
     lastName: 'Ferrari', 
     classification: { general: 'TS'}
   }
   ```

#### Demonstrate that the user can't write objects that they would not have access to.

8. Confirm the following command fails because the user doesn't have sufficient clearance. This demonstrates that __general classification__ marking enforcement is in effect.
   ```
   INSERT INTO People CONTENT 
   { 
     firstName: 'Enzo', 
     lastName: 'Ferrari', 
     classification: { general: 'S'}
   }
   ```

9. Confirm the following command fails because the user doesn't have sufficient clearance. This demonstrates that __source based__ classification marking enforcement is in effect.
   ```
   INSERT INTO People CONTENT 
   { 
     firstName: 'Enzo U',
     lastName: 'Ferrari',
     sources: {
       '1': '(S) ABC 33322'
     }
   }
   ```
   
#### Demonstrate that the user can create unclass objects

10. Confirm the following command succeeds, because the user can create unclass objects with __general__ classification markings.
   ```
   INSERT INTO People CONTENT 
   { 
     firstName: 'Enzo1', 
     lastName: 'Ferrari', 
     classification: { general: 'U'}
   }
   ```

11. Confirm the following command succeeds, because the user can create unclass objects with __source based__ classification markings.
   ```
   INSERT INTO People CONTENT 
   { 
     firstName: 'Enzo2',
     lastName: 'Ferrari',
     sources: {
       '1': '(U) ABC 33322'
     }
   }
   ```

12. In keycloak, bump the user's `clearance-USA` attribute to `S`.

#### Demonstrate that the user can now create Secret classified objects.

13. In Arcade, confirm the following command succeeds now with general classification markings
   ```
   INSERT INTO People CONTENT 
   { 
    firstName: 'Enzo3', 
    lastName: 'Ferrari', 
    classification: { general: 'S'}
   }
   ```

14. Confirm the following command succeeds now with source based classification markings
   ```
   INSERT INTO People CONTENT 
   { 
     firstName: 'Enzo4',
     lastName: 'Ferrari',
     sources: {
       '1': '(S) ABC 33322'
     }
   }
   ```

15. Create the following object with an ACCM no foreign attribute in preperation for demoing basic ACCM handling.
   ```
   INSERT INTO People CONTENT 
   { 
     firstName: 'Enzo5',
     lastName: 'Ferrari',
     sources: {
       '1': '(S//NF) IIR 12345'
     }
   }
   ```

#### Demonstrate that all __U__, __S__, and __S No Foreign__ objects are visible to a US national with a Secret clearance

16. Run the following command, and confirm that 5 objects are returned
   ```
   select from `People` limit 30
   ```

#### Demonstrate that the previously created object with a __No Foreign__ attribute are hidden from a foreign national

17. In keycloak, update the user's nationality attribute to `GBR`.

18. Rerun the select command above, and confirm that only 4 objects are returned. The No Foreign object should be hidden.

19. Confirm the following command fails where a foreign national tries to create a no foreign marked object.
   ```
   INSERT INTO People CONTENT 
   { 
     firstName: 'Enzo6',
     lastName: 'Ferrari',
     sources: {
       '1': '(S//NF) IIR 12345'
     }
   }
   ```

#### Demonstrate that users can't see objects higher than their clearance level.

20. In keycloak, update the user's `clearance-USA` attribute to `U`.

21. In arcade, run the following query and confirm that only 2 objects are returned, both of them unclass.
   ```
   select from `People` limit 30
   ```

#### Now we are going to demonstrate data access enforcement with graph relationships

22. In keycloak, revert the value of the `clearance-USA` attribute back to `S`

23. In keycloak, revert the `nationality` attribute back to `USA`

24. In aracde, run the following command to create a new `Location` node type
   ```
   CREATE VERTEX TYPE Location
   ```

25. Create a new Location object
   ```
   INSERT INTO Location CONTENT 
   { 
     name: 'Richmond',
     type: 'City',
     classification: {
       general: 'U'
     }
   }
   ```

26. Create a new edge type, LivesIn
   ```
   CREATE EDGE TYPE LivesIn
   ```

27. Confirm that new edge creation fails with invalid classification markings
   ```
   CREATE EDGE LivesIn 
   FROM (SELECT FROM People WHERE firstName = 'Enzo1') TO
        (SELECT FROM Location WHERE name = 'Richmond')
   ```
28. Confirm that new edge creation succeeds with proper classification markings
   ```
   CREATE EDGE LivesIn 
   FROM (SELECT FROM People WHERE firstName = 'Enzo1') TO
        (SELECT FROM Location WHERE name = 'Richmond')
   CONTENT {
     sources: {
       '1': '(S//NF) IIR 12345'
     }
   }
   ```

29. Run the following command to load the person for which we just added an edge.
```
SELECT FROM People WHERE firstName = 'Enzo1'
```

30. In the Arcade graph result GUI, select the returned node

31. In the context paragraph on the right side, select the `both` button to load incoming and outgoing edges 

32. Confirm the LivesIn edge, and location Richmond load into the GUI.

#### Now we will demonstrate that this relationship will not load if you don't have permissions to the relationship

33. In keycloak, update the user's `nationality` attribute to `GBR`.

34. Repeat steps 29 - 31. Confirm the LivesIn edge and location Richmond __do not__ load

