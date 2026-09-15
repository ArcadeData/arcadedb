// Bolt conformance type-round-trip fixture (issue #4883).
// Seed via HTTP /command (language=cypher), NOT via a Bolt session - see
// fixtures.type_matrix.seeded_by in spec.yaml for why.
// Verified to execute successfully against a live ArcadeDB instance
// (2026-07-03, embedded Cypher engine, ArcadeDB MCP tools).
// nestedMapProp was dropped (issue #7629): a map-valued property is not a
// primitive/array-of-primitives, so real Neo4j refuses it too - it was never
// a valid type for this fixture to claim round-trips, and testing it here
// exercised a gap rather than conformance.
CREATE (:TypeMatrix {
  localDateProp: date('2026-01-15'),
  localTimeProp: localtime('14:30:00'),
  localDateTimeProp: localdatetime('2026-01-15T14:30:00'),
  offsetDateTimeProp: datetime('2026-01-15T14:30:00+02:00'),
  durationProp: duration('P1DT2H30M'),
  pointProp: point({x: 12.34, y: 56.78}),
  nestedListProp: [1, 2, [3, 4]],
  nullProp: null
});
