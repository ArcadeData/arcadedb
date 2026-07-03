// Bolt conformance type-round-trip fixture (issue #4883).
// Seed via HTTP /command (language=cypher), NOT via a Bolt session - see
// fixtures.type_matrix.seeded_by in spec.yaml for why.
// Verified to execute successfully against a live ArcadeDB instance
// (2026-07-03, embedded Cypher engine, ArcadeDB MCP tools).
CREATE (:TypeMatrix {
  localDateProp: date('2026-01-15'),
  localTimeProp: localtime('14:30:00'),
  localDateTimeProp: localdatetime('2026-01-15T14:30:00'),
  offsetDateTimeProp: datetime('2026-01-15T14:30:00+02:00'),
  durationProp: duration('P1DT2H30M'),
  pointProp: point({x: 12.34, y: 56.78}),
  nestedListProp: [1, 2, [3, 4]],
  nestedMapProp: {a: 1, b: {c: 2}},
  nullProp: null
});
