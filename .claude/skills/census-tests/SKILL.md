---
name: census-tests
description: Use when writing or reviewing a census test (a test shipped as "the next offender is a red build") - how to enumerate from the authority it protects, CompiledClasses / CallOperands / ShapeProbes in the census module, and the "scan still reaches X" assertion every census must carry.
---

# Census tests enumerate from the authority they protect (#11199)

A test shipped as "the next offender is a red
build" must range over the thing that defines an offender, and must assert that it reached it:
- if the authority is a method, scan the compiled call sites of that method (see
  `WholeNumberArgumentsTest.NoNarrowingReads`, `SchemaPresentationReadableWalkCensusTest`); if it is a table or
  registry, read its key set (see `Issue11170MCPToolAnnotationsTest`, `RemoteExceptionRebuilder.names()`)
- a census over more than one module goes in the `census` module, on `CompiledClasses` (every class of every module
  whatever its package, call sites keyed `class#method`, a named nested class keyed `Outer$Nested`, lambdas and
  anonymous classes folded into the method and class they are written in); a probe of the shapes it must see goes in
  that module's tests (`ShapeProbes`) and is read the same way
- an offender that is a call with a particular ARGUMENT (a format constant against the varargs that follow it, a value
  handed on from a request, a message formatted without a `Locale`) reads what produced each operand through
  `CallOperands` (the method's data flow, followed through locals, casts and into the helper that returned the value),
  never the argument's spelling. A constant javac folds still defeats it, and that is the one reason left to read text
- following calls transitively (`CompiledClasses.methodsReaching`) is right when a helper really does the work (an
  inherited `registerRunningQuery`), and turns a census vacuous when a shared base does something every subclass
  then inherits (the algo base checks the guard while loading the graph): pick one, and keep a control that fails
- a census rooted in a directory, a module list, a package or a source spelling can be left by writing the offender
  somewhere else, and a reviewer reading it cannot tell. A test that still reads `.java` text is listed with its
  reason in `SourceTextTestsAreRecordedTest` (census module), which fails on a new one (#11288)
- every census carries at least one assertion of the form "the scan still reaches X", where X names the authority and
  the failure message names it too
