val `scala 212` = "2.12.10"

val scalacOpts = Seq(
  "-deprecation", // Emit warning and location for usages of deprecated APIs.
  "-encoding",
  "utf-8", // Specify character encoding used by source files.
  "-explaintypes", // Explain type errors in more detail.
  "-feature", // Emit warning and location for usages of features that should be imported explicitly.
  "-language:existentials", // Existential types (besides wildcard types) can be written and inferred
  "-language:experimental.macros", // Allow macro definition (besides implementation and application)
  "-language:higherKinds", // Allow higher-kinded types
  "-language:implicitConversions", // Allow definition of implicit functions called views
  "-unchecked", // Enable additional warnings where generated code depends on assumptions.
  "-Xcheckinit", // Wrap field accessors to throw an exception on uninitialized access.
  "-Xfatal-warnings", // Fail the compilation if there are any warnings.
  "-Xfuture", // Turn on future language features.
  "-Xlint:adapted-args", // Warn if an argument list is modified to match the receiver.
  "-Xlint:by-name-right-associative", // By-name parameter of right associative operator.
  "-Xlint:delayedinit-select", // Selecting member of DelayedInit.
  "-Xlint:doc-detached", // A Scaladoc comment appears to be detached from its element.
  "-Xlint:inaccessible", // Warn about inaccessible types in method signatures.
  "-Xlint:infer-any", // Warn when a type argument is inferred to be `Any`.
  "-Xlint:missing-interpolator", // A string literal appears to be missing an interpolator id.
  "-Xlint:nullary-override", // Warn when non-nullary `def f()' overrides nullary `def f'.
  "-Xlint:nullary-unit", // Warn when nullary methods return Unit.
  "-Xlint:option-implicit", // Option.apply used implicit view.
  "-Xlint:package-object-classes", // Class or object defined in package object.
  "-Xlint:poly-implicit-overload", // Parameterized overloaded implicit methods are not visible as view bounds.
  "-Xlint:private-shadow", // A private field (or class parameter) shadows a superclass field.
  "-Xlint:stars-align", // Pattern sequence wildcard must align with sequence component.
  "-Xlint:type-parameter-shadow", // A local type parameter shadows a type already in scope.
  "-Xlint:unsound-match", // Pattern match may not be typesafe.
  "-Yno-adapted-args", // Do not adapt an argument list (either by inserting () or creating a tuple) to match the receiver.
  "-Ypartial-unification", // Enable partial unification in type constructor inference
  "-Ywarn-dead-code", // Warn when dead code is identified.
  "-Ywarn-inaccessible", // Warn about inaccessible types in method signatures.
  "-Ywarn-infer-any", // Warn when a type argument is inferred to be `Any`.
  "-Ywarn-nullary-override", // Warn when non-nullary `def f()' overrides nullary `def f'.
  "-Ywarn-nullary-unit", // Warn when nullary methods return Unit.
  "-Ywarn-numeric-widen" // Warn when numerics are widened.
)

lazy val customResolvers = Seq(
  Resolver.bintrayRepo("ovotech", "maven"),
  "Confluent" at "https://packages.confluent.io/maven/"
)

val V = new {
  val scalatest      = "3.0.8"
  val kindProjector  = "0.10.3"
  val silencer       = "1.4.2"
  val cats           = "2.0.0"
  val catsEffect     = "2.0.0"
  val fs2            = "2.1.0"
  val logEffect      = "0.12.0"
  val refined        = "0.9.10"
  val logbackClassic = "1.2.3"
  val fs2Kafka       = "0.19.9"
  val circe          = "0.12.3"
  val circeOptics    = "0.12.0"
  val laserdisc      = "0.2.3"
  val http4s         = "0.20.14"
  val confluent      = "5.3.1"
  val avro           = "1.9.1"
  val prometheus     = "0.9.0-M5"
  val diffson        = "4.0.1"
}

lazy val appDependencies = Seq(
  "com.github.ghik"         %% "silencer-lib"         % V.silencer,
  "org.typelevel"           %% "cats-core"            % V.cats,
  "org.typelevel"           %% "cats-effect"          % V.catsEffect,
  "io.laserdisc"            %% "log-effect-core"      % V.logEffect,
  "io.laserdisc"            %% "log-effect-fs2"       % V.logEffect,
  "co.fs2"                  %% "fs2-core"             % V.fs2,
  "eu.timepit"              %% "refined"              % V.refined,
  "com.ovoenergy"           %% "fs2-kafka"            % V.fs2Kafka,
  "org.apache.avro"         % "avro"                  % V.avro,
  "io.confluent"            % "kafka-avro-serializer" % V.confluent,
  "io.laserdisc"            %% "log-effect-core"      % V.logEffect,
  "io.laserdisc"            %% "log-effect-fs2"       % V.logEffect,
  "io.circe"                %% "circe-core"           % V.circe,
  "io.circe"                %% "circe-generic"        % V.circe,
  "io.circe"                %% "circe-parser"         % V.circe,
  "io.circe"                %% "circe-optics"         % V.circeOptics,
  "com.github.ghik"         %% "silencer-lib"         % V.silencer,
  "io.laserdisc"            %% "laserdisc-core"       % V.laserdisc,
  "io.laserdisc"            %% "laserdisc-fs2"        % V.laserdisc,
  "org.http4s"              %% "http4s-dsl"           % V.http4s,
  "org.http4s"              %% "http4s-circe"         % V.http4s,
  "org.http4s"              %% "http4s-blaze-client"  % V.http4s,
  "org.lyranthe.prometheus" %% "client"               % V.prometheus,
  "org.gnieh"               %% "diffson-circe"        % V.diffson,
  "ch.qos.logback"          % "logback-classic"       % V.logbackClassic % Test,
  "org.scalatest"           %% "scalatest"            % V.scalatest % Test
) map (_.withSources)

lazy val compilerPluginsDependencies = Seq(
  compilerPlugin(
    "org.typelevel" %% "kind-projector" % V.kindProjector cross CrossVersion.binary
  ),
  compilerPlugin("com.github.ghik" %% "silencer-plugin" % V.silencer)
)

lazy val root = project
  .in(file("."))
  .settings(
    name                := "stream-it",
    scalaVersion        := `scala 212`,
    scalacOptions       ++= scalacOpts,
    organization        := "io.laserdisc",
    resolvers           ++= customResolvers,
    libraryDependencies ++= appDependencies ++ compilerPluginsDependencies,
    addCommandAlias("format", ";scalafmt;test:scalafmt;scalafmtSbt"),
    addCommandAlias(
      "updates",
      ";dependencyUpdates; reload plugins; dependencyUpdates;reload return"
    ),
    addCommandAlias("fullBuild", ";checkFormat;clean;test"),
    addCommandAlias(
      "fullCiBuild",
      ";set scalacOptions in ThisBuild ++= Seq(\"-opt:l:inline\", \"-opt-inline-from:**\");fullBuild"
    ),
    addCommandAlias(
      "checkFormat",
      ";scalafmtCheck;test:scalafmtCheck;scalafmtSbtCheck"
    )
  )
