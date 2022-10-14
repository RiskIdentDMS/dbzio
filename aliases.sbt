addCommandAlias(
  "fmt",
  """;eval println("Formatting source code");scalafmt;eval println("Formatting test code");Test / scalafmt;eval println("Formatting SBT files");scalafmtSbt"""
)
