dependencies {
    api(project(":mcp-policy"))
    // JSqlParser is load-bearing, not a convenience: the table allowlist is enforced against
    // names resolved from the parsed AST. Matching on the raw SQL string is the hole that
    // produced the published injection bug in the reference Postgres MCP server.
    implementation(libs.jsqlparser)
    implementation(libs.hikaricp)

    testImplementation(libs.logback.classic)
    // H2 in-memory. The redaction rules are a security boundary and belong in the fast suite,
    // which must run with no Docker daemon; H2 supplies a real driver and real ResultSet
    // metadata, which is exactly what those rules read.
    testImplementation(libs.h2)
    testImplementation(platform(libs.testcontainers.bom))
    testImplementation(libs.testcontainers.junit)
    testImplementation(libs.testcontainers.postgresql)
    testRuntimeOnly(libs.postgresql)
}
