dependencies {
    api(project(":mcp-policy"))
    // JSqlParser is load-bearing, not a convenience: the table allowlist is enforced against
    // names resolved from the parsed AST. Matching on the raw SQL string is the hole that
    // produced the published injection bug in the reference Postgres MCP server.
    implementation(libs.jsqlparser)
    implementation(libs.hikaricp)

    testImplementation(libs.logback.classic)
    testImplementation(platform(libs.testcontainers.bom))
    testImplementation(libs.testcontainers.junit)
    testImplementation(libs.testcontainers.postgresql)
    testRuntimeOnly(libs.postgresql)
}
