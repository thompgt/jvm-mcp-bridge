// mcp-core deliberately depends on the MCP SDK and nothing else. No Spring, no JDBC driver,
// no Kafka client. If a dependency ever needs adding here, that is a signal the abstraction
// leaked and the code belongs in an adapter instead.
// JSON goes through the SDK's own McpJsonMapper rather than a direct Jackson dependency:
// the SDK 2.0 bundle is built on Jackson 3 (`tools.jackson`), while Spring Boot 3.5 in
// server-app is on Jackson 2. Binding to neither here keeps that split from mattering.
dependencies {
    api(libs.mcp)
    api(libs.slf4j.api)

    testImplementation(libs.logback.classic)
}
