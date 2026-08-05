dependencies {
    api(project(":mcp-policy"))
    // OpenAPI 3 documents are parsed once at startup and turned into MCP tools; the JDK
    // HttpClient does the calling, so there is no HTTP client dependency here.
    implementation(libs.swagger.parser)

    testImplementation(libs.logback.classic)
}
