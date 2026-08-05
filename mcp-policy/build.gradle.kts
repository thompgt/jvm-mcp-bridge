// The guardrail engine is backend-agnostic: it reasons about tool calls and declared
// resource names, never about SQL or topics. That keeps every rule unit-testable with no
// Docker and no live backend.
dependencies {
    api(project(":mcp-core"))

    testImplementation(libs.logback.classic)
}
