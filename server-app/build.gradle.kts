plugins {
    alias(libs.plugins.spring.boot)
    alias(libs.plugins.spring.dependency.management)
}

// server-app is the only module that sees Spring. Boot is here for config binding, the
// servlet container, OAuth2 and Actuator — the MCP protocol itself is handled by the SDK
// directly, not by Spring AI's starters. See docs/adr/001.
dependencies {
    implementation(project(":mcp-core"))
    implementation(project(":mcp-policy"))
    implementation(project(":adapter-jdbc"))
    implementation(project(":adapter-kafka"))
    implementation(project(":adapter-jvm"))
    implementation(project(":adapter-http"))

    implementation(libs.spring.boot.starter)
    implementation(libs.spring.boot.starter.web)
    implementation(libs.spring.boot.starter.actuator)
    implementation(libs.postgresql)

    testImplementation(libs.spring.boot.starter.test)
    testImplementation(libs.mcp.test)
    testImplementation(platform(libs.testcontainers.bom))
    testImplementation(libs.testcontainers.junit)
    testImplementation(libs.testcontainers.postgresql)
}

tasks.named<org.springframework.boot.gradle.tasks.bundling.BootJar>("bootJar") {
    archiveFileName.set("jvm-mcp-bridge.jar")
}
