plugins {
    alias(libs.plugins.spring.boot)
    alias(libs.plugins.spring.dependency.management)
}

// Both Jackson generations are on this module's classpath: Spring Boot 3.5 is on Jackson 2
// (com.fasterxml.jackson), the MCP SDK 2.0 is on Jackson 3 (tools.jackson). They coexist —
// except for jackson-annotations, which BOTH generations share. Jackson 3.0.3 needs
// JsonFormat.Shape.POJO, added in annotations 2.20, and Boot's BOM pins the Jackson 2 line to
// 2.19. Left alone, the server starts, registers its tools, and then dies with
// NoSuchFieldError the moment the SDK validates a tool schema.
//
// Forcing annotations to 2.20 satisfies both: the 2.x annotations line is compatible with
// Boot's 2.19 databind, and it is the only coordinate the two generations share, so nothing
// else in either stack moves.
// Declared through the Spring dependency-management plugin rather than a Gradle
// resolutionStrategy force: the plugin applies its own managed versions after resolution
// strategies run, so a force here loses and the jar silently ships 2.19 again.
dependencyManagement {
    dependencies {
        dependency("com.fasterxml.jackson.core:jackson-annotations:2.20")
    }
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

// McpRoundTripTest launches the real jar as a subprocess and talks JSON-RPC to it over pipes,
// which is the only way to catch a stdout-corruption or bad-schema bug. That means the tests
// depend on the packaged artifact, not just on the classes.
tasks.named<Test>("test") {
    val bootJar = tasks.named<org.springframework.boot.gradle.tasks.bundling.BootJar>("bootJar")
    dependsOn(bootJar)
    jvmArgumentProviders.add(
        CommandLineArgumentProvider {
            listOf("-Dbridge.jar=${bootJar.get().archiveFile.get().asFile.absolutePath}")
        }
    )
}
