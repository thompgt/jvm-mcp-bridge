dependencies {
    api(project(":mcp-policy"))
    implementation(libs.kafka.clients)

    testImplementation(libs.logback.classic)
    testImplementation(platform(libs.testcontainers.bom))
    testImplementation(libs.testcontainers.junit)
    testImplementation(libs.testcontainers.kafka)
}
