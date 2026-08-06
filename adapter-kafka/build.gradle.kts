dependencies {
    api(project(":mcp-policy"))
    implementation(libs.kafka.clients)

    testImplementation(libs.logback.classic)
    testImplementation(platform(libs.testcontainers.bom))
    testImplementation(libs.testcontainers.junit)
    // Redpanda, matching docker-compose.yml: one binary, no KRaft bootstrap dance, and the
    // same Kafka protocol. The apache/kafka image needs cluster configuration this test has
    // no reason to own.
    testImplementation(libs.testcontainers.redpanda)
}
