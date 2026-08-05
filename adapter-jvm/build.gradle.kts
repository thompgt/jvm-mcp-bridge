// JMX, JFR and the platform MXBeans all ship with the JDK, so this adapter needs no
// third-party client. That is the whole argument for writing this bridge in Java.
dependencies {
    api(project(":mcp-policy"))

    testImplementation(libs.logback.classic)
}
