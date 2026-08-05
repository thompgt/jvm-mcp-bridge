rootProject.name = "jvm-mcp-bridge"

// Layered on purpose: mcp-core and mcp-policy know nothing about any backend, so they
// stay unit-testable without Docker. Adapters depend on both and on exactly one backend
// driver each. server-app is the only module that sees Spring.
include("mcp-core")
include("mcp-policy")
include("adapter-jdbc")
include("adapter-kafka")
include("adapter-jvm")
include("adapter-http")
include("server-app")

dependencyResolutionManagement {
    repositoriesMode.set(RepositoriesMode.PREFER_SETTINGS)
    repositories {
        mavenCentral()
    }
}
