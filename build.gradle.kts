plugins {
    alias(libs.plugins.spring.boot) apply false
    alias(libs.plugins.spring.dependency.management) apply false
}

subprojects {
    // `java-library`, not `java`: modules distinguish `api` from `implementation` so that an
    // adapter cannot accidentally leak its driver onto a consumer's compile classpath.
    apply(plugin = "java-library")

    group = "io.github.thompgt.jvmmcp"
    version = "0.1.0-SNAPSHOT"

    // Repositories are declared once in settings.gradle.kts under PREFER_SETTINGS.

    extensions.configure<JavaPluginExtension> {
        sourceCompatibility = JavaVersion.VERSION_21
    }

    // Every module compiles with `-Werror`. This server sits between an LLM and a production
    // database; an unchecked cast or a leaked resource warning is exactly the class of bug
    // that turns into a data incident, so none are allowed to accumulate.
    tasks.withType<JavaCompile>().configureEach {
        // `release` rather than a toolchain: JDK 24 is what's installed locally and on CI,
        // but MCP servers get embedded in other people's applications, so the bytecode has
        // to stay at 21. `release` also pins the platform API, which sourceCompatibility
        // alone does not.
        options.release.set(21)
        options.compilerArgs.addAll(
            listOf("-Xlint:all", "-Xlint:-processing", "-Werror", "-parameters")
        )
        options.encoding = "UTF-8"
    }

    tasks.withType<Test>().configureEach {
        // Integration tests that need Docker are tagged `integration` and excluded from the
        // default `test` run, so `./gradlew build` stays fast and works without a Docker
        // daemon. CI runs them explicitly via `-PincludeIntegration`.
        useJUnitPlatform {
            if (!project.hasProperty("includeIntegration")) {
                excludeTags("integration")
            }
        }
        testLogging {
            events("passed", "skipped", "failed")
            exceptionFormat = org.gradle.api.tasks.testing.logging.TestExceptionFormat.FULL
        }
    }

    dependencies {
        val libs = rootProject.extensions
            .getByType<VersionCatalogsExtension>()
            .named("libs")

        add("testImplementation", platform(libs.findLibrary("junit-bom").get()))
        add("testImplementation", libs.findLibrary("junit-jupiter").get())
        add("testImplementation", libs.findLibrary("assertj-core").get())
        add("testRuntimeOnly", libs.findLibrary("junit-platform-launcher").get())
    }
}
