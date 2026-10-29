dependencies {
    testImplementation(project(":core"))
    testImplementation(project(":adapters:nats"))
    testImplementation(project(":adapters:state-valkey"))
    testImplementation(project(":proxy"))
    testImplementation(project(":worker"))
    testImplementation(libs.ktor.server.core)
    testImplementation(libs.ktor.server.netty)
    testImplementation(libs.nats.jnats)
    testImplementation(libs.lettuce.core)
    testImplementation(libs.kotlinx.coroutines.core)
    testImplementation(libs.kotlinx.serialization.json)
    testImplementation(libs.logback.classic)
    testImplementation(libs.testcontainers.core)
    testImplementation(libs.testcontainers.junit)
}

// Every test here is an integration test, so `test` matches none.
tasks.named<Test>("test") {
    filter {
        isFailOnNoMatchingTests = false
    }
}
