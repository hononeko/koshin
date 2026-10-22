dependencies {
    implementation(project(":core"))
    implementation(libs.nats.jnats)
    implementation(libs.kotlinx.coroutines.core)
    implementation(libs.kotlinx.serialization.json)
    implementation(libs.slf4j.api)

    testImplementation(libs.logback.classic)
    testImplementation(libs.kotlinx.coroutines.test)
    testImplementation(libs.testcontainers.core)
    testImplementation(libs.testcontainers.junit)
}
