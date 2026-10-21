dependencies {
    implementation(project(":core"))
    implementation(libs.nats.jnats)
    implementation(libs.kotlinx.coroutines.core)
    implementation(libs.kotlinx.serialization.json)

    testImplementation(libs.kotlinx.coroutines.test)
    testImplementation(libs.testcontainers.core)
    testImplementation(libs.testcontainers.junit)
}
