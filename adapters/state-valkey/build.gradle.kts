dependencies {
    implementation(project(":core"))
    implementation(libs.lettuce.core)
    implementation(libs.kotlinx.coroutines.core)
    implementation(libs.kotlinx.serialization.json)

    testImplementation(libs.kotlinx.coroutines.test)
    testImplementation(libs.testcontainers.core)
    testImplementation(libs.testcontainers.junit)
}
