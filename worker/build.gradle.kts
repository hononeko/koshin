dependencies {
    implementation(project(":core"))
    implementation(libs.kotlinx.coroutines.core)
    implementation(libs.logback.classic)

    testImplementation(libs.kotlinx.coroutines.test)
}
