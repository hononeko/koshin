dependencies {
    implementation(libs.kotlinx.coroutines.core)
    implementation(libs.kotlinx.serialization.json)

    testImplementation(libs.kotlinx.coroutines.test)
}

kover {
    reports {
        total {
            verify {
                rule("Core coverage line verification") {
                    minBound(95)
                }
            }
        }
    }
}
