plugins {
    application
}

application {
    mainClass.set("dev.hononeko.koshin.proxy.ApplicationKt")
}

dependencies {
    implementation(project(":core"))
    implementation(project(":adapters:nats"))
    implementation(libs.ktor.server.core)
    implementation(libs.ktor.server.netty)
    implementation(libs.ktor.server.content.negotiation)
    implementation(libs.ktor.serialization.kotlinx.json)
    implementation(libs.nats.jnats)
    implementation(libs.logback.classic)
    implementation(libs.logstash.logback.encoder)
    implementation(libs.jul.to.slf4j)

    testImplementation(libs.ktor.server.test.host)
    testImplementation(libs.kotlinx.coroutines.test)
}

