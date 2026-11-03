plugins {
    application
}

application {
    mainClass.set("dev.hononeko.koshin.proxy.ApplicationKt")
}

dependencies {
    implementation(project(":core"))
    implementation(project(":adapters:nats"))
    implementation(project(":adapters:state-valkey"))
    implementation(project(":adapters:github-app"))
    implementation(libs.kotlinx.coroutines.core)
    implementation(libs.ktor.server.core)
    implementation(libs.ktor.server.netty)
    implementation(libs.ktor.server.content.negotiation)
    implementation(libs.ktor.serialization.kotlinx.json)
    implementation(libs.ktor.client.core)
    implementation(libs.ktor.client.java)
    implementation(libs.nats.jnats)
    implementation(libs.lettuce.core)
    implementation(libs.logback.classic)
    implementation(libs.logstash.logback.encoder)
    implementation(libs.jul.to.slf4j)

    testImplementation(libs.ktor.server.test.host)
    testImplementation(libs.ktor.client.mock)
    testImplementation(libs.kotlinx.coroutines.test)
    testImplementation(libs.testcontainers.core)
    testImplementation(libs.testcontainers.junit)
}

