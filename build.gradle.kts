plugins {
    alias(libs.plugins.kotlin.jvm) apply false
    alias(libs.plugins.kotlin.serialization) apply false
    alias(libs.plugins.detekt)
    alias(libs.plugins.kover)
    id("base")
}

repositories {
    mavenCentral()
}

dependencies {
    detektPlugins(libs.detekt.rules.ktlint.wrapper)

    kover(project(":core"))
    kover(project(":adapters:nats"))
    kover(project(":adapters:state-valkey"))
    kover(project(":adapters:github-app"))
    kover(project(":proxy"))
    kover(project(":worker"))
}

kover {
    reports {
        total {
            xml {
                onCheck = true
            }
            html {
                onCheck = true
            }
            verify {
                onCheck = true
                rule {
                    minBound(0)
                }
            }
        }
    }
}

subprojects {
    if (childProjects.isNotEmpty()) {
        return@subprojects
    }

    apply(plugin = "org.jetbrains.kotlin.jvm")
    apply(plugin = "org.jetbrains.kotlin.plugin.serialization")
    apply(plugin = "dev.detekt")
    apply(plugin = "org.jetbrains.kotlinx.kover")

    repositories {
        mavenCentral()
    }

    dependencies {
        "detektPlugins"(rootProject.libs.detekt.rules.ktlint.wrapper)
        "testImplementation"(rootProject.libs.junit.jupiter)
        "testImplementation"(rootProject.libs.mockk)
        "testRuntimeOnly"(rootProject.libs.junit.platform.launcher)
    }

    configure<org.jetbrains.kotlin.gradle.dsl.KotlinJvmProjectExtension> {
        jvmToolchain(rootProject.libs.versions.jvm.get().toInt())
        compilerOptions {
            allWarningsAsErrors.set(true)
        }
    }

    tasks.withType<Test>().configureEach {
        useJUnitPlatform()
    }

    detekt {
        buildUponDefaultConfig = true
        config.setFrom(files(rootProject.layout.projectDirectory.file("config/detekt/detekt.yml")))
    }

    tasks.withType<dev.detekt.gradle.Detekt>().configureEach {
        reports {
            html.required.set(true)
            sarif.required.set(true)
            checkstyle.required.set(false)
            markdown.required.set(false)
        }
    }

    tasks.named("check") {
        dependsOn("detektMain", "detektTest")
    }
}
