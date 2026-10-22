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

    val javaVersion = rootProject.layout.projectDirectory.file(".java-version").asFile.readText().trim().toInt()

    configure<org.jetbrains.kotlin.gradle.dsl.KotlinJvmProjectExtension> {
        jvmToolchain(javaVersion)
        compilerOptions {
            allWarningsAsErrors.set(true)
        }
    }

    kover {
        currentProject {
            instrumentation {
                disabledForTestTasks.add("integrationTest")
            }
        }
    }

    tasks.withType<Test>().configureEach {
        useJUnitPlatform()
    }

    tasks.named<Test>("test") {
        filter {
            excludeTestsMatching("*IntegrationTest")
            excludeTestsMatching("*IT")
        }
    }

    val testTask = tasks.named<Test>("test")

    tasks.register<Test>("integrationTest") {
        description = "Runs integration tests using Testcontainers."
        group = "verification"
        testClassesDirs = testTask.get().testClassesDirs
        classpath = testTask.get().classpath
        filter {
            includeTestsMatching("*IntegrationTest")
            includeTestsMatching("*IT")
            isFailOnNoMatchingTests = false
        }
        shouldRunAfter(testTask)
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

tasks.register("integrationTest") {
    group = "verification"
    description = "Runs integration tests across all subprojects."
    dependsOn(subprojects.map { it.tasks.matching { t -> t.name == "integrationTest" } })
}
