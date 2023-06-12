import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

val awaitilityVersion = "4.2.0"
val awsSdkVersion = "2.20.83"
val kotlinLoggingVersion = "3.0.5"
val kotlinTestVersion = "5.6.2"
val testContainersVersion = "1.18.3"

plugins {
    id("org.springframework.boot") version "3.0.5"
    id("io.spring.dependency-management") version "1.1.0"
    kotlin("jvm") version "1.7.22"
    kotlin("plugin.spring") version "1.8.21"
}

group = "com.example"
version = "0.0.1-SNAPSHOT"
java.sourceCompatibility = JavaVersion.VERSION_17

repositories {
    mavenCentral()
}

extra["testcontainersVersion"] = "1.17.6"

dependencies {
    implementation("org.springframework.boot:spring-boot-starter-webflux")
    implementation("io.projectreactor.kotlin:reactor-kotlin-extensions")
    implementation("io.projectreactor:reactor-core-micrometer")
    implementation("org.jetbrains.kotlin:kotlin-reflect")
    implementation("com.fasterxml.jackson.module:jackson-module-kotlin")
    implementation("io.github.microutils:kotlin-logging:$kotlinLoggingVersion")

    testImplementation("org.springframework.boot:spring-boot-starter-test")
    testImplementation("org.testcontainers:junit-jupiter")
    testImplementation("org.testcontainers:junit-jupiter:$testContainersVersion")
    testImplementation("io.kotest:kotest-runner-junit5:$kotlinTestVersion")
    testImplementation("io.kotest:kotest-assertions-core:$kotlinTestVersion")
    testImplementation("org.awaitility:awaitility-kotlin:$awaitilityVersion")
    testImplementation("org.testcontainers:localstack:$testContainersVersion")
    implementation(platform("software.amazon.awssdk:bom:$awsSdkVersion"))
    testImplementation("com.amazonaws:aws-java-sdk-core:1.12.479") {
        exclude(group = "*", module = "*")
    }
    implementation("software.amazon.awssdk:s3")
    implementation("software.amazon.awssdk:netty-nio-client")
    implementation("software.amazon.awssdk:transcribe")
}

dependencyManagement {
    imports {
        mavenBom("org.testcontainers:testcontainers-bom:${property("testcontainersVersion")}")
    }
}

tasks.withType<KotlinCompile> {
    kotlinOptions {
        freeCompilerArgs = listOf("-Xjsr305=strict")
        jvmTarget = "17"
    }
}

tasks.withType<Test> {
    useJUnitPlatform()
}
