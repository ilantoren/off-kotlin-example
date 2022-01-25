plugins {
    java
    kotlin("jvm") version "1.6.0"
}

group = "org.example"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    testImplementation("org.junit.jupiter:junit-jupiter-api:5.6.0")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine")
    implementation("org.apache.logging.log4j:log4j-core:2.17.1")
    implementation("org.apache.logging.log4j:log4j-slf4j-impl:2.17.1")
    implementation("org.slf4j:slf4j-simple:2.0.0-alpha6")
    implementation("org.mongodb:mongodb-driver-reactivestreams:4.4.1")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-bom:1.6.0-native-mt")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.6.0-native-mt")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-reactor:1.6.0-native-mt")
    implementation("org.jetbrains.kotlinx:kotlinx-serialization-json:1.3.2")
}

tasks.getByName<Test>("test") {
    useJUnitPlatform()
}