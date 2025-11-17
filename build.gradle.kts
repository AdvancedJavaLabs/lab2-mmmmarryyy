plugins {
    kotlin("jvm") version "1.9.20"
    java
    application
}

group = "org.itmo"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    implementation("jakarta.jms:jakarta.jms-api:2.0.1")
    implementation("org.apache.activemq:activemq-broker:6.1.1")
    testImplementation(kotlin("test"))

    implementation("com.rabbitmq:amqp-client:5.16.0")
    implementation("org.apache.kafka:kafka-clients:3.5.1")
}

tasks.test {
    useJUnitPlatform()
}

kotlin {
    jvmToolchain(21)
}

application {
    mainClass.set("Main")
}