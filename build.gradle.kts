plugins {
    signing
    `java-library`
    `maven-publish`
    id("org.springframework.boot") version "3.3.5"
    id("io.spring.dependency-management") version "1.1.6"
    id("com.google.protobuf") version "0.9.4"
}

group = "io.github.orz-api"
version = "0.0.3"

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(17)
    }
    withJavadocJar()
    withSourcesJar()
}

configurations {
    compileOnly {
        extendsFrom(configurations.annotationProcessor.get())
    }
    configureEach {
        resolutionStrategy {
            cacheDynamicVersionsFor(0, "seconds")
            cacheChangingModulesFor(0, "seconds")
        }
    }
}

repositories {
    maven {
        name = "Central Portal Snapshots"
        url = uri("https://central.sonatype.com/repository/maven-snapshots/")
        content {
            includeVersionByRegex(".*", ".*", ".*SNAPSHOT")
        }
    }
    mavenCentral()
    maven {
        url = uri("https://packages.confluent.io/maven/")
    }
}

dependencies {
    api("io.github.orz-api:orz-base-spring-boot-starter:0.0.3")

    compileOnly("org.springframework.kafka:spring-kafka")
    testImplementation("org.springframework.kafka:spring-kafka")
    compileOnly("com.ctrip.framework.apollo:apollo-client:2.2.0")
    testImplementation("com.ctrip.framework.apollo:apollo-client:2.2.0")

    compileOnly("io.confluent:kafka-json-schema-serializer:7.7.0")
    testImplementation("io.confluent:kafka-json-schema-serializer:7.7.0")
    compileOnly("io.confluent:kafka-protobuf-serializer:7.7.0")
    testImplementation("io.confluent:kafka-protobuf-serializer:7.7.0")
    compileOnly("com.google.protobuf:protobuf-java:3.25.4")
    testImplementation("com.google.protobuf:protobuf-java:3.25.4")
    compileOnly("com.google.protobuf:protobuf-java-util:3.25.4")
    testImplementation("com.google.protobuf:protobuf-java-util:3.25.4")

    annotationProcessor("org.springframework.boot:spring-boot-configuration-processor")
    testImplementation("org.springframework.boot:spring-boot-starter-test")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")

    compileOnly("org.projectlombok:lombok")
    testCompileOnly("org.projectlombok:lombok")
    annotationProcessor("org.projectlombok:lombok")
    testAnnotationProcessor("org.projectlombok:lombok")

    testImplementation("org.springframework.boot:spring-boot-testcontainers")
    testImplementation("org.testcontainers:junit-jupiter")
    testImplementation("org.testcontainers:kafka")
}

tasks.withType<Test> {
    useJUnitPlatform()
}

tasks.javadoc {
    val options = (options as StandardJavadocDocletOptions)
    options.encoding("UTF-8")
    options.addStringOption("Xdoclint:none", "-quiet")
    if (JavaVersion.current().isJava9Compatible) {
        options.addBooleanOption("html5", true)
    }
}

publishing {
    publications {
        create<MavenPublication>("mavenJava") {
            from(components["java"])
            pom {
                name = "orz-mq-spring-boot-starter"
                description = "orz-api mq spring boot starter"
                url = "https://github.com/orz-api/orz-mq-spring-boot-starter"
                licenses {
                    license {
                        name = "MIT License"
                        url = "https://opensource.org/license/mit/"
                    }
                }
                developers {
                    developer {
                        id = "reset7523"
                        name = "reset7523"
                        email = "reset7523@gmail.com"
                    }
                }
                scm {
                    connection = "scm:git:git://github.com:orz-api/orz-mq-spring-boot-starter.git"
                    developerConnection = "scm:git:git://github.com:orz-api/orz-mq-spring-boot-starter.git"
                    url = "https://github.com/orz-api/orz-mq-spring-boot-starter"
                }
            }
        }
    }
    repositories {
        maven {
            url = uri(
                if (version.toString().endsWith("SNAPSHOT")) "https://central.sonatype.com/repository/maven-snapshots/"
                else "https://ossrh-staging-api.central.sonatype.com/service/local/staging/deploy/maven2/"
            )
            credentials {
                val orzSonatypeUsername: String by project
                val orzSonatypePassword: String by project
                username = orzSonatypeUsername
                password = orzSonatypePassword
            }
        }
    }
}

signing {
    sign(publishing.publications["mavenJava"])
}

protobuf {
    protoc {
        artifact = "com.google.protobuf:protoc:3.25.4"
    }
}
