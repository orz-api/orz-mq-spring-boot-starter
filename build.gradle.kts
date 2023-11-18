plugins {
    signing
    `java-library`
    `maven-publish`
    id("org.springframework.boot") version "3.1.4"
    id("io.spring.dependency-management") version "1.1.3"
}

group = "io.github.orz-api"
version = "0.0.1-SNAPSHOT"

java {
    sourceCompatibility = JavaVersion.VERSION_17
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
    mavenCentral()
    maven {
        url = uri("https://s01.oss.sonatype.org/content/repositories/snapshots/")
    }
}

dependencies {
    api("io.github.orz-api:orz-base-spring-boot-starter:0.0.1-SNAPSHOT")

    compileOnly("org.springframework.kafka:spring-kafka")
    testImplementation("org.springframework.kafka:spring-kafka")
    compileOnly("com.ctrip.framework.apollo:apollo-client:2.1.0")
    testImplementation("com.ctrip.framework.apollo:apollo-client:2.1.0")

    annotationProcessor("org.springframework.boot:spring-boot-configuration-processor")
    testImplementation("org.springframework.boot:spring-boot-starter-test")

    compileOnly("org.projectlombok:lombok")
    testCompileOnly("org.projectlombok:lombok")
    annotationProcessor("org.projectlombok:lombok")
    testAnnotationProcessor("org.projectlombok:lombok")
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
                    if (version.toString().endsWith("SNAPSHOT")) "https://s01.oss.sonatype.org/content/repositories/snapshots/"
                    else "https://s01.oss.sonatype.org/service/local/staging/deploy/maven2/"
            )
            credentials {
                val orzOssrhUsername: String by project
                val orzOssrhPassword: String by project
                username = orzOssrhUsername
                password = orzOssrhPassword
            }
        }
    }
}

signing {
    sign(publishing.publications["mavenJava"])
}
