import java.security.MessageDigest

plugins {
    kotlin("jvm")
    id("maven-publish")
    id("org.jetbrains.dokka")
    signing
}

group = "io.github.datafabricrus"
version = "1.8"

repositories {
    mavenCentral()
    maven(url = "https://jitpack.io")
}

dependencies {
    val junitVersion: String by project
    val kotlinCoroutinesVersion: String by project
    val resourceIteratorVersion: String by project

    implementation("io.github.datafabricrus:resource-iterator-jvm:$resourceIteratorVersion")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:$kotlinCoroutinesVersion")
    testImplementation("org.junit.jupiter:junit-jupiter:$junitVersion")
}

publishing {
    publications {
        create<MavenPublication>("maven") {
            groupId = project.group.toString()
            artifactId = project.name
            version = project.version.toString()

            from(components["java"])

            pom {
                name.set("${project.group}:${project.name}")
                description.set("Text-file Utils")
                url.set("https://github.com/DataFabricRus/textfile-utils")
                licenses {
                    license {
                        name.set("The Apache License, Version 2.0")
                        url.set("http://www.apache.org/licenses/LICENSE-2.0.txt")
                    }
                }
                scm {
                    connection.set("scm:git:git://github.com/DataFabricRus/textfile-utils.git")
                    developerConnection.set("scm:git:ssh://github.com/DataFabricRus/textfile-utils.git")
                    url.set("https://github.com/DataFabricRus/textfile-utils")
                }
                developers {
                    developer {
                        id.set("sszuev")
                        name.set("Sergei Zuev")
                        email.set("sss.zuev@gmail.com")
                    }
                }
            }
        }
    }
}

tasks.register<Jar>("javadocJar") {
    dependsOn("dokkaHtml")
    archiveClassifier.set("javadoc")
    from(buildDir.resolve("dokka/html"))
}

java {
    withSourcesJar()
    withJavadocJar()
}

signing {
    sign(publishing.publications)
}

tasks.test {
    useJUnitPlatform()
}

tasks.named("publishToMavenLocal") {
    doLast {
        println("================================================")
        println("Generate MD5 files")
        println("================================================")
        val mavenLocalDir = file(repositories.mavenLocal().url)
        val artifactPathAsString = project.group.toString().replace('.', '/') + "/${project.name}/${version}"
        val artifactFile = mavenLocalDir.resolve(artifactPathAsString)
        val files = artifactFile.walkTopDown()
            .filter {
                it.isFile && (it.extension == "jar" || it.extension == "pom" || it.extension == "module")
            }
        files.forEach { file ->
            val md5 = MessageDigest.getInstance("MD5")
                .digest(file.readBytes()).joinToString("") { "%02x".format(it) }
            val sha1 = MessageDigest.getInstance("SHA-1")
                .digest(file.readBytes()).joinToString("") { "%02x".format(it) }

            file.resolveSibling("${file.name}.md5").writeText(md5)
            file.resolveSibling("${file.name}.sha1").writeText(sha1)
        }
    }
}

kotlin {
    jvmToolchain(11)
}