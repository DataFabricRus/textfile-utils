rootProject.name = "text-file-utils"

pluginManagement {
    plugins {
        plugins {
            val kotlinVersion: String by settings
            val dokkaVersion: String by settings

            kotlin("jvm") version kotlinVersion
            id("org.jetbrains.dokka") version dokkaVersion
        }
    }
}


