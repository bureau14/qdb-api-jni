package Release_MacOS_X8664Core2

import Release_MacOS_X8664Core2.buildTypes.*
import jetbrains.buildServer.configs.kotlin.*
import jetbrains.buildServer.configs.kotlin.Project

object Project : Project({
    id("Release_MacOS_X8664Core2")
    name = "x86_64 Core2"

    buildType(Release_MacOS_X8664Core2_Composite)
    buildType(Release_MacOS_X8664Core2_OpenJDK18)

    params {
        text("os_arch", "x86_64",
             readOnly = true,
             allowEmpty = false)
        text("java_path", "%java_home%/bin/java",
             readOnly = true,
             allowEmpty = false)
        text("env.QDB_CPU_ARCHITECTURE_CORE2", "ON",
             readOnly = true,
             allowEmpty = false)
    }
})
