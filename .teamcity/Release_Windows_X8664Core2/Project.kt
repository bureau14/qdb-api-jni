package Release_Windows_X8664Core2

import Release_Windows_X8664Core2.buildTypes.*
import jetbrains.buildServer.configs.kotlin.*
import jetbrains.buildServer.configs.kotlin.Project

object Project : Project({
    id("Release_Windows_X8664Core2")
    name = "x86_64 Core2"

    buildType(Release_Windows_X8664Core2_Composite)
    buildType(Release_Windows_X8664Core2_OpenJDK18)

    params {
        text("os_arch", "amd64",
             readOnly = true,
             allowEmpty = false)
        text("windows_target_arch", "win64",
             readOnly = true,
             allowEmpty = false)
        text("java_home", "%system.java-64.home%",
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
