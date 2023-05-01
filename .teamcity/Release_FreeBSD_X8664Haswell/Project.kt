package Release_FreeBSD_X8664Haswell

import Release_FreeBSD_X8664Haswell.buildTypes.*
import jetbrains.buildServer.configs.kotlin.*
import jetbrains.buildServer.configs.kotlin.Project

object Project : Project({
    id("Release_FreeBSD_X8664Haswell")
    name = "x86_64 Haswell"

    buildType(Release_FreeBSD_X8664Haswell_Composite)
    buildType(Release_FreeBSD_X8664Core2_OpenJDK18)

    params {
        text("os_arch", "amd64",
             readOnly = true,
             allowEmpty = false)
        text("java_path", "%java_home%/bin/java",
             readOnly = true,
             allowEmpty = false)
    }
})
