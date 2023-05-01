package Release_Windows_X86Haswell

import Release_Windows_X86Haswell.buildTypes.*
import jetbrains.buildServer.configs.kotlin.*
import jetbrains.buildServer.configs.kotlin.Project

object Project : Project({
    id("Release_Windows_X86Haswell")
    name = "x86 Haswell"

    buildType(Release_Windows_X86Haswell_OpenJDK18)
    buildType(Release_Windows_X86Haswel_Composite)

    params {
        text("os_arch", "amd64",
             readOnly = true,
             allowEmpty = false)
        text("windows_target_arch", "win32",
             readOnly = true,
             allowEmpty = false)
        text("java_home", "%system.java-32.home%",
             readOnly = true,
             allowEmpty = false)
        text("java_path", "%java_home%/bin/java",
             readOnly = true,
             allowEmpty = false)
    }
})
