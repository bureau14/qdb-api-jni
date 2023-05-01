package Release_Linux

import Release_Linux.buildTypes.*
import jetbrains.buildServer.configs.kotlin.*
import jetbrains.buildServer.configs.kotlin.Project

object Project : Project({
    id("Release_Linux")
    name = "Linux"

    buildType(Release_Linux_Composite)

    params {
        text("cmake_generator", "Ninja",
             readOnly = true,
             allowEmpty = false)
        text("os_name", "Linux",
             readOnly = true,
             allowEmpty = false)
        text("cmake_cxx_compiler", "%system.g++.path%",
             readOnly = true,
             allowEmpty = false)
        text("cmake_c_compiler", "%system.gcc.path%",
             readOnly = true,
             allowEmpty = false)
        text("windows_target_arch", "",
             readOnly = true,
             allowEmpty = false)
    }

    subProject(Release_Linux_X8664Core2.Project)
})
