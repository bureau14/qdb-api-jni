package Release_MacOS

import Release_MacOS.buildTypes.*
import jetbrains.buildServer.configs.kotlin.*
import jetbrains.buildServer.configs.kotlin.Project

object Project : Project({
    id("Release_MacOS")
    name = "macOS"

    buildType(Release_MacOS_Composite)

    params {
        text("cmake_generator", "Ninja",
             readOnly = true,
             allowEmpty = false)
        text("os_name", "Mac OS X",
             readOnly = true,
             allowEmpty = false)
        text("cmake_cxx_compiler", "%system.clang++.path%",
             readOnly = true,
             allowEmpty = false)
        text("cmake_c_compiler", "%system.clang.path%",
             readOnly = true,
             allowEmpty = false)
        text("windows_target_arch", "",
             readOnly = true,
             allowEmpty = false)
    }

    subProject(Release_MacOS_X8664Core2.Project)
})
