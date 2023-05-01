package Release_FreeBSD

import Release_FreeBSD.buildTypes.*
import jetbrains.buildServer.configs.kotlin.*
import jetbrains.buildServer.configs.kotlin.Project

object Project : Project({
    id("Release_FreeBSD")
    name = "FreeBSD"

    buildType(Release_FreeBSD_Composite)

    params {
        text("cmake_generator", "Ninja", readOnly = true, allowEmpty = false)
        text("os_name", "FreeBSD", readOnly = true, allowEmpty = false)
        text("cmake_cxx_compiler", "%system.clang++.path%", readOnly = true, allowEmpty = false)
        text("cmake_c_compiler", "%system.clang.path%", readOnly = true, allowEmpty = false)
        text("windows_target_arch", "", readOnly = true, allowEmpty = false)
    }

    subProject(Release_FreeBSD_X8664Haswell.Project)
})
