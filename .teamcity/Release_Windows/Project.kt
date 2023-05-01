package Release_Windows

import Release_Windows.buildTypes.*
import jetbrains.buildServer.configs.kotlin.*
import jetbrains.buildServer.configs.kotlin.Project

object Project : Project({
    id("Release_Windows")
    name = "Windows"

    buildType(Release_Windows_Composite)

    params {
        text("cmake_c_compiler", "",
             readOnly = true,
             allowEmpty = false)
        text("cmake_cxx_compiler", "",
             readOnly = true,
             allowEmpty = false)
        text("cmake_generator", "Visual Studio 17 2022",
             readOnly = true,
             allowEmpty = false)
        text("os_name", "Windows Server 2019",
             readOnly = true,
             allowEmpty = false)
        text("env.MVN_PATH", "%teamcity.tool.maven.3.5.3%/bin/mvn",
             readOnly = true,
             allowEmpty = false)
    }

    subProject(Release_Windows_X8664Core2.Project)
    subProject(Release_Windows_X86Haswell.Project)
})
