package Release

import Release.buildTypes.*
import jetbrains.buildServer.configs.kotlin.*
import jetbrains.buildServer.configs.kotlin.Project

object Project : Project({
    id("Release")
    name = "Release"

    buildType(Release_Composite)

    params {
        param("cmake_build_type", "Release")
    }

    subProject(Release_FreeBSD.Project)
    subProject(Release_Windows.Project)
    subProject(Release_Linux.Project)
    subProject(Release_MacOS.Project)
})
