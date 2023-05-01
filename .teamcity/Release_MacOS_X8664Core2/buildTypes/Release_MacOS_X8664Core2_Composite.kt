package Release_MacOS_X8664Core2.buildTypes

import jetbrains.buildServer.configs.kotlin.*

object Release_MacOS_X8664Core2_Composite : BuildType({
    name = "Composite"

    type = BuildTypeSettings.Type.COMPOSITE

    vcs {
        showDependenciesChanges = true
    }

    dependencies {
        dependency(Release_MacOS_X8664Core2_OpenJDK18) {
            snapshot {
            }

            artifacts {
                artifactRules = "**"
            }
        }
    }
})
