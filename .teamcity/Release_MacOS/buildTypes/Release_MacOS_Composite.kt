package Release_MacOS.buildTypes

import jetbrains.buildServer.configs.kotlin.*

object Release_MacOS_Composite : BuildType({
    name = "Composite"

    type = BuildTypeSettings.Type.COMPOSITE

    vcs {
        showDependenciesChanges = true
    }

    dependencies {
        dependency(Release_MacOS_X8664Core2.buildTypes.Release_MacOS_X8664Core2_Composite) {
            snapshot {
            }

            artifacts {
                artifactRules = "**"
            }
        }
    }
})
