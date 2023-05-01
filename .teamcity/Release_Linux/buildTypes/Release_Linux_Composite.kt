package Release_Linux.buildTypes

import jetbrains.buildServer.configs.kotlin.*

object Release_Linux_Composite : BuildType({
    name = "Composite"

    type = BuildTypeSettings.Type.COMPOSITE

    vcs {
        root(AbsoluteId("QdbApiJni"))

        showDependenciesChanges = true
    }

    dependencies {
        dependency(Release_Linux_X8664Core2.buildTypes.Release_Linux_X8664Core2_Composite) {
            snapshot {
            }

            artifacts {
                artifactRules = "**"
            }
        }
    }
})
