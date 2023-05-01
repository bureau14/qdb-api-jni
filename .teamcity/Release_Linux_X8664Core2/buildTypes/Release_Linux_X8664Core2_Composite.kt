package Release_Linux_X8664Core2.buildTypes

import jetbrains.buildServer.configs.kotlin.*

object Release_Linux_X8664Core2_Composite : BuildType({
    name = "Composite"

    type = BuildTypeSettings.Type.COMPOSITE

    vcs {
        root(AbsoluteId("QdbApiJni"))

        showDependenciesChanges = true
    }

    dependencies {
        dependency(Release_Linux_X8664Core2_OpenJDK18) {
            snapshot {
            }

            artifacts {
                artifactRules = "**"
            }
        }
    }
})
