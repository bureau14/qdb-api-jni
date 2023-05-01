package Release_FreeBSD_X8664Haswell.buildTypes

import jetbrains.buildServer.configs.kotlin.*

object Release_FreeBSD_X8664Haswell_Composite : BuildType({
    name = "Composite"

    type = BuildTypeSettings.Type.COMPOSITE

    vcs {
        root(AbsoluteId("QdbApiJni"))

        showDependenciesChanges = true
    }

    dependencies {
        dependency(Release_FreeBSD_X8664Core2_OpenJDK18) {
            snapshot {
            }

            artifacts {
                artifactRules = "**"
            }
        }
    }
})
