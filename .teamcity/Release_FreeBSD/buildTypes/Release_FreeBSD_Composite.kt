package Release_FreeBSD.buildTypes

import jetbrains.buildServer.configs.kotlin.*

object Release_FreeBSD_Composite : BuildType({
    name = "Composite"

    type = BuildTypeSettings.Type.COMPOSITE

    vcs {
        root(AbsoluteId("QdbApiJni"))

        showDependenciesChanges = true
    }

    dependencies {
        dependency(Release_FreeBSD_X8664Haswell.buildTypes.Release_FreeBSD_X8664Haswell_Composite) {
            snapshot {
            }

            artifacts {
                artifactRules = "**"
            }
        }
    }
})
