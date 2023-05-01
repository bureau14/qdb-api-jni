package Release.buildTypes

import jetbrains.buildServer.configs.kotlin.*

object Release_Composite : BuildType({
    name = "Composite"

    type = BuildTypeSettings.Type.COMPOSITE

    vcs {
        root(AbsoluteId("QdbApiJni"))

        showDependenciesChanges = true
    }

    dependencies {
        dependency(Release_FreeBSD.buildTypes.Release_FreeBSD_Composite) {
            snapshot {
            }

            artifacts {
                artifactRules = "**"
            }
        }
        dependency(Release_Linux.buildTypes.Release_Linux_Composite) {
            snapshot {
            }

            artifacts {
                artifactRules = "**"
            }
        }
        dependency(Release_MacOS.buildTypes.Release_MacOS_Composite) {
            snapshot {
            }

            artifacts {
                artifactRules = "**"
            }
        }
        dependency(Release_Windows.buildTypes.Release_Windows_Composite) {
            snapshot {
            }

            artifacts {
                artifactRules = "**"
            }
        }
    }
})
