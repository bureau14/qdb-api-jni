package Release_Windows.buildTypes

import jetbrains.buildServer.configs.kotlin.*

object Release_Windows_Composite : BuildType({
    name = "Composite"

    type = BuildTypeSettings.Type.COMPOSITE

    vcs {
        showDependenciesChanges = true
    }

    dependencies {
        dependency(Release_Windows_X8664Core2.buildTypes.Release_Windows_X8664Core2_Composite) {
            snapshot {
            }

            artifacts {
                artifactRules = "**"
            }
        }
        dependency(Release_Windows_X86Haswell.buildTypes.Release_Windows_X86Haswel_Composite) {
            snapshot {
            }

            artifacts {
                artifactRules = "**"
            }
        }
    }
})
