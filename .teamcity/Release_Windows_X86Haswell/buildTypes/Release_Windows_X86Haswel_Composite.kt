package Release_Windows_X86Haswell.buildTypes

import jetbrains.buildServer.configs.kotlin.*

object Release_Windows_X86Haswel_Composite : BuildType({
    name = "Composite"

    type = BuildTypeSettings.Type.COMPOSITE

    vcs {
        showDependenciesChanges = true
    }

    dependencies {
        dependency(Release_Windows_X86Haswell_OpenJDK18) {
            snapshot {
            }

            artifacts {
                artifactRules = "**"
            }
        }
    }
})
