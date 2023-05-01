package _Self.buildTypes

import jetbrains.buildServer.configs.kotlin.*

object Composite : BuildType({
    name = "Composite"

    type = BuildTypeSettings.Type.COMPOSITE

    vcs {
        root(AbsoluteId("QdbApiJni"))

        showDependenciesChanges = true
    }

    dependencies {
        dependency(Release.buildTypes.Release_Composite) {
            snapshot {
            }

            artifacts {
                artifactRules = "**"
            }
        }
    }
})
