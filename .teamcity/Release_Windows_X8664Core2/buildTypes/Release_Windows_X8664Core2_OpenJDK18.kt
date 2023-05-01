package Release_Windows_X8664Core2.buildTypes

import jetbrains.buildServer.configs.kotlin.*

object Release_Windows_X8664Core2_OpenJDK18 : BuildType({
    templates(_Self.buildTypes.Build)
    name = "OpenJDK 1.8"

    dependencies {
        dependency(AbsoluteId("Quasardb_Artifacts_Release_Windows_X8664Core2")) {
            snapshot {
                onDependencyFailure = FailureAction.FAIL_TO_START
            }

            artifacts {
                artifactRules = """
                    *-windows-64bit-core2-c-api.zip!**/*=>qdb
                    *-windows-64bit-core2-utils.zip!**/*=>qdb
                    *-windows-64bit-core2-server.zip!**/*=>qdb
                """.trimIndent()
            }
        }
    }
})
