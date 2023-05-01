package Release_Windows_X86Haswell.buildTypes

import jetbrains.buildServer.configs.kotlin.*

object Release_Windows_X86Haswell_OpenJDK18 : BuildType({
    templates(_Self.buildTypes.Build)
    name = "OpenJDK 1.8"

    dependencies {
        dependency(AbsoluteId("Quasardb_Artifacts_Release_Windows_X86Haswell")) {
            snapshot {
                onDependencyFailure = FailureAction.FAIL_TO_START
            }

            artifacts {
                artifactRules = """
                    *-windows-32bit-c-api.zip!**/*=>qdb
                    *-windows-32bit-utils.zip!**/*=>qdb
                    *-windows-32bit-server.zip!**/*=>qdb
                """.trimIndent()
            }
        }
    }
})
