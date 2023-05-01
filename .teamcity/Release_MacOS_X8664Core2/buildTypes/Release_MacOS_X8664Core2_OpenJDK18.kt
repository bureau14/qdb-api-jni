package Release_MacOS_X8664Core2.buildTypes

import jetbrains.buildServer.configs.kotlin.*

object Release_MacOS_X8664Core2_OpenJDK18 : BuildType({
    templates(_Self.buildTypes.Build)
    name = "OpenJDK 1.8"

    params {
        text("java_home", "%env.JDK_1_8_x64%",
             readOnly = true,
             allowEmpty = false)
    }

    dependencies {
        dependency(AbsoluteId("Quasardb_Artifacts_Release_MacOS_X8664Core2_Default")) {
            snapshot {
                onDependencyFailure = FailureAction.FAIL_TO_START
            }

            artifacts {
                artifactRules = """
                    *-darwin-64bit-core2-c-api.tar.gz!**/*=>qdb
                    *-darwin-64bit-core2-utils.tar.gz!**/*=>qdb
                    *-darwin-64bit-core2-server.tar.gz!**/*=>qdb
                """.trimIndent()
            }
        }
    }
})
