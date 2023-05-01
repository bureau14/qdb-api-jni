package Release_FreeBSD_X8664Haswell.buildTypes

import jetbrains.buildServer.configs.kotlin.*

object Release_FreeBSD_X8664Core2_OpenJDK18 : BuildType({
    templates(_Self.buildTypes.Build)
    name = "OpenJDK 1.8"

    params {
        text("java_home", "%env.JDK_1_8_x64%",
             readOnly = true,
             allowEmpty = false)
    }

    dependencies {
        dependency(AbsoluteId("Quasardb_Artifacts_Release_FreeBSD_X8664Haswell_Default")) {
            snapshot {
                onDependencyFailure = FailureAction.FAIL_TO_START
            }

            artifacts {
                artifactRules = """
                    *-freebsd-64bit-c-api.tar.gz!**/*=>qdb
                    *-freebsd-64bit-utils.tar.gz!**/*=>qdb
                    *-freebsd-64bit-server.tar.gz!**/*=>qdb
                """.trimIndent()
            }
        }
    }
})
