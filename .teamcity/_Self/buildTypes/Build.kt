package _Self.buildTypes

import jetbrains.buildServer.configs.kotlin.*
import jetbrains.buildServer.configs.kotlin.buildFeatures.XmlReport
import jetbrains.buildServer.configs.kotlin.buildFeatures.perfmon
import jetbrains.buildServer.configs.kotlin.buildFeatures.xmlReport
import jetbrains.buildServer.configs.kotlin.buildSteps.exec
import jetbrains.buildServer.configs.kotlin.buildSteps.maven

object Build : Template({
    name = "Build template"

    artifactRules = """
        +:target/*.jar
        -:target/*-sources.jar
        +:target/*-javadoc.jar
    """.trimIndent()

    params {
        text("env.CMAKE_BUILD_TYPE", "%cmake_build_type%",
             readOnly = true,
             allowEmpty = false)
        text("env.CMAKE_GENERATOR", "%cmake_generator%",
             readOnly = true,
             allowEmpty = false)
        text("env.CMAKE_C_COMPILER", "%cmake_c_compiler%",
             readOnly = true,
             allowEmpty = false)
        text("env.CMAKE_CXX_COMPILER", "%cmake_cxx_compiler%",
             readOnly = true,
             allowEmpty = false)
        text("env.MVN_PATH", "%teamcity.tool.maven.3.5.3%/bin/mvn",
             readOnly = true,
             allowEmpty = false)
        text("env.JAVA_PATH", "%java_path%",
             readOnly = true,
             allowEmpty = false)
        text("env.JAVA_HOME", "%java_home%",
             readOnly = true,
             allowEmpty = false)
        text("env.WINDOWS_TARGET_ARCH", "%windows_target_arch%",
             readOnly = true,
             allowEmpty = false)
    }

    vcs {
        root(AbsoluteId("QdbApiJni"))

        cleanCheckout = true
        showDependenciesChanges = true
    }

    steps {
        exec {
            name = "Build"
            workingDir = "scripts/teamcity/"
            path = "bash"
            arguments = "10.build.sh"
        }
        exec {
            name = "Start services"
            path = "bash"
            arguments = "scripts/tests/setup/start-services.sh"
            param("script.content", "./start-services")
        }
        exec {
            name = "Test"
            workingDir = "scripts/teamcity/"
            path = "bash"
            arguments = "20.test.sh"
        }
        exec {
            name = "Example"
            workingDir = "scripts/teamcity/"
            path = "bash"
            arguments = "30.example.sh"
        }
        exec {
            name = "Stop services"
            executionMode = BuildStep.ExecutionMode.ALWAYS
            path = "bash"
            arguments = "scripts/tests/setup/stop-services.sh"
            param("script.content", "./stop-services")
        }
    }

    failureConditions {
        executionTimeoutMin = 60
    }

    features {
        perfmon {
            id = "perfmon"
        }
        xmlReport {
            reportType = XmlReport.XmlReportType.SUREFIRE
            rules = "target/surefire-reports/TEST-*.xml"
            verbose = true
        }
    }

    requirements {
        equals("teamcity.agent.jvm.os.name", "%os_name%")
        equals("teamcity.agent.jvm.os.arch", "%os_arch%")
        startsWith("teamcity.agent.name", "default")
    }
})
