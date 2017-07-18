maven_jar(
    name = "com_fasterxml_jackson_core_jackson_annotations",
    artifact = "com.fasterxml.jackson.core:jackson-annotations:2.6.6",
)

maven_jar(
    name = "com_fasterxml_jackson_core_jackson_databind",
    artifact = "com.fasterxml.jackson.core:jackson-databind:2.6.6",
)

maven_jar(
    name = "com_fasterxml_jackson_core_jackson_core",
    artifact = "com.fasterxml.jackson.core:jackson-core:2.6.6",
)

maven_jar(
    name = "org_apache_logging_log4j_log4j_api",
    artifact = "org.apache.logging.log4j:log4j-api:2.8.1",
)

maven_jar(
    name = "org_apache_logging_log4j_log4j_core",
    artifact = "org.apache.logging.log4j:log4j-core:2.8.1",
)

maven_jar(
    name = "com_google_errorprone_error_prone_annotations",
    artifact = "com.google.errorprone:error_prone_annotations:2.0.11",
)

maven_jar(
    name = "com_google_instrumentation_instrumentation_api",
    artifact = "com.google.instrumentation:instrumentation-api:0.4.2",
)

maven_jar(
    name = "junit_junit",
    artifact = "junit:junit:4.12",
)

maven_jar(
    name = "org_hamcrest_hamcrest_core",
    artifact = "org.hamcrest:hamcrest-core:1.3",
)

maven_jar(
    name = "org_powermock_powermock_module_junit4",
    artifact = "org.powermock:powermock-module-junit4:1.6.6",
)

maven_jar(
    name = "org_powermock_powermock_module_junit4_common",
    artifact = "org.powermock:powermock-module-junit4-common:1.6.6",
)

maven_jar(
    name = "org_powermock_powermock_core",
    artifact = "org.powermock:powermock-core:1.6.6",
)

maven_jar(
    name = "org_javassist_javassist",
    artifact = "org.javassist:javassist:3.21.0-GA",
)

maven_jar(
    name = "org_powermock_powermock_reflect",
    artifact = "org.powermock:powermock-reflect:1.6.6",
)

maven_jar(
    name = "org_powermock_powermock_api_mockito",
    artifact = "org.powermock:powermock-api-mockito:1.6.6",
)

maven_jar(
    name = "org_mockito_mockito_core",
    artifact = "org.mockito:mockito-core:1.10.19",
)

maven_jar(
    name = "org_objenesis_objenesis",
    artifact = "org.objenesis:objenesis:2.1",
)

maven_jar(
    name = "org_powermock_powermock_api_mockito_common",
    artifact = "org.powermock:powermock-api-mockito-common:1.6.6",
)

maven_jar(
    name = "org_powermock_powermock_api_support",
    artifact = "org.powermock:powermock-api-support:1.6.6",
)

maven_jar(
	name = "io_netty_netty_codec_socks",
	artifact = "io.netty:netty-codec-socks:4.1.8.Final",
)

maven_jar(
   name = "net_sf_trove4j_trove4j",
   artifact = "net.sf.trove4j:trove4j:3.0.1",
)

maven_jar(
   name = "com_pingcap_tipb",
   artifact = "com.pingcap.tipb:tipb:0.1.0",
)

maven_jar(
   name = "com_pingcap_tikv",
   artifact = "com.pingcap.tikv:kvproto:0.1.0",
)

git_repository(
    name = "org_pubref_rules_protobuf",
    remote = "https://github.com/zhexuany/rules_protobuf",
    commit = "7d505c7",
)

load("@org_pubref_rules_protobuf//java:rules.bzl", "java_proto_repositories")
java_proto_repositories()

bazel_shade_version = "master"
http_archive(
             name = "com_github_zhexuany_bazel_shade",
             url = "https://github.com/zhexuany/bazel_shade_plugin/archive/%s.zip"%bazel_shade_version,
             type = "zip",
             strip_prefix= "bazel_shade_plugin-%s"%bazel_shade_version
)
load(
    "@com_github_zhexuany_bazel_shade//:java_shade.bzl",
    "java_shade_repositories",
    "java_shade"
)
java_shade_repositories()
