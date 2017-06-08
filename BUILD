load(
    "@com_github_zhexuany_bazel_shade//:java_shade.bzl",
    "java_shade"
)
package(default_visibility = ["//visibility:public"])

filegroup(
    name = "srcs",
    srcs = glob(["**"]) + [
        "//src/main/java/com/pingcap/tikv:srcs",
        "//src/main/resources:srcs",
        "//src/test/java/com/pingcap/tikv:srcs",
    ],
)

java_binary(
    name = "tikv-java-client",
    main_class = "com.pingcap.tikv.Main",
    runtime_deps = [
        "//src/main/java/com/pingcap/tikv:tikv-java-client-lib",
        ":shaded_scalding",
    ],
)
java_shade(
    name = "shaded_args",
    input_jar = "@io_netty_netty_codec_socks//jar",
    rules = "shading_rule"
)

java_import(
    name = "shaded_scalding",
    jars = ["shaded_args.jar"]
)