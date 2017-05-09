filegroup(
    name = "srcs",
    srcs = glob(["**"]),
    visibility = ["//src:__pkg__"],
)

java_test(
    name = "tikv_test",
    srcs = glob(["*.java"]),
    test_class = "com.google.devtools.common.options.AllTests",
    deps = [
        "//src/main/java/com/pingcap/tikv",
    ],
)