exports_files(["bazel-bin/tikv.jar"])

java_binary(
    name = "tikv",
    srcs =glob([
         "src/main/java/com/pingcap/tikv/*.java",
         "src/main/java/com/pingcap/tikv/codec/*.java",
         "src/main/java/com/pingcap/tikv/util/*.java",
         "src/main/java/com/pingcap/tikv/meta/*.java",
         "src/main/java/com/pingcap/tikv/policy/*.java",
         "src/main/java/com/pingcap/tikv/operation/*.java",
         "src/main/java/com/pingcap/tikv/type/*.java",
         "src/main/java/com/pingcap/tikv/catalog/*.java",
         "src/main/java/com/pingcap/tikv/exception/*.java",
         ]),
     main_class = "com.pingcap.tikv.Main",
     deps = [
          "@com_google_guava_guava//jar",
          "@com_google_errorprone_error_prone_annotations//jar",
          "@io_grpc_grpc_context//jar",
          "@com_fasterxml_jackson_core_jackson_annotations//jar",
          "@com_fasterxml_jackson_core_jackson_databind//jar",
          "@com_fasterxml_jackson_core_jackson_core//jar",
          "@io_grpc_grpc_core//jar",
          "@com_google_code_gson_gson//jar",
          "@io_grpc_grpc_protobuf_lite//jar",
          "@io_grpc_grpc_protobuf//jar",
          "@com_google_protobuf_protobuf_java//jar",
          "@io_grpc_grpc_stub//jar",
          "@org_apache_logging_log4j_log4j_api//jar",
          "@org_apache_logging_log4j_log4j_core//jar",
          "@com_google_code_findbugs_jsr305//jar",
          "//src/main/proto:java",
          ],
)

java_library(
    name = "lib",
    runtime_deps = [
          ":tikv_java_proto",
    ],
)
