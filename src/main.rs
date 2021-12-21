use tremor_grpc_build::generate;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // let files = gen_file_descriptor(&[
    // "opentelemetry-proto/opentelemetry/proto/collector/logs/v1/logs_service.proto",
    // "opentelemetry-proto/opentelemetry/proto/collector/metrics/v1/metrics_service.proto",
    // "opentelemetry-proto/opentelemetry/proto/metrics/experimental/metrics_config_service.proto",
    // "opentelemetry-proto/opentelemetry/proto/collector/trace/v1/trace_service.proto",
    // "proto/helloworld/helloworld.proto",
    // "proto/routeguide/routeguide.proto"
    // ], &[
    // "proto/helloworld", "proto/routeguide"
    // ]).unwrap();
    // for file in files.file {
    // dbg!(file.package());
    // for service in &file.service {
    // for method in &service.method {
    // dbg!(method.name());
    // dbg!(method.input_type());
    // dbg!(method.output_type());
    // }
    // }
    // }
    // generate(&["proto/helloworld/helloworld.proto, proto/routeguide/routeguide.proto"], &["proto/helloworld", "proto/routeguide"], std::env::var_os("OUT_DIR").unwrap(), false, true).unwrap();
    generate(&[
        "opentelemetry-proto/opentelemetry/proto/collector/logs/v1/logs_service.proto",
        "opentelemetry-proto/opentelemetry/proto/collector/metrics/v1/metrics_service.proto",
        "opentelemetry-proto/opentelemetry/proto/metrics/experimental/metrics_config_service.proto",
        "opentelemetry-proto/opentelemetry/proto/collector/trace/v1/trace_service.proto",
    ], &[
    "opentelemetry-proto"
    ], "/home/sanskarjaiswal/gen-tonic-impls",false, true)?;
    Ok(())
}
