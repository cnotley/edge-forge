fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rustc-check-cfg=cfg(fuzzing)");

    if let Ok(protoc) = protoc_bin_vendored::protoc_bin_path() {
        if let Ok(include) = protoc_bin_vendored::include_path() {
            std::env::set_var("PROTOC", &protoc);
            std::env::set_var("PROTOC_INCLUDE", &include);
            println!("cargo:rustc-env=PROTOC={}", protoc.display());
            println!("cargo:rustc-env=PROTOC_INCLUDE={}", include.display());
        }
    }
}
