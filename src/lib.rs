// Copyright 2021, Sanskar Jaiswal

pub mod client;
mod ident;
mod module;

use heck::SnakeCase;
use ident::{to_snake, to_upper_camel};
use itertools::Itertools;
use module::Module;
use proc_macro2::{Ident, TokenStream};
use prost::Message;
use prost_build::protoc;
use prost_types::{
    FileDescriptorProto, FileDescriptorSet, MethodDescriptorProto, ServiceDescriptorProto,
};
use quote::quote;
use std::{
    collections::HashMap,
    ffi::OsStr,
    fs,
    path::{Path, PathBuf},
    process::Command,
    str::FromStr,
};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

trait ServiceProtoInfo {
    fn client_ident(&self) -> Ident;
    fn server_ident(&self) -> Ident;
    fn server_mod_ident(&self) -> Ident;
    fn client_mod_ident(&self) -> Ident;
    fn trait_ident(&self) -> Ident;
}

pub trait MethodProtoInfo {
    fn name_ident(&self) -> Ident;
    fn request_message_ident(&self, package: &str) -> TokenStream;
    fn response_message_ident(&self, package: &str) -> TokenStream;
}

trait FileProtoInfo {
    fn package_ident(&self) -> Ident;
    fn mod_ident(&self) -> TokenStream;
}

impl FileProtoInfo for FileDescriptorProto {
    fn package_ident(&self) -> Ident {
        quote::format_ident!("{}", self.package())
    }

    fn mod_ident(&self) -> TokenStream {
        let mod_string = self.package().replace(".", "::");
        TokenStream::from_str(&mod_string).unwrap()
    }
}

impl ServiceProtoInfo for ServiceDescriptorProto {
    fn client_mod_ident(&self) -> Ident {
        quote::format_ident!("{}", format!("{}Client", self.name()).to_snake_case())
    }

    fn server_mod_ident(&self) -> Ident {
        quote::format_ident!("{}", format!("{}Server", self.name()).to_snake_case())
    }

    fn client_ident(&self) -> Ident {
        quote::format_ident!("{}", format!("{}Client", self.name()))
    }

    fn server_ident(&self) -> Ident {
        quote::format_ident!("{}", format!("{}Server", self.name()))
    }

    fn trait_ident(&self) -> Ident {
        quote::format_ident!("{}", self.name())
    }
}

impl MethodProtoInfo for MethodDescriptorProto {
    fn name_ident(&self) -> Ident {
        quote::format_ident!("{}", self.name().to_snake_case())
    }

    fn request_message_ident(&self, _package: &str) -> TokenStream {
        TokenStream::from_str(&get_message_type(self.input_type())).unwrap()
    }

    fn response_message_ident(&self, _package: &str) -> TokenStream {
        TokenStream::from_str(&get_message_type(self.output_type())).unwrap()
    }
}

/// Generate the custom client and server code.
pub fn generate(
    protos: &[impl AsRef<Path>],
    includes: &[impl AsRef<Path>],
    out_dir: impl Into<PathBuf>,
    _server: bool,
    client: bool,
) -> Result<()> {
    let buf = gen_file_descriptor(protos, includes)?;
    let descriptor_set: FileDescriptorSet = FileDescriptorSet::decode(&*buf)?;
    if client {
        let output_file = out_dir.into().join("grpc_client.rs");
        fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(output_file.clone())?;
        let mut buf = String::new();
        let tonic_modules = format!("{}", generate_tonic_modules(descriptor_set.file.clone()));
        buf.push_str(&tonic_modules);
        let grpc_client_code = client::generate_grpc_client_impl(descriptor_set.file);
        buf.push_str(&grpc_client_code);
        fs::write(output_file.clone(), buf)?;
        apply_rustfmt(output_file, "2018")?;
    }
    Ok(())
}

fn generate_tonic_modules(files: Vec<FileDescriptorProto>) -> TokenStream {
    let mut parents: HashMap<String, Module> = HashMap::new();
    for file in files {
        let package = file.package();
        let split = package
            .split('.')
            .map(|x| x.to_string())
            .collect::<Vec<String>>();

        if let Some(parent_module) = parents.get_mut(&split[0]) {
            parent_module.add(&split, 1);
        } else {
            let module = Module::new(&split, 0).expect("Could not parse protobuf packages.");
            parents.insert(split[0].clone(), module);
        }
    }
    let mut modules = vec![];
    for module in parents.values() {
        let mut each_module = vec![];
        module.generate(&mut each_module);
        let mut module_block = TokenStream::new();
        module_block.extend(each_module);
        modules.push(module_block);
    }
    let mut code = TokenStream::new();
    code.extend(modules);
    quote! {
        #code
    }
}

fn apply_rustfmt(file: impl AsRef<OsStr>, edition: &str) -> Result<()> {
    let mut cmd = Command::new("rustfmt");
    cmd.arg("--edition").arg(edition).arg(file);
    cmd.status()?;
    Ok(())
}

// Generate a file descriptor for all protobufs provided.
fn gen_file_descriptor(
    protos: &[impl AsRef<Path>],
    includes: &[impl AsRef<Path>],
) -> Result<Vec<u8>> {
    let tmp = tempfile::Builder::new().prefix("prost-build").tempdir()?;
    let file_descriptor_set_path = tmp.path().join("prost-descriptor-set");

    let mut cmd = Command::new(protoc());
    cmd.arg("--include_imports")
        .arg("--include_source_info")
        .arg("-o")
        .arg(&file_descriptor_set_path);
    for include in includes {
        cmd.arg("-I").arg(include.as_ref());
    }

    for proto in protos {
        cmd.arg(proto.as_ref());
    }

    cmd.status()?;

    let buf = fs::read(file_descriptor_set_path)?;
    tmp.close()?;
    Ok(buf)
}

// Get the Rust type for a message.
pub fn get_message_type(pb_ident: &str) -> String {
    let mut ident_path = pb_ident[1..].split('.');
    let ident_type = ident_path.next_back().unwrap();
    let ident_path = ident_path.peekable();

    ident_path
        .map(to_snake)
        .chain(std::iter::once(to_upper_camel(ident_type)))
        .join("::")
}
