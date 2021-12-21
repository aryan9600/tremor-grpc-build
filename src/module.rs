use proc_macro2::{Literal, Punct, TokenStream};
use quote::quote;

#[derive(Debug)]
pub struct Module {
    name: String,
    include_package: Option<String>,
    children: Vec<Module>
}

impl Module {
    pub fn new(arr: &[String], index: usize) -> Option<Self> {
        if index >= arr.len() {
            return None;
        }

        let child_node_opt = Module::new(arr, index + 1);
        let mut node = Module {
            name: arr[index].clone(),
            include_package: None,
            children: vec![],
        };
        if index == arr.len() - 1 {
            node.include_package = Some(arr.join("."));
        }

        if let Some(child_node) = child_node_opt {
            node.children.push(child_node);
        }

        Some(node)
    }

    pub fn add(&mut self, arr: &[String], index: usize) {
        if index >= arr.len() {
            return;
        }

        let mut exists = false;
        for child in &mut self.children {
            if arr[index] == child.name {
                exists = true;
                child.add(arr, index + 1);
                break;
            }
        }

        if !exists {
            let mut node = Module {
                name: arr[index].clone(),
                include_package: None,
                children: vec![],
            };
            if index == arr.len() - 1 {
                node.include_package = Some(arr.join("."));
            }
            node.add(arr, index + 1);
            self.children.push(node);
        }
    }

    pub fn generate(&self, tonic_modules: &mut Vec<TokenStream>) {
        let module_ident = quote::format_ident!("{}", self.name);
        let opening_bracket = Punct::new('{', proc_macro2::Spacing::Alone);
        tonic_modules.push(quote! {
            pub mod #module_ident #opening_bracket
        });
        if let Some(package) = &self.include_package {
            let include_file = Literal::string(package);
            tonic_modules.push(quote! {
                tonic::include_proto!(#include_file);
            });
        }
        for child in &self.children {
            child.generate(tonic_modules);
        }
        let closing_bracket = Punct::new('}', proc_macro2::Spacing::Alone);
        tonic_modules.push(quote! {
            #closing_bracket
        })
    }
}
