{
    description = "Rust Download Manager - CLI";

    inputs = {
        nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";
        crane.url = "github:ipetkov/crane";
        flake-utils.url = "github:numtide/flake-utils";
        rust-overlay = {
            url = "github:oxalica/rust-overlay";
            inputs.nixpkgs.follows = "nixpkgs";
        };
    };

    outputs =
        {
            self,
            nixpkgs,
            crane,
            flake-utils,
            rust-overlay,
            ...
        }:
        flake-utils.lib.eachDefaultSystem (
            system:
            let
                pkgs = import nixpkgs {
                    inherit system;
                    overlays = [ (import rust-overlay) ];
                };
                mingwPkgs = import nixpkgs {
                    overlays = [ (import rust-overlay) ];
                    localSystem = system;
                    crossSystem = {
                        config = "x86_64-w64-mingw32";
                        libc = "msvcrt";
                    };
                };

                craneLib = crane.mkLib pkgs;
                mingwCraneLib = (crane.mkLib mingwPkgs).overrideToolchain (
                    p:
                    p.rust-bin.stable.latest.default.override {
                        targets = [ "x86_64-pc-windows-gnu" ];
                    }
                );

                src = craneLib.cleanCargoSource ./.;

                commonArgs = {
                    inherit src;
                    strictDeps = true;
                };

                cargoArtifacts = craneLib.buildDepsOnly commonArgs;
                mingwCargoArtifacts = mingwCraneLib.buildDepsOnly (
                    commonArgs
                    // {
                        CARGO_BUILD_TARGET = "x86_64-pc-windows-gnu";
                    }
                );

                default = craneLib.buildPackage (
                    commonArgs
                    // {
                        inherit cargoArtifacts;
                    }
                );

                windows = mingwCraneLib.buildPackage (
                    commonArgs
                    // {
                        cargoArtifacts = mingwCargoArtifacts;
                        CARGO_BUILD_TARGET = "x86_64-pc-windows-gnu";
                        CARGO_TARGET_X86_64_PC_WINDOWS_GNU_LINKER =
                            "${mingwPkgs.stdenv.cc.targetPrefix}gcc";
                    }
                );
            in
            {
                checks = {
                    crate-clippy = craneLib.cargoClippy (
                        commonArgs
                        // {
                            inherit cargoArtifacts;
                            cargoClippyExtraArgs = "--all-targets -- --deny warnings";
                        }
                    );
                };

                packages = {
                    inherit default;
                    inherit windows;
                };

                devShells.default = craneLib.devShell {
                    checks = self.checks.${system};
                    packages = [];
                };
            }
        );
}
