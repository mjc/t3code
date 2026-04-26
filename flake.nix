{
  description = "T3 Code — reproducible dev/test environment";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, flake-utils }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = import nixpkgs { inherit system; };
      in
      {
        devShells.default = pkgs.mkShell {
          packages = [
            pkgs.bun
            pkgs.nodejs_22
            pkgs.git
            pkgs.bash
            pkgs.coreutils
          ];

          # node:sqlite is used by apps/server/src/persistence/NodeSqliteClient.ts.
          # On Node.js 22 it remains experimental and requires the flag.
          # Node.js 24 enables it by default; the flag is harmless there.
          NODE_OPTIONS = "--experimental-sqlite";
        };
      });
}
