with import <nixpkgs> {};
stdenv.mkDerivation rec {
  name = "env";
  env = buildEnv { name = name; paths = buildInputs; };
  buildInputs = [
    python27Packages.kazoo
    python27Packages.pytest
    python27Packages.ansible
    python27Packages.ipython
  ];
}
