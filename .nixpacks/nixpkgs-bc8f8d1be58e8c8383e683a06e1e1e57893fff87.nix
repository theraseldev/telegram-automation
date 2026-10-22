{ }:

let pkgs = import (fetchTarball "https://github.com/NixOS/nixpkgs/archive/bc8f8d1be58e8c8383e683a06e1e1e57893fff87.tar.gz") { overlays = [  ]; };
in with pkgs;
  let
    APPEND_LIBRARY_PATH = "${lib.makeLibraryPath [ stdenv.cc.cc.lib zlib ] }";
    myLibraries = writeText "libraries" ''
      export LD_LIBRARY_PATH="${APPEND_LIBRARY_PATH}:$LD_LIBRARY_PATH"
      
    '';
  in
    buildEnv {
      name = "bc8f8d1be58e8c8383e683a06e1e1e57893fff87-env";
      paths = [
        (runCommand "bc8f8d1be58e8c8383e683a06e1e1e57893fff87-env" { } ''
          mkdir -p $out/etc/profile.d
          cp ${myLibraries} $out/etc/profile.d/bc8f8d1be58e8c8383e683a06e1e1e57893fff87-env.sh
        '')
        ffmpeg-headless gcc postgresql_16.dev python311
      ];
    }
