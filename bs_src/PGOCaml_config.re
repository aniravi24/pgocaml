let dir =
  if (Sys.file_exists("/var/run/postgresql")) {
    "/var/run/postgresql";
  } else {
    Filename.get_temp_dir_name();
  };

let default_port = 5432;
let default_user = "postgres";
let default_password = "";
let default_unix_domain_socket_dir = ".";
let default_comment_src_loc = false;