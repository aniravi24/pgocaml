module String = {
  include String;

  let starts_with = (str, prefix) => {
    let len = length(prefix);
    if (length(str) < len) {
      false;
    } else {
      let rec aux = i =>
        if (i >= len) {
          true;
        } else if (unsafe_get(str, i) != unsafe_get(prefix, i)) {
          false;
        } else {
          aux(i + 1);
        };
      aux(0);
    };
  };

  let join = concat;

  let implode = xs => {
    let buf = Buffer.create(List.length(xs));
    List.iter(Buffer.add_char(buf), xs);
    Buffer.contents(buf);
  };

  let fold_left = (f, init, str) => {
    let len = length(str);
    let rec loop = (i, accum) =>
      if (i == len) {
        accum;
      } else {
        loop(i + 1, f(accum, str.[i]));
      };
    loop(0, init);
  };

  /* Only available in the standard library since OCaml 4.02 */
  let init = (n, f) => {
    let s = Bytes.create(n);
    for (i in 0 to n - 1) {
      Bytes.unsafe_set(s, i, f(i));
    };
    Bytes.to_string(s);
  };
};

module Option = {
  let default = v =>
    fun
    | Some(v) => v
    | None => v;

  let get =
    fun
    | Some(v) => v
    | None => invalid_arg("PGOCaml_aux.Option.get");

  let map = f =>
    fun
    | Some(v) => Some(f(v))
    | None => None;
};

module List = {
  include List;

  let iteri = (f, xs) => {
    let rec loop = i =>
      fun
      | [] => ()
      | [hd, ...tl] => {
          f(i, hd);
          loop(i + 1, tl);
        };
    loop(0, xs);
  };

  let mapi = (f, xs) => {
    let rec loop = i =>
      fun
      | [] => []
      | [hd, ...tl] => {
          let hd' = f(i, hd);
          [hd', ...loop(i + 1, tl)];
        };
    loop(0, xs);
  };
};
