module String: {
  include (module type of String);

  let starts_with: (string, string) => bool;
  let join: (string, list(string)) => string;
  let implode: list(char) => string;
  let fold_left: (('a, char) => 'a, 'a, string) => 'a;
  let init: (int, int => char) => string;
};

module Option: {
  let default: ('a, option('a)) => 'a;
  let get: option('a) => 'a;
  let map: ('a => 'b, option('a)) => option('b);
};

module List: {
  include (module type of List);

  let iteri: ((int, 'a) => unit, list('a)) => unit;
  let mapi: ((int, 'a) => 'b, list('a)) => list('b);
};
