/* PG'OCaml - type safe interface to PostgreSQL.
 * Copyright (C) 2005-2016 Richard Jones and other authors.
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Library General Public
 * License as published by the Free Software Foundation; either
 * version 2 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Library General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this library; see the file COPYING.  If not, write to
 * the Free Software Foundation, Inc., 59 Temple Place - Suite 330,
 * Boston, MA 02111-1307, USA.
 */

open PGOCaml_aux;
open Printf;

open Migrate_parsetree;
open Migrate_parsetree.Ast_407.Ast_mapper;
open Migrate_parsetree.Ast_407.Ast_helper;
open Migrate_parsetree.Ast_407.Asttypes;
open Migrate_parsetree.Ast_407.Parsetree;
open Migrate_parsetree.Ast_407.Longident;

let nullable_name = "nullable";
let unravel_name = "unravel";
let typname_name = "typname";

/* We need a database connection while compiling.  If people use the
 * override flags like "database=foo", then we may connect to several
 * databases at once.  Keep track of that here.  Note that in the normal
 * case we have just one database handle, opened to the default
 * database (as controlled by environment variables such as $PGHOST,
 * $PGDATABASE, etc.)
 */
type key = PGOCaml.connection_desc;

let connections: Hashtbl.t(key, PGOCaml.t(unit)) = Hashtbl.create(16);

%if
ocaml_version < (4, 8, 0);
let exp_of_string = (~loc as _, x) => {
  let lexer = Lexing.from_string(x);
  Migrate_parsetree.Parse.expression(
    Migrate_parsetree.Versions.ocaml_407,
    lexer,
  );
};
[%%else];
let exp_of_string = (~loc, x) => {
  let lexer = {
    let acc = Lexing.from_string(~with_positions=false, x);
    acc.Lexing.lex_start_p = loc.Warnings.loc_start;
    acc.Lexing.lex_curr_p = loc.Warnings.loc_end;
    acc;
  };

  Migrate_parsetree.Parse.expression(
    Migrate_parsetree.Versions.ocaml_407,
    lexer,
  );
};
[%%endif];

/** [get_connection key] Find the database connection specified by [key],
  *  otherwise attempt to create a new one from [key] and return that (or an
  *  error).
  */

let get_connection = (~loc, key) =>
  switch (Hashtbl.find_opt(connections, key)) {
  | Some(connection) => Rresult.(Ok(connection))
  | None =>
    /* Create a new connection. */
    try (
      {
        let dbh = PGOCaml.connect(~desc=key, ());
        /* Prepare the nullable test - see result conversions below. */
        let nullable_query = "select attnotnull from pg_attribute where attrelid = $1 and attnum = $2";
        PGOCaml.prepare(dbh, ~query=nullable_query, ~name=nullable_name, ());

        /* Prepare the unravel test. */
        let unravel_query = "select typname, typtype, typbasetype from pg_type where oid = $1";
        PGOCaml.prepare(dbh, ~query=unravel_query, ~name=unravel_name, ());

        /* Prepare the type name query. */
        let typname_query = "select typname from pg_type where oid = $1";
        PGOCaml.prepare(dbh, ~query=typname_query, ~name=typname_name, ());

        Hashtbl.add(connections, key, dbh);
        Rresult.Ok(dbh);
      }
    ) {
    | err =>
      [@implicit_arity]
      Error(
        "Could not make the connection "
        ++ PGOCaml.connection_desc_to_string(key)
        ++ ", error: "
        ++ Printexc.to_string(err),
        loc,
      )
    }
  };

/* Wrapper around [PGOCaml.name_of_type].
 */
let name_of_type_wrapper = (dbh, oid) =>
  try (Some(PGOCaml.name_of_type(oid))) {
  | PGOCaml.Error(_) =>
    let params = [Some(PGOCaml.string_of_oid(oid))];
    let rows = PGOCaml.execute(dbh, ~name=typname_name, ~params, ());
    switch (rows) {
    | [[Some("citext")]] => Some("string")
    | [[Some("hstore")]] => Some("hstore")
    | _ => None
    };
  };

/* By using CREATE DOMAIN, the user may define types which are essentially aliases
  * for existing types.  If the original type is not recognised by PG'OCaml, this
  * functions recurses through the pg_type table to see if it happens to be an alias
  * for a type which we do know how to handle.
 */
let unravel_type =
    (dbh, ~load_custom_from=?, ~colnam=?, ~argnam=?, ~typnam=?, orig_type) => {
  let get_custom = typnam =>
    switch (
      PGOCaml.find_custom_typconvs(
        ~typnam?,
        ~lookin=?load_custom_from,
        ~colnam?,
        ~argnam?,
        (),
      )
    ) {
    | Ok(convs) => (Option.default("FIXME", typnam), convs)
    | Error(exc) => failwith(exc)
    };

  let rec unravel_type_aux = ft => {
    let rv = {
      let rv =
        switch (typnam) {
        | Some(x) => (Some(x), None)
        | None => (name_of_type_wrapper(dbh, ft), None)
        };

      switch (
        PGOCaml.find_custom_typconvs(
          ~typnam=?fst(rv),
          ~lookin=?load_custom_from,
          ~colnam?,
          ~argnam?,
          (),
        )
      ) {
      | Ok(x) => (fst(rv), x)
      | Error(err) => failwith(err)
      };
    };

    switch (rv) {
    | (None, _) =>
      let params = [Some(PGOCaml.string_of_oid(ft))];
      let rows = PGOCaml.execute(dbh, ~name=unravel_name, ~params, ());
      switch (rows) {
      | [[typnam, ..._]]
          when
            Rresult.R.is_ok(
              PGOCaml.find_custom_typconvs(
                ~typnam?,
                ~lookin=?load_custom_from,
                ~colnam?,
                ~argnam?,
                (),
              ),
            ) =>
        get_custom(typnam)
      | [[Some(_), Some(typtype), _]] when typtype == "e" => (
          "string",
          None,
        )
      | [[Some(_), Some(typtype), Some(typbasetype)]] when typtype == "d" =>
        unravel_type_aux(PGOCaml.oid_of_string(typbasetype))
      | _ => failwith("Impossible")
      };
    | (typnam, coders) => (Option.default("FIXME", typnam), coders)
    };
  };
  unravel_type_aux(orig_type);
};

/* Return the list of numbers a <= i < b. */
let rec range = (a, b) =>
  if (a < b) {
    [a, ...range(a + 1, b)];
  } else {
    [];
  };

let rex =
  Re.(
    [
      char('$'),
      opt(group(char('@'))),
      opt(group(char('?'))),
      group(
        alt([
          seq([
            alt([char('_'), rg('a', 'z')]),
            rep(
              alt([
                char('_'),
                char('\''),
                rg('a', 'z'),
                rg('A', 'Z'),
                rg('0', '9'),
              ]),
            ),
          ]),
          seq([char('{'), rep(diff(any, char('}'))), char('}')]),
        ]),
      ),
    ]
    |> seq
    |> compile
  );

let loc_raise = (_loc, exn) => raise(exn);

let const_string = (~loc, str) => {
  pexp_desc: Pexp_constant([@implicit_arity] Pconst_string(str, None)),
  pexp_loc: loc,
  pexp_attributes: [],
};

let parse_flags = (_config, flags, loc) => {
  let f_execute = ref(false);
  let f_nullable_results = ref(false);
  let host = ref(None);
  let port = ref(None);
  let user = ref(None);
  let password = ref(None);
  let database = ref(None);
  let unix_domain_socket_dir = ref(None);
  let comment_src_loc = ref(PGOCaml.comment_src_loc());
  let show = ref(None);
  let load_custom_from = ref(None);
  List.iter(
    fun
    | "execute" => f_execute := true
    | "nullable-results" => f_nullable_results := true
    | "show" => show := Some("show")
    | str when String.starts_with(str, "host=") => {
        let host' = String.sub(str, 5, String.length(str) - 5);
        host := Some(host');
      }
    | str when String.starts_with(str, "port=") => {
        let port' =
          int_of_string(String.sub(str, 5, String.length(str) - 5));
        port := Some(port');
      }
    | str when String.starts_with(str, "user=") => {
        let user' = String.sub(str, 5, String.length(str) - 5);
        user := Some(user');
      }
    | str when String.starts_with(str, "password=") => {
        let password' = String.sub(str, 9, String.length(str) - 9);
        password := Some(password');
      }
    | str when String.starts_with(str, "database=") => {
        let database' = String.sub(str, 9, String.length(str) - 9);
        database := Some(database');
      }
    | str when String.starts_with(str, "unix_domain_socket_dir=") => {
        let socket = String.sub(str, 23, String.length(str) - 23);
        unix_domain_socket_dir := Some(socket);
      }
    | str when String.starts_with(str, "comment_src_loc=") => {
        let comment_src_loc' = String.sub(str, 19, String.length(str) - 19);
        switch (comment_src_loc') {
        | "yes"
        | "1"
        | "on" => comment_src_loc := true
        | "no"
        | "0"
        | "off" => comment_src_loc := false
        | _ =>
          loc_raise(
            loc,
            Failure("Unrecognized value for option 'comment_src_loc'"),
          )
        };
      }
    | str when String.starts_with(str, "show=") => {
        let shownam = String.sub(str, 5, String.length(str) - 5);
        show := Some(shownam);
      }
    | str when String.starts_with(str, "load_custom_from=") => {
        let txt = String.sub(str, 17, String.length(str) - 17);
        load_custom_from := Some(Unix.getcwd() ++ "/" ++ txt);
      }
    | str => loc_raise(loc, Failure("Unknown flag: " ++ str)),
    flags,
  );
  let f_execute = f_execute^;
  let f_nullable_results = f_nullable_results^;
  let host = host^;
  let user = user^;
  let password = password^;
  let database = database^;
  let port = port^;
  let unix_domain_socket_dir = unix_domain_socket_dir^;
  let key =
    PGOCaml.describe_connection(
      ~host?,
      ~user?,
      ~password?,
      ~database?,
      ~port?,
      ~unix_domain_socket_dir?,
      (),
    );
  (
    key,
    f_execute,
    f_nullable_results,
    comment_src_loc^,
    show^,
    load_custom_from^,
  );
};

let mk_conversions = (~load_custom_from=?, ~loc, ~dbh, results) =>
  List.mapi(
    (i, (result, nullable)) => {
      let field_type = result.PGOCaml.field_type;
      let fn =
        switch (
          unravel_type(
            dbh,
            ~load_custom_from?,
            ~colnam=result.PGOCaml.name,
            field_type,
          )
        ) {
        | (nam, None) =>
          let fn = nam ++ "_of_string";
          [@metaloc loc]
          [%expr
            {
              open PGOCaml;
              %e
              exp_of_string(~loc, fn);
            }
          ];
        | (_nam, Some((_, deserialize))) => exp_of_string(~loc, deserialize)
        };

      let col = {
        let cname = "c" ++ string_of_int(i);
        Exp.ident({txt: Lident(cname), loc});
      };

      let sconv =
        [@metaloc loc]
        (
          switch%expr ([%e col]) {
          | Some(x) => x
          | None => "-"
          }
        );
      if (nullable) {
        (
          [@metaloc loc] [%expr PGOCaml_aux.Option.map([%e fn], [%e col])],
          sconv,
        );
      } else {
        (
          [@metaloc loc]
          [%expr
            [%e fn](
              try (PGOCaml_aux.Option.get([%e col])) {
              | _ =>
                failwith(
                  "ppx_pgsql's nullability heuristic has failed - use \"nullable-results\"",
                )
              },
            )
          ],
          sconv,
        );
      };
    },
    results,
  );

let coretype_of_type = (~loc, ~dbh, oid) => {
  let typ =
    switch (unravel_type(dbh, oid)) {
    | ("timestamp", _) =>
      [@implicit_arity]
      Longident.Ldot(
        [@implicit_arity] Ldot(Lident("CalendarLib"), "Calendar"),
        "t",
      )
    | (nam, _) => Lident(nam)
    };

  {
    ptyp_desc: [@implicit_arity] Ptyp_constr({txt: typ, loc}, []),
    ptyp_loc: loc,
    ptyp_attributes: [],
  };
};

/** produce a list pattern to match the result of a query */

let mk_listpat = (~loc, results) =>
  List.fold_right(
    (i, tail) => {
      let var = Pat.var @@ {txt: "c" ++ string_of_int(i), loc};
      [@metaloc loc] [%pat? [[%p var], ...[%p tail]]];
    },
    range(0, List.length(results)),
    [@metaloc loc] [%pat? []],
  );

let pgsql_expand = (~genobject, ~flags=[], ~config, loc, dbh, query) => {
  open Rresult;
  let (
    key,
    f_execute,
    f_nullable_results,
    comment_src_loc,
    show,
    load_custom_from,
  ) =
    parse_flags(config, flags, loc);
  let query =
    if (comment_src_loc) {
      let start = loc.Location.loc_start;
      Lexing.(
        Printf.sprintf("-- '%s' L%d\n", start.pos_fname, start.pos_lnum)
        ++ query
      );
    } else {
      query;
    };

  /* Connect, if necessary, to the database. */
  get_connection(~loc, key)
  >>= (
    my_dbh => {
      /* Split the query into text and variable name parts using Re.split_full.
        * eg. "select id from employees where name = $name and salary > $salary"
        * would become a structure equivalent to:
        * ["select id from employees where name = "; "$name"; " and salary > ";
        * "$salary"].
        * Actually it's a wee bit more complicated than that ...
       */
      let split = {
        let f =
          fun
          | `Text(text) => `Text(text)
          | `Delim(subs) =>
            `Var(Re.Group.(get(subs, 3), test(subs, 1), test(subs, 2)));
        List.map(f, Re.split_full(rex, query));
      };

      /* Go to the database, prepare this statement, and find out exactly
        * what the parameter types and return values are.  Exceptions can
        * be raised here if the statement is bad SQL.
       */
      let ((params, results), varmap) = {
        /* Rebuild the query with $n placeholders for each variable. */
        let next = {
          let i = ref(0);
          () => {
            incr(i);
            i^;
          };
        };
        let varmap = Hashtbl.create(8);
        let query =
          String.concat(
            "",
            List.map(
              fun
              | `Text(text) => text
              | `Var(varname, false, option) => {
                  let i = next();
                  Hashtbl.add(varmap, i, (varname, false, option));
                  sprintf("$%d", i);
                }
              | `Var(varname, true, option) => {
                  let i = next();
                  Hashtbl.add(varmap, i, (varname, true, option));
                  sprintf("($%d)", i);
                },
              split,
            ),
          );
        let varmap =
          Hashtbl.fold((i, var, vars) => [(i, var), ...vars], varmap, []);
        try (
          {
            PGOCaml.prepare(my_dbh, ~query, ());
            (PGOCaml.describe_statement(my_dbh, ()), varmap);
          }
        ) {
        | exn => loc_raise(loc, exn)
        };
      };

      /* If the PGSQL(dbh) "execute" flag was used, we will actually
        * execute the statement now.  Normally this would never be used, but
        * some statements need to be executed, particularly CREATE TEMPORARY
        * TABLE.
       */
      if (f_execute) {
        ignore(PGOCaml.execute(my_dbh, ~params=[], ()));
      };

      /* Number of params should match length of map, otherwise something
        * has gone wrong in the substitution above.
       */
      if (List.length(varmap) != List.length(params)) {
        loc_raise(
          loc,
          Failure(
            "Mismatch in number of parameters found by database. "
            ++ "Most likely your statement contains bare $, $number, etc.",
          ),
        );
      };

      /* Generate a function for converting the parameters.
        *
        * See also:
        * http://archives.postgresql.org/pgsql-interfaces/2006-01/msg00043.php
       */
      let params =
        List.fold_right(
          ((i, {PGOCaml.param_type}), tail) => {
            let (varname, list, option) = List.assoc(i, varmap);
            let argnam =
              if (String.starts_with(varname, "{")) {
                None;
              } else {
                Some(varname);
              };

            let varname =
              if (String.starts_with(varname, "{")) {
                String.sub(varname, 1, String.length(varname) - 2);
              } else {
                varname;
              };

            let (varname, typnam) =
              switch (String.index_opt(varname, ':')) {
              | None => (varname, None)
              | Some(_) =>
                [@warning "-8"]
                {
                  let [varname, typnam] = String.split_on_char(':', varname);
                  (varname, Some(String.trim(typnam)));
                }
              };

            let varname = exp_of_string(~loc, varname);
            let varname = {...varname, pexp_loc: loc};
            let fn =
              switch (
                unravel_type(
                  ~load_custom_from?,
                  ~argnam?,
                  ~typnam?,
                  my_dbh,
                  param_type,
                )
              ) {
              | (nam, None) =>
                let fn = exp_of_string(~loc, "string_of_" ++ nam);
                [@metaloc loc]
                [%expr
                  {
                    open PGOCaml;
                    %e
                    fn;
                  }
                ];
              | (_, Some((serialize, _))) => exp_of_string(~loc, serialize)
              };

            let head =
              switch (list, option) {
              | (false, false) =>
                [@metaloc loc] [%expr [Some([%e fn]([%e varname]))]]
              | (false, true) =>
                [@metaloc loc]
                [%expr [PGOCaml_aux.Option.map([%e fn], [%e varname])]]
              | (true, false) =>
                [@metaloc loc]
                [%expr List.map(x => Some([%e fn](x)), [%e varname])]
              | (true, true) =>
                [@metaloc loc]
                [%expr
                  List.map(
                    x => PGOCaml_aux.Option.map([%e fn]),
                    [%e varname],
                  )
                ]
              };

            [@metaloc loc] [%expr [[%e head], ...[%e tail]]];
          },
          List.combine(range(1, 1 + List.length(varmap)), params),
          [@metaloc loc] [%expr []],
        );

      /* Substitute expression. */
      let expr = {
        let split =
          List.fold_right(
            (s, tail) => {
              let head =
                switch (s) {
                | `Text(text) =>
                  [@metaloc loc] [%expr `Text([%e const_string(~loc, text)])]
                | `Var(varname, list, option) =>
                  let list =
                    if (list) {
                      [@metaloc loc] [%expr true];
                    } else {
                      [@metaloc loc] [%expr false];
                    };
                  let option =
                    if (option) {
                      [@metaloc loc] [%expr true];
                    } else {
                      [@metaloc loc] [%expr false];
                    };
                  [@metaloc loc]
                  [%expr
                    `Var((
                      [%e const_string(~loc, varname)],
                      [%e list],
                      [%e option],
                    ))
                  ];
                };
              [@metaloc loc] [%expr [[%e head], ...[%e tail]]];
            },
            split,
            [@metaloc loc] [%expr []],
          );
        /* let original_query = $str:query$ in * original query string */
        /* Rebuild the query with appropriate placeholders.  A single list
          * param can expand into several placeholders.
         */
        /* Flatten the parameters to a simple list now. */
        /* Get a unique name for this query using an MD5 digest. */
        /* Get the hash table used to keep track of prepared statements. */
        /* Have we prepared this statement already?  If not, do so. */
        /* Execute the statement, returning the rows. */
        [@metaloc loc]
        {
          let%expr dbh = [%e dbh];
          let params: list(list(option(string))) = [%e params];
          let split = [%e split]; /* split up query */

          let i = ref(0); /* Counts parameters. */
          let j = ref(0); /* Counts placeholders. */
          let query =
            String.concat(
              "",
              List.map(
                fun
                | `Text(text) => text
                | `Var(_varname, false, _) => {
                    /* non-list item */
                    let () = incr(i); /* next parameter */
                    let () = incr(j); /* next placeholder number */
                    "$" ++ string_of_int(j.contents);
                  }
                | `Var(_varname, true, _) => {
                    /* list item */
                    let param = List.nth(params, i.contents);
                    let () = incr(i); /* next parameter */
                    "("
                    ++ String.concat(
                         ",",
                         List.map(
                           _ => {
                             let () = incr(j); /* next placeholder number */
                             "$" ++ string_of_int(j.contents);
                           },
                           param,
                         ),
                       )
                    ++ ")";
                  },
                split,
              ),
            );

          let params = List.flatten(params);

          let name = "ppx_pgsql." ++ Digest.to_hex(Digest.string(query));

          let hash =
            try (PGOCaml.private_data(dbh)) {
            | Not_found =>
              let hash = Hashtbl.create(17);
              PGOCaml.set_private_data(dbh, hash);
              hash;
            };

          let is_prepared = Hashtbl.mem(hash, name);
          PGOCaml.bind(
            if (!is_prepared) {
              PGOCaml.bind(
                PGOCaml.prepare(dbh, ~name, ~query, ()),
                () => {
                  Hashtbl.add(hash, name, true);
                  PGOCaml.return();
                },
              );
            } else {
              PGOCaml.return();
            },
            () =>
            PGOCaml.execute_rev(dbh, ~name, ~params, ())
          );
        };
      };

      /*** decorate the results with the nullability heuristic */
      let results' =
        switch (results) {
        | Some(results) =>
          Some(
            List.map(
              result =>
                switch (result.PGOCaml.table, result.PGOCaml.column) {
                | (Some(table), Some(column)) =>
                  /* Find out whether the column is nullable from the
                    * database pg_attribute table.
                   */
                  let params = [
                    Some(PGOCaml.string_of_oid(table)),
                    Some(PGOCaml.string_of_int(column)),
                  ];
                  let _rows =
                    PGOCaml.execute(my_dbh, ~name=nullable_name, ~params, ());
                  let not_nullable =
                    switch (_rows) {
                    | [[Some(b)]] => PGOCaml.bool_of_string(b)
                    | _ => false
                    };
                  (result, f_nullable_results || !not_nullable);
                | _ => (result, f_nullable_results || true) /* Assume it could be nullable. */
                },
              results,
            ),
          )
        | None => None
        };

      let mkexpr = (~convert, ~list) =>
        /* This should never happen, even if the schema changes.
             * Well, maybe if the user does 'SELECT *'.
         */
        [@metaloc loc]
        [%expr
          PGOCaml.bind([%e expr], _rows =>
            PGOCaml.return(
              {
                let original_query = [%e const_string(~loc, query)];
                List.rev_map(
                  row =>
                    switch (row) {
                    | [%p list] =>
                      %e
                      convert
                    | _ =>
                      let msg =
                        "ppx_pgsql: internal error: "
                        ++ "Incorrect number of columns returned from query: "
                        ++ original_query
                        ++ ".  Columns are: "
                        ++ String.concat(
                             "; ",
                             List.map(
                               fun
                               | Some(str) => Printf.sprintf("%S", str)
                               | None => "NULL",
                               row,
                             ),
                           );
                      raise(PGOCaml.Error(msg));
                    },
                  _rows,
                );
              },
            )
          )
        ];

      /* If we're expecting any result rows, then generate a function to
        * convert them.  Otherwise return unit.  Note that we can only
        * determine the nullability of results if they correspond to real
        * columns in a table, otherwise the type will always be 'type option'.
       */
      switch (genobject, results') {
      | (true, Some(results)) =>
        let list = mk_listpat(~loc, results);
        let fields =
          List.map(
            (({PGOCaml.name, field_type, _}, nullable)) => (
              name,
              coretype_of_type(~loc, ~dbh=my_dbh, field_type),
              nullable,
            ),
            results,
          );

        let convert =
          List.fold_left2(
            ((lsacc, showacc), (name, _, _), (conv, sconv)) => {
              let hd = {
                pcf_desc:
                  [@implicit_arity]
                  Pcf_method(
                    {txt: name, loc},
                    Public,
                    [@implicit_arity] Cfk_concrete(Fresh, conv),
                  ),
                pcf_loc: loc,
                pcf_attributes: [],
              };

              let ename = const_string(~loc, name);
              let showacc =
                [@metaloc loc]
                {
                  let%expr fields = [([%e ename], [%e sconv]), ...fields];
                  %e
                  showacc;
                };

              ([hd, ...lsacc], showacc);
            },
            (
              [],
              [@metaloc loc]
              [%expr
                List.fold_left(
                  (buffer, (name, value)) => {
                    let () = Buffer.add_string(buffer, name);
                    let () = Buffer.add_char(buffer, ':');
                    let () = Buffer.add_char(buffer, ' ');
                    let () = Buffer.add_string(buffer, value);
                    let () = Buffer.add_char(buffer, '\n');
                    buffer;
                  },
                  Buffer.create(16),
                  fields,
                )
                |> Buffer.contents
              ],
            ),
            fields,
            mk_conversions(~load_custom_from?, ~loc, ~dbh=my_dbh, results),
          )
          |> (
            ((fields, fshow)) => {
              let fshow =
                [@metaloc loc]
                {
                  let%expr fields = [];
                  %e
                  fshow;
                };

              let fields =
                switch (show) {
                | Some(txt) => [
                    {
                      pcf_desc:
                        [@implicit_arity]
                        Pcf_method(
                          {txt, loc},
                          Public,
                          [@implicit_arity] Cfk_concrete(Fresh, fshow),
                        ),
                      pcf_loc: loc,
                      pcf_attributes: [],
                    },
                    ...fields,
                  ]
                | None => fields
                };

              Exp.mk(
                Pexp_object({
                  pcstr_self: Pat.any(~loc, ()),
                  pcstr_fields: fields,
                }),
              );
            }
          );

        let expr = mkexpr(~convert, ~list);
        Ok(expr);
      | (true, None) =>
        [@implicit_arity]
        Error(
          "It doesn't make sense to make an object to encapsulate results that aren't coming",
          loc,
        )
      | (false, Some(results)) =>
        let list = mk_listpat(~loc, results);
        let convert = {
          let conversions =
            mk_conversions(~load_custom_from?, ~loc, ~dbh=my_dbh, results)
            |> List.map(fst);

          /* Avoid generating a single-element tuple. */
          switch (conversions) {
          | [] => [@metaloc loc] [%expr ()]
          | [a] => a
          | conversions => Exp.tuple(conversions)
          };
        };

        Ok(mkexpr(~convert, ~list));
      | (false, None) =>
        Ok(
          [@metaloc loc]
          [%expr PGOCaml.bind([%e expr], _rows => PGOCaml.return())],
        )
      };
    }
  );
};

let expand_sql = (~genobject, ~config, loc, dbh, extras) => {
  let (query, flags) =
    switch (List.rev(extras)) {
    | [] => assert(false)
    | [query, ...flags] => (query, flags)
    };
  try (pgsql_expand(~config, ~genobject, ~flags, loc, dbh, query)) {
  | Failure(s) => [@implicit_arity] Error(s, loc)
  | PGOCaml.Error(s) => [@implicit_arity] Error(s, loc)
  | [@implicit_arity] PGOCaml.PostgreSQL_Error(s, fields) =>
    let fields' =
      List.map(((c, s)) => Printf.sprintf("(%c: %s)", c, s), fields);
    [@implicit_arity]
    Error(
      "Postgres backend error: "
      ++ s
      ++ ": "
      ++ s
      ++ String.concat(",", fields'),
      loc,
    );
  | exn =>
    [@implicit_arity]
    Error("Unexpected PG'OCaml PPX error: " ++ Printexc.to_string(exn), loc)
  };
};

/* Returns the empty list if one of the elements is not a string constant */
let list_of_string_args = (mapper, args) => {
  let maybe_strs =
    List.map(
      fun
      | (
          Nolabel,
          {
            pexp_desc:
              Pexp_constant([@implicit_arity] Pconst_string(str, None)),
            _,
          },
        ) =>
        Some(str)
      | (_, other) =>
        switch (mapper(other)) {
        | {
            pexp_desc:
              Pexp_constant([@implicit_arity] Pconst_string(str, None)),
            _,
          } =>
          Some(str)
        | _ => None
        },
      args,
    );

  if (List.mem(None, maybe_strs)) {
    [];
  } else {
    List.map(
      fun
      | Some(x) => x
      | None => assert(false),
      maybe_strs,
    );
  };
};

let pgocaml_rewriter = (config, _cookies) => {
  ...default_mapper,
  expr: (mapper, expr) => {
    let unsupported = loc => {
      ...expr,
      pexp_desc:
        Pexp_extension(
          extension_of_error @@
          Location.error(~loc, Printf.sprintf("Something unsupported")),
        ),
    };

    switch (expr) {
    | {
        pexp_desc:
          [@implicit_arity]
          Pexp_extension(
            {txt, loc},
            PStr([
              {
                pstr_desc:
                  [@implicit_arity]
                  Pstr_eval(
                    {
                      pexp_desc: [@implicit_arity] Pexp_apply(dbh, args),
                      pexp_loc: qloc,
                      _,
                    },
                    _,
                  ),
                _,
              },
            ]),
          ),
        _,
      }
        when String.starts_with(txt, "pgsql") =>
      open Rresult;
      let genobject = txt == "pgsql.object";
      switch (list_of_string_args(default_mapper.expr(mapper), args)) {
      | [] => unsupported(loc)
      | args =>
        let x = expand_sql(~config, ~genobject, loc, dbh, args);
        switch (x) {
        | Rresult.Ok({pexp_desc, pexp_loc: _, pexp_attributes}) => {
            pexp_desc,
            pexp_loc: qloc,
            pexp_attributes,
          }
        | [@implicit_arity] Error(s, loc) => {
            ...expr,
            pexp_desc:
              Pexp_extension(
                extension_of_error @@
                Location.error(~loc, "PG'OCaml PPX error: " ++ s),
              ),
            pexp_loc: loc,
          }
        };
      };
    | {
        pexp_desc: [@implicit_arity] Pexp_extension({txt: "pgsql", loc}, _),
        _,
      } =>
      unsupported(loc)
    | other => default_mapper.expr(mapper, other)
    };
  },
};

/*let migration =
    Versions.migrate Versions.ocaml_407 Versions.ocaml_current

  let _ =
    Migrate_parsetree.Compiler_libs.Ast_mapper.register
      "pgocaml"
      (fun args -> migration.copy_mapper (pgocaml_mapper args))
  */

let () = {
  Migrate_parsetree.Driver.register(
    ~name="pgocaml",
    ~args=[],
    Versions.ocaml_407,
    pgocaml_rewriter
  );

  let argv =
    switch (Sys.argv) {
    | [|program, input_file, output_file|] =>
      [|program, input_file, "-o", output_file, "--dump-ast" |]
    | _ =>
      Sys.argv
      /* Or print some error message, because BuckleScript should
         never pass any other pattern of arguments. */
    };

  Migrate_parsetree.Driver.run_main(~argv, ());
};
