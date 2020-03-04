let init_dbh = dbh => {
  let () = [%pgsql
    dbh(
      "execute",
      "create temporary table employees\n    (\n    userid serial primary key,\n    name text not null,\n    salary int not null,\n    email text\n    )",
    )
  ];

  let () = [%pgsql
    dbh(
      "execute",
      " DO $$ BEGIN\n      CREATE DOMAIN cash_money AS float;\n      EXCEPTION\n        WHEN duplicate_object THEN null;\n      END $$",
    )
  ];

  %pgsql
  dbh(
    "execute",
    "CREATE TEMPORARY TABLE customtable (\n    userid int4 NOT NULL,\n    salary cash_money NOT NULL\n  )",
  );
};

module Userid: {
  type t;
  let to_string: t => string;
  let from_string: string => t;
  let to_int: t => int;
} = {
  type t = int;
  let to_string = string_of_int;
  let from_string = int_of_string;
  let to_int = x => x;
};

let employee_exists = (dbh, ~email=?, n) => [%pgsql
  dbh(
    "SELECT EXISTS (SELECT 1 FROM employees WHERE name = $n AND email = $?email)",
  )
];

let () = {
  let dbh = PGOCaml.connect();

  init_dbh(dbh);

  let insert = (name, pay, email) => [%pgsql
    dbh(
      "insert into employees (name, salary, email) values ($name, $pay, $?email)",
    )
  ];
  insert("Ann", 10000l, None);
  insert("Bob", 45000l, None);
  insert("Jim", 20000l, None);
  insert("Mary", 30000l, Some("mary@example.com"));

  let rows = [%pgsql
    dbh(
      "load_custom_from=tests_ppx/config.sexp",
      "select userid, name, salary, email from employees",
    )
  ];

  List.iter(
    ((id, name, salary, email)) => {
      let email =
        switch (email) {
        | Some(email) => email
        | None => "-"
        };
      Printf.printf(
        "%d %S %ld %S\n",
        Userid.to_int(id),
        name,
        salary,
        email,
      );
    },
    rows,
  );

  let ids = [1l, 3l];
  let rows = [%pgsql.object
    dbh("show=pp", "select * from employees where userid in $@ids")
  ];
  List.iter(obj => print_endline(obj#pp), rows);
  let uid = Userid.from_string("69");
  let salary = "$420.00";
  let () = [%pgsql
    dbh(
      "load_custom_from=tests_ppx/config.sexp",
      "INSERT INTO customtable (userid, salary) VALUES (${uid:userid}, $salary)",
    )
  ];
  let rows' = [%pgsql.object
    dbh(
      "load_custom_from=tests_ppx/config.sexp",
      "show",
      "SELECT * FROM customtable WHERE salary = $salary",
    )
  ];

  List.iter(
    obj =>
      Printf.printf(
        "%d was paid %s\n",
        Userid.to_int(obj#userid),
        obj#salary,
      ),
    rows',
  );
  let all_employees = [%pgsql.object
    dbh(
      "load_custom_from=tests_ppx/config.sexp",
      "SELECT array_agg(userid) as userids FROM employees",
    )
  ];

  let () = print_endline("All userID's:");
  List.iter(
    x =>
      Option.map(
        List.iter(x =>
          Option.map(
            userid => Userid.to_string(userid) |> Printf.printf("\t%s\n"),
            x,
          )
          |> ignore
        ),
        x#userids,
      )
      |> ignore,
    all_employees,
  );

  PGOCaml.close(dbh);
};
