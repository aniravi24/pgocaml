( ( (And
      ( (Or
          ( (Rule (typnam userid))
            (Rule (typnam int4))
            (Rule (typnam int32))
          )
        )
        (Or
          ( (Rule (typnam userid))
            (Rule (argnam userid))
            (Rule (colnam userid))
          )
        )
      )
    )
    ( (serialize Userid.to_string)
      (deserialize Userid.from_string)
    )
  )
  ( (And
      ( (Or
          ( (Rule (typnam cash_money))
            (Rule (typnam float))
          )
        )
        (Or
          ( (Rule (argnam salary))
            (Rule (colnam salary))
            (Rule (argnam customsalary))
          )
        )
      )
    )
    ( (serialize "fun x -> String.(sub x 1 (length x - 1)) |> String.trim")
      (deserialize "fun x -> \"$\" ^ x")
    )
  )
  ( (And
      ( (Rule (colnam userids))
        (Rule (typnam int32_array))
      )
    )
    ( (serialize "PGOCaml.string_of_arbitrary_array Userid.to_string")
      (deserialize "PGOCaml.arbitrary_array_of_string Userid.from_string")
    )
  )
)