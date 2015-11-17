defmodule Snow.Test do
  use ExUnit.Case

  test "enrich" do
    Snow.Payload.Generator.payloads()
    |> Enum.take(4)
    |> Snow.Enrich.enrich(Etl.Enrich.Schemas, ["tag123"])
    |> Snow.Enrich.common()
    |> Snow.Enrich.web()
    |> Snow.Enrich.to_json()
    |> Snow.Enrich.into(fn(_) -> [] end)
    |> Enum.map(fn({prefix, data}) ->
      IO.puts([prefix, ":\n\n", data])
    end)
  end
end
