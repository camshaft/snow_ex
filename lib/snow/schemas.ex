defmodule Snow.Schemas do
  defmacro __using__(opts) do
    schemas = (opts[:index] || "") <> "/**/*.json"
    |> Path.wildcard()
    |> Enum.map(&(&1 |> File.read! |> Poison.decode!))

    quote do
      use Snow.Schemas.Compiler, schemas: unquote(schemas)
    end
  end
end
