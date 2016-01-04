defmodule Snow.Enrich.Serializer.CSV do
  def exec(stream, paths, options \\ []) do
    stream
    |> Stream.map(fn(item) ->
      name = Snow.Model.name(item)
      data = [paths.match(name, item)] |> CSV.encode(options)
      {name, data}
    end)
  end
end
