defmodule Snow.Enrich do
  def enrich(stream, schemas \\ nil, tags \\ nil) do
    stream
    |> __MODULE__.Tag.exec(tags)
    |> __MODULE__.Context.exec(schemas)
    |> __MODULE__.Unstructured.exec(schemas)
  end

  def web(stream) do
    stream
    |> __MODULE__.Web.URL.exec()
    |> __MODULE__.Web.Referer.exec()
    |> __MODULE__.Web.Marketing.exec()
    |> __MODULE__.Web.IPAddress.exec()
    |> __MODULE__.Web.UserAgent.exec()
    |> __MODULE__.Web.Browser.exec()
  end

  def to_json(stream) do
    stream
    |> Snow.Payload.derived_contexts()
    |> __MODULE__.Serializer.JSON.exec()
  end

  def into(stream, factory) do
    stream
    |> Nile.route_into(&(&1), factory)
  end
end
