defmodule Snow.Enrich do
  def contexts do
    __MODULE__.Web.contexts
    ++ __MODULE__.Common.contexts
  end

  def extract_contexts(dir \\ "schemas") do
    contexts
    |> Enum.each(fn(%{"self" => %{"vendor" => vendor, "name" => name,
                                  "format" => format, "version" => version}} = context) ->
      target = [dir, vendor, name, format] |> Path.join()
      target |> File.mkdir_p!()

      [target, "#{version}.json"]
      |> Path.join()
      |> File.write!(Poison.encode!(context, pretty: true))
    end)
  end

  def enrich(stream, schemas \\ nil, tags \\ nil) do
    stream
    |> __MODULE__.Tag.exec(tags)
    |> __MODULE__.Context.exec(schemas)
    |> __MODULE__.Unstructured.exec(schemas)
  end

  def common(stream) do
    __MODULE__.Common.exec(stream)
  end

  def web(stream) do
    __MODULE__.Web.exec(stream)
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
