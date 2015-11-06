defmodule Snow.ETL.Shredder.Unstructured do
  @derives [Poison.Encoder]

  defstruct schema: %{},
            hierarchy: %{},
            data: %{}

  @unstruct_event "iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1"

  def exec(stream, schemas \\ %{}) do
    stream
    |> Nile.expand(fn
      (%Snow.Model{} = model) ->
        [model | shred(model, schemas)]
      (item) ->
        [item]
    end)
  end

  defp shred(%{unstruct_event: nil}, _) do
    []
  end
  defp shred(model = %{unstruct_event: json}, schemas) when is_binary(json) do
    shred(%{model | unstruct_event: Poison.decode!(json)}, schemas)
  rescue
    Poison.SyntaxError ->
      []
  end
  defp shred(model = %{unstruct_event: %{"schema" => @unstruct_event <> _, "data" => data}}, schemas) do
    shred(%{model | unstruct_event: data}, schemas)
  end
  defp shred(model = %{unstruct_event: %{"schema" => schema}}, schemas) do
    case Dict.fetch(schemas, schema) do
      {:ok, schema} ->
        format(model, schema)
      :error ->
        [error(model, schema)]
    end
  end

  defp format(event = %{unstruct_event: unstruct_event}, %{"self" => schema}) do
    %__MODULE__{
      schema: schema,
      hierarchy: Snow.ETL.Shredder.Utils.hierarchy(event, schema["name"]),
      data: unstruct_event["data"] || %{}
    }
    |> Snow.ETL.Shredder.Utils.explode()
  end

  defp error(model, schema) do
    %__MODULE__{
      schema: %{
        vendor: "com.snowplowanalytics.snowplow",
        name: "bad_raw_event",
        format: "jsonschema",
        version: "1-0-0"
      },
      hierarchy: Snow.ETL.Shredder.Utils.hierarchy(model, "bad_raw_event"),
      data: %{
        line: "",
        errors: Poison.encode!(["Unknown schema #{inspect(schema)}"])
      }
    }
  end
end
