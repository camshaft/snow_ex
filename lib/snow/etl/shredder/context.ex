defmodule Snow.ETL.Shredder.Context do
  @derives [Poison.Encoder]

  defstruct schema: %{},
            hierarchy: %{},
            data: %{}

  @context "iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1"

  def exec(stream, schemas \\ %{}) do
    stream
    |> Nile.expand(fn
      (%Snow.Model{} = model) ->
        [model | shred(model, schemas)]
      (item) ->
        [item]
    end)
  end

  defp shred(%{context: nil}, _) do
    []
  end
  defp shred(model = %{context: json}, schemas) when is_binary(json) do
    shred(%{model | context: Poison.decode!(json)}, schemas)
  end
  defp shred(model = %{context: %{"schema" => @context <> _, "data" => contexts}}, schemas) do
    shred(%{model | context: contexts}, schemas)
  end
  defp shred(model = %{context: contexts}, schemas) when is_list(contexts) do
    Enum.reduce(contexts, [], fn(%{"schema" => schema}, acc) ->
      case Dict.fetch(schemas, schema) do
        {:ok, schema} ->
          format(model, schema) ++ acc
        :error ->
          [error(model, schema) | acc]
      end
    end)
  end

  defp format(parent, %{"self" => schema} = event) do
    %__MODULE__{
      schema: schema,
      hierarchy: Snow.ETL.Shredder.Utils.hierarchy(parent, schema["name"]),
      data: event["data"] || %{}
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
