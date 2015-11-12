defmodule Snow.ETL.Shredder.Context do
  import Snow.ETL.Schemas.BadRawEvent
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
  rescue
    Poison.SyntaxError ->
      [syntax_error(json, model)]
  end
  defp shred(model = %{context: %{"schema" => @context <> _, "data" => contexts}}, schemas) do
    shred(%{model | context: contexts}, schemas)
  end
  defp shred(model = %{context: contexts}, schemas) when is_list(contexts) do
    Enum.reduce(contexts, [], fn(event, acc) ->
      schemas.shred(event, model) ++ acc
    end)
  end
end
