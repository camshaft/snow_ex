defmodule Snow.Enrich.Unstructured do
  import Snow.Model.BadRawEvent
  @unstruct_event "iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1"

  def exec(stream, schemas \\ %{}) do
    stream |> Snow.Payload.derive(&(shred(&1, schemas)))
  end

  defp shred(%{unstruct_event: nil}, _) do
    []
  end
  defp shred(model = %{unstruct_event: json}, schemas) when is_binary(json) do
    shred(%{model | unstruct_event: Poison.decode!(json)}, schemas)
  rescue
    Poison.SyntaxError ->
      [syntax_error(json, model)]
  end
  defp shred(model = %{unstruct_event: %{"schema" => @unstruct_event <> _, "data" => data}}, schemas) do
    shred(%{model | unstruct_event: data}, schemas)
  end
  defp shred(model = %{unstruct_event: event}, schemas) do
    schemas.shred(event, model)
  end
end
