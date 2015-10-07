defmodule Snow.Shredder.Unstructured do
  @derives [Poison.Encoder]

  defstruct schema: %{},
            hierarchy: %{},
            data: %{}

  @unstruct_event "iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1"

  def exec(stream, schemas \\ %{}) do
    stream
    |> Stream.transform(nil, fn(model, a) ->
      {shred(model, schemas), a}
    end)
  end

  defp shred(%{unstruct_event: nil}, _) do
    []
  end
  defp shred(model = %{unstruct_event: json}, schemas) when is_binary(json) do
    shred(%{model | unstruct_event: Poison.decode!(json)}, schemas)
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

  defp format(%Snow.Model{event_id: event_id, collector_tstamp: collector_tstamp, unstruct_event: unstruct_event}, %{"self" => schema}) do
    %__MODULE__{
      schema: schema,
      hierarchy: %{
        rootId: event_id,
        rootTstamp: collector_tstamp,
        refRoot: "events",
        refTree: Poison.encode!(["events", schema["name"]]),
        refParent: "events"
      },
      data: unstruct_event["data"] || %{}
    }
    |> explode()
  end

  defp error(%{event_id: event_id, collector_tstamp: collector_tstamp}, schema) do
    %__MODULE__{
      schema: %{
        vendor: "com.snowplowanalytics.snowplow",
        name: "bad_raw_event",
        format: "jsonschema",
        version: "1-0-0"
      },
      hierarchy: %{
        rootId: event_id,
        rootTstamp: collector_tstamp,
        refRoot: "events",
        refTree: unquote(Poison.encode!(["events", "bad_raw_event"])),
        refParent: "events"
      },
      data: %{
        line: "",
        errors: Poison.encode!(["Unknown schema #{inspect(schema)}"])
      }
    }
  end

  defp explode(event = %{data: data}) do
    data
    |> Enum.map(fn
      ({key, value}) when is_list(value) ->
        {key, value}
      ({key, value}) ->
        {key, [value]}
    end)
    |> explode(event)
  end

  defp explode([], event) do
    [event]
  end

  for count <- 1..30 do
    key = fn(i) -> "key_#{i}" |> String.to_atom |> Macro.var(nil) end
    value = fn(i) -> "value_#{i}" |> String.to_atom |> Macro.var(nil) end
    value_i = fn(i) -> "value_#{i}_i" |> String.to_atom |> Macro.var(nil) end

    range = 1..count

    args = range |> Enum.map(&({key.(&1), value.(&1)}))
    f_args = range |> Enum.map(&({:<-, [], [value_i.(&1), value.(&1)]}))
    m_args = range |> Enum.map(&({key.(&1), value_i.(&1)}))

    defp explode(unquote(args), event) do
      for unquote_splicing(f_args) do
        %{event | data: :maps.from_list(unquote(m_args))}
      end
    end
  end
end
