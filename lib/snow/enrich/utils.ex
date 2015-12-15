defmodule Snow.Enrich.Utils do
  def hierarchy(%{event_id: event_id, collector_tstamp: collector_tstamp}, name) do
    %{
      "rootId": event_id,
      "rootTstamp": collector_tstamp,
      "refRoot": "events",
      "refTree": Poison.encode!(["events", name]),
      "refParent": "events"
    }
  end

  def explode(event = %{data: data}) do
    data
    |> Enum.map(fn
      ({key, []}) ->
        {key, [nil]}
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
