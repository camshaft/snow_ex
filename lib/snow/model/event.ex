defmodule Snow.Model.Event do
            #https://github.com/snowplow/snowplow/wiki/canonical-event-model#211-application-fields
  fields = [app_id: :string,
            platform: :string,

            #https://github.com/snowplow/snowplow/wiki/canonical-event-model#212-date--time-fields
            collector_tstamp: :integer,
            dvce_created_tstamp: :integer,
            dvce_sent_tstamp: :integer,
            etl_tstamp: :integer,
            derived_tstamp: :integer,
            true_tstamp: :integer,

            #https://github.com/snowplow/snowplow/wiki/canonical-event-model#213-event--transaction-fields
            event: :string,
            event_id: :string,

            #https://github.com/snowplow/snowplow/wiki/canonical-event-model#214-snowplow-version-fields
            v_tracker: :string,
            v_collector: :string,
            v_etl: :string,
            name_tracker: :string,
            etl_tags: :string]

  defstruct Enum.map(fields, &({elem(&1, 0), nil}))

  field_vars = fields
  |> Dict.drop([:etl_tags, :etl_tstamp, :v_etl, :derived_tstamp])
  |> Dict.keys()
  |> Enum.map(&({&1, Macro.var(&1, nil)}))

  def from_payload(payload = %{unquote_splicing(field_vars)}) do
    %{payload | atomic_event: %__MODULE__{unquote_splicing([{:etl_tags, []} | field_vars])}}
  end

  use Dict

  def delete(event, key) do
    Map.put(event, key, nil)
  end

  def fetch(event, key) do
    Map.fetch(event, key)
  end

  def new do
    %__MODULE__{}
  end

  def put(event, :etl_tags, []) do
    event
  end
  def put(event = %{etl_tags: etl_tags}, :etl_tags, contexts) when is_list(contexts) do
    %{event | etl_tags: etl_tags ++ contexts}
  end
  def put(event, :etl_tags, contexts) do
    put(event, :etl_tags, [contexts])
  end
  def put(event, key, value) when key in unquote(fields) do
    Map.put(event, key, value)
  end

  def size(_) do
    unquote(length(fields))
  end

  def reduce(event, acc, fun) do
    event
    |> Map.delete(:__struct__)
    |> Enumerable.Map.reduce(acc, fun)
  end
end

defimpl Poison.Encoder, for: Snow.Model.Event do
  def encode(event, options) do
    :maps.fold(fn
      (_, nil, acc) ->
        acc
      (k, [], acc) when k in [:etl_tags] ->
        acc
      (k, tags, acc) when k in [:etl_tags] ->
        Map.put(acc, k, to_string(Poison.encode!(tags, options)))
      (k, v, acc) ->
        Map.put(acc, k, v)
    end, %{}, Map.delete(event, :__struct__))
    |> Poison.encode!(options)
  end
end

defimpl Snow.Model, for: Snow.Model.Event do
  def name(_), do: "events"
end
