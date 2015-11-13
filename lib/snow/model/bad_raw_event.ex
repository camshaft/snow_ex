defmodule Snow.Model.BadRawEvent do
  def syntax_error(data, parent) do
    %{bad_raw_event(parent) | data: %{
      "line": "",
      "errors": ["Syntax error #{inspect(data, limit: 5_000)}"]
    }}
  end

  def unknown_schema(%{"schema" => schema}, parent) do
    %{bad_raw_event(parent) | data: %{
      "line": "",
      "errors": ["Unknown schema #{inspect(schema, limit: 5_000)}"]
    }}
  end

  def invalid_data(event, parent) do
    data = (event["data"] || %{}) |> Poison.encode!()
    %{bad_raw_event(parent) | data: %{
      "line": "",
      "errors": ["Invalid data for " <> event["schema"] <> ": " <> data]
    }}
  end

  def bad_raw_event(parent) do
    %Snow.Model.Context{
      parent: parent,
      schema: %{
        "vendor": "com.snowplowanalytics.snowplow",
        "name": "bad_raw_event",
        "format": "jsonschema",
        "version": "1-0-0"
      }
    }
  end
end
