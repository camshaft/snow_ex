defmodule Snow.ETL.Schemas.BadRawEvent do
  def syntax_error(data, parent) do
    %{bad_raw_event(parent) | data: %{
      "line": "",
      "errors": Poison.encode!(["Syntax error #{inspect(data)}"])
    }}
  end

  def unknown_schema(%{"schema" => schema}, parent) do
    %{bad_raw_event(parent) | data: %{
      "line": "",
      "errors": Poison.encode!(["Unknown schema #{inspect(schema)}"])
    }}
  end

  def invalid_data(event, parent) do
    data = (event["data"] || %{}) |> Poison.encode!()
    %{bad_raw_event(parent) | data: %{
      "line": "",
      "errors": Poison.encode!(["Invalid data for " <> event["schema"] <> ": " <> data])
    }}
  end

  def bad_raw_event(parent) do
    %Snow.ETL.Schemas.Context{
      schema: %{
        "vendor": "com.snowplowanalytics.snowplow",
        "name": "bad_raw_event",
        "format": "jsonschema",
        "version": "1-0-0"
      },
      hierarchy: Snow.ETL.Shredder.Utils.hierarchy(parent, "bad_raw_event")
    }
  end
end
