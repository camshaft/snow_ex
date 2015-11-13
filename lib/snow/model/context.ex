defmodule Snow.Model.Context do
  defstruct schema: %{},
            data: %{},
            parent: nil
end

defimpl Poison.Encoder, for: Snow.Model.Context do
  def encode(context, options) do
    %{schema: schema, data: data, parent: parent} = context
    Poison.encode!(%{
      "schema" => schema,
      "data" => data,
      "hierarchy" => Snow.Enrich.Utils.hierarchy(parent, schema[:name] || schema["name"])
    }, options)
  end
end

defimpl Snow.Model, for: Snow.Model.Context do
  for size <- 1..6 do
    def name(%{schema: %{ vendor:    vendor,  name:    name,  version:    <<version :: binary-size(unquote(size)), "-", _ :: binary>>}}) do
      String.replace(vendor, ".", "_") <> "_" <> name <> "_" <> version
    end
    def name(%{schema: %{"vendor" => vendor, "name" => name, "version" => <<version :: binary-size(unquote(size)), "-", _ :: binary>>}}) do
      String.replace(vendor, ".", "_") <> "_" <> name <> "_" <> version
    end
  end
end
