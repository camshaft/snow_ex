defmodule Snow.Model.Context do
  defstruct schema: %{},
            data: %{},
            parent: nil

  defmacro __using__(_) do
    quote do
      import unquote(__MODULE__)
      Module.register_attribute __MODULE__, :snow_contexts, accumulate: true

      @before_compile unquote(__MODULE__)
    end
  end

  defmacro __before_compile__(_) do
    contexts = Module.get_attribute(__CALLER__.module, :snow_contexts)

    quote do
      def contexts do
        unquote(Macro.escape(contexts))
      end
    end
  end

  defmacro put_context(payload, schema, data) do
    quote do
      Dict.put(unquote(payload), :derived_contexts, create_context(unquote(payload), unquote(schema), unquote(data)))
    end
  end

  defmacro create_context(parent, schema, data) do
    schema = schema
    |> Macro.expand(__CALLER__)
    |> put_defaults([version: "1-0-0", format: "jsonschema"])
    |> Dict.put(:vendor, format_vendor(schema))
    |> keys_to_string()
    |> values_to_string()
    |> :maps.from_list()

    {data, properties} = data
    |> Macro.expand(__CALLER__)
    |> keys_to_string()
    |> extract_properties()

    mod = __CALLER__.module
    Module.register_attribute mod, :snow_contexts, accumulate: true
    Module.put_attribute(mod, :snow_contexts, %{
      "$schema" => "http://iglucentral.com/schemas/com.snowplowanalytics.self-desc/schema/jsonschema/1-0-0#",
      "description" => schema["description"],
      "self" => %{
        "vendor" => schema["vendor"],
        "name" => schema["name"],
        "format" => schema["format"],
        "version" => schema["version"]
      },
      "type" => "object",
      "properties" => :maps.from_list(properties)
    })

    quote do
      %Snow.Model.Context{
        schema: unquote(Macro.escape(schema)),
        data: %{unquote_splicing(data)},
        parent: unquote(parent)
      }
    end
  end

  defp keys_to_string(keys) do
    keys
    |> Enum.map(fn {key, value} ->
      {to_string(key), value}
    end)
  end

  defp values_to_string(keys) do
    keys
    |> Enum.map(fn {key, value} ->
      {key, to_string(value)}
    end)
  end

  defp extract_properties(data) do
    data
    |> Enum.map_reduce([], fn
      ({key, {:::, _, [value, types]}}, acc) ->
        {{key, value}, [{key, %{"type" => extract_types(types)}} | acc]}
      ({key, value}, acc) ->
        {{key, value}, [{key, %{"type" => extract_types(:string)}} | acc]}
    end)
  end

  defp extract_types(types) when is_list(types) do
    (types |> Enum.map(&extract_type/1)) ++ ["null"] |> Enum.uniq()
  end
  defp extract_types(type) do
    [type] |> extract_types()
  end

  defp extract_type({type, _, _}), do: to_string(type)
  defp extract_type(type), do: to_string(type)

  defp put_defaults(obj, defaults) do
    Enum.reduce(defaults, obj, fn({k, v}, acc) ->
      Dict.put_new(acc, k, v)
    end)
  end

  defp format_vendor(schema) do
    case Dict.get(schema, :vendor) do
      nil ->
        "com.camshaft.snow"
      vendor ->
        "com.camshaft.snow.#{vendor}"
    end
  end
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
