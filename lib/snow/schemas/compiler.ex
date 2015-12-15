defmodule Snow.Schemas.Compiler do
  defmacro __using__(opts) do
    parts = Enum.map(opts[:schemas], &format/1)

    quote do
      def shred(%{"schema" => schema} = event, parent) do
        import Snow.Model.BadRawEvent
        case fetch(schema) do
          nil ->
            [unknown_schema(event, parent)]
          config = %{"self" => self} ->
            if validate(config, event) do
              %Snow.Model.Context{
                schema: self,
                parent: parent,
                data: event["data"] || %{}
              }
              |> Snow.Enrich.Utils.explode()
            else
              [invalid_data(event, parent)]
            end
        end
      end

      unquote(parts)
      defp fetch(_), do: nil
      defp validate(_, _), do: false
    end
  end

  defp format(schema = %{"self" => %{"vendor" => vendor,
                                     "name" => name,
                                     "format" => format,
                                     "version" => version} = self,
                         "properties" => properties}) do

    short = "#{vendor}/#{name}/#{format}/#{version}"
    short_w_iglu = "iglu:#{vendor}/#{name}/#{format}/#{version}"
    formats = [short, short_w_iglu]

    {required, checks} = properties |> Map.to_list |> format_checks([], [])

    quote do
      defp fetch(s) when s in unquote(formats) do
        unquote(Macro.escape(schema))
      end

      defp validate(%{"self" => unquote(Macro.escape(self))},
                    %{"data" => data = unquote(required)}) do
        Enum.all?(data, unquote(checks))
      end
    end
  end

  defp format_checks([], required, checks) do
    {{:%{}, [], required}, {:fn, [], checks ++ quote do
      (_) ->
        false
    end}}
  end
  defp format_checks([{key, %{"type" => type} = config} | properties], required, checks) when is_binary(type) do
    format_checks([{key, %{config | "type" => [type]}} | properties], required, checks)
  end
  defp format_checks([{key, %{"type" => types}} | properties], required, checks) when is_list(types) do
    required = if !("null" in types || nil in types) do
      [required(key) | required]
    end || required

    format_checks(properties, required, checks ++ quote do
      ({unquote(key), unquote(to_var(key))}) when unquote(types_to_check(types, key)) ->
        true
    end)
  end

  defp format_checks([{key, %{"enum" => enum}} | properties], required, checks) when is_list(enum) do
    var = to_var(key)
    format_checks(properties, [required(key) | required], checks ++ quote do
      ({unquote(key), unquote(var)}) when unquote(var) in unquote(enum) ->
        true
    end)
  end

  defp required(key) do
    {key, Macro.var(:_, nil)}
  end

  defp types_to_check(types, key) do
    key = key |> to_var()
    types
    |> Enum.map(&(type_to_check(&1, key)))
    |> fn_join(:or)
  end

  defp type_to_check("array", key), do: {:is_list, [], [key]}
  defp type_to_check("object", key), do: {:is_map, [], [key]}
  defp type_to_check("string", key), do: {:is_binary, [], [key]}
  defp type_to_check("integer", key), do: {:is_integer, [], [key]}
  defp type_to_check("boolean", key), do: {:is_boolean, [], [key]}
  defp type_to_check("float", key), do: {:is_float, [], [key]}
  defp type_to_check("double", key), do: {:is_float, [], [key]}
  defp type_to_check("number", key), do: {:or, [], [type_to_check("integer", key), type_to_check("float", key)]}
  defp type_to_check("null", key), do: {:is_nil, [], [key]}
  defp type_to_check(nil, key), do: {:is_nil, [], [key]}

  defp fn_join([], _) do
    true
  end
  defp fn_join([val], _) do
    val
  end
  defp fn_join([val | rest], fun) do
    {fun, [], [val, fn_join(rest, fun)]}
  end

  defp to_var(name) when is_binary(name) do
    name |> String.to_atom() |> to_var
  end
  defp to_var(name) when is_atom(name) do
    name |> Macro.var(nil)
  end
end
