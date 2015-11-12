defmodule Snow.ETL.Schemas.Compiler do
  def define(schemas, mod) do
    parts = Enum.map(schemas, &format/1)

    quote do
      defmodule unquote(mod) do
        def shred(%{"schema" => schema} = event, parent) do
          import Snow.ETL.Schemas.BadRawEvent
          case fetch(schema) do
            nil ->
              [unknown_schema(event, parent)]
            config = %{"self" => self} ->
              if validate(config, event) do
                %Snow.ETL.Schemas.Context{
                  schema: self,
                  hierarchy: Snow.ETL.Shredder.Utils.hierarchy(parent, self["name"]),
                  data: event["data"] || %{}
                }
                |> Snow.ETL.Shredder.Utils.explode()
              else
                [invalid_data(event, parent)]
              end
          end
        end

        unquote(parts)
        def fetch(_), do: nil
        def validate(_, _), do: false
      end
    end
    |> Snow.Utils.eval_quoted()
  end

  defp format(schema = %{"self" => %{"vendor" => vendor,
                                     "name" => name,
                                     "format" => format,
                                     "version" => version} = self,
                         "properties" => properties}) do

    short = "#{vendor}/#{name}/#{format}/#{version}"
    short_w_iglu = "iglu:#{vendor}/#{name}/#{format}/#{version}"
    formats = [short, short_w_iglu]

    data_keys = {:%{}, [], properties
    |> Map.keys()
    |> Enum.map(&({&1, to_var(&1)}))}

    data_checks = properties
    |> Enum.map(&format_checks/1)
    |> fn_join(:and)

    quote do
      def fetch(s) when s in unquote(formats) do
        unquote(Macro.escape(schema))
      end

      def validate(%{"self" => unquote(Macro.escape(self))},
                   %{"data" => unquote(data_keys)}) when unquote(data_checks) do
        true
      end
    end
  end

  defp format_checks({key, %{"type" => type}}) when is_binary(type) do
    format_checks(key, [type])
  end
  defp format_checks({key, %{"type" => types}}) when is_list(types) do
    format_checks(key, types)
  end
  defp format_checks({key, %{"enum" => enum}}) when is_list(enum) do
    quote do
      unquote(key |> to_var()) in unquote(enum)
    end
  end
  defp format_checks(key, types) do
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
