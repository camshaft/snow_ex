defmodule Snow.Enrich.Web.UserAgent do
  def exec(stream) do
    Stream.map(stream, &handle/1)
  end

  defp handle(%{useragent: useragent} = payload) when is_binary(useragent) do
    context = useragent
    |> UAInspector.parse_client()
    |> extract(payload)

    Dict.put(payload, :derived_contexts, [%Snow.Model.Context{
      parent: payload,
      schema: %{
        "vendor": "com.camshaft.snow.web",
        "name": "useragent",
        "format": "jsonschema",
        "version": "1-0-0"
      },
      data: %{
        "useragent" => useragent
      }
    } | context])
  end
  defp handle(payload) do
    payload
  end

  types = [client: [:engine, :name, :type, :version],
           device: [:brand, :model, :type],
           os: [:name, :platform, :version]]

  defp extract(info, payload) do
    unquote(Enum.reduce(types, [], fn({name, _}, acc) ->
      {:"extract_#{name}", [], [acc, Macro.var(:info, nil), Macro.var(:payload, nil)]}
    end))
  end

  for {name, params} <- types do
    fn_name = :"extract_#{name}"

    args_unknown = {:%{}, [], params |> Enum.map(&({&1, :unknown}))}
    args = {:%{}, [], params |> Enum.map(&({&1, Macro.var(&1, nil)}))}
    data = {:%{}, [], params |> Enum.map(&({to_string(&1), Macro.var(&1, nil)}))}

    defp unquote(fn_name)(acc, %{unquote(name) => unquote(args_unknown)}, _) do
      acc
    end
    defp unquote(fn_name)(acc, %{unquote(name) => unquote(args)}, payload) do
      [%Snow.Model.Context{
        parent: payload,
        schema: %{
          "vendor": "com.camshaft.snow.web.useragent",
          "name": unquote(to_string(name)),
          "format": "jsonschema",
          "version": "1-0-0"
        },
        data: unquote(data)
      } | acc]
    end
    defp unquote(fn_name)(acc, _, _) do
      acc
    end
  end
end
