defmodule Snow.Enrich.Web.IPAddress do
  def exec(stream) do
    stream
    |> Stream.map(&handle/1)
  end

  defp handle(%{user_ipaddress: ip} = payload) when not is_nil(ip) do
    context = ip
    |> Geolix.lookup(locale: :en)
    |> extract(payload)

    Dict.put(payload, :derived_contexts, [%Snow.Model.Context{
      parent: payload,
      schema: %{
        "vendor": "com.camshaft.snow.web",
        "name": "ip_address",
        "format": "jsonschema",
        "version": "1-0-0"
      },
      data: %{
        "ip_address" => ip
      }
    } | context])
  end
  defp handle(payload) do
    payload
  end

  types = [city: [:name, :geoname_id],
           continent: [:name, :geoname_id, :code],
           country: [:name, :geoname_id, :iso_code],
           location: [:latitude, :longitude, :metro_code, :time_zone],
           postal: [:code],
           subdivisions: [:name, :geoname_id, :iso_code]]

  defp extract(info, payload) do
    unquote(Enum.reduce(types, [], fn({name, _}, acc) ->
      {:"extract_#{name}", [], [acc, Macro.var(:info, nil), Macro.var(:payload, nil)]}
    end))
  end

  for {name, params} <- types do
    fn_name = :"extract_#{name}"

    args = {:%{}, [], params |> Enum.map(&({&1, Macro.var(&1, nil)}))}
    data = {:%{}, [], params |> Enum.map(&({to_string(&1), Macro.var(&1, nil)}))}

    for section <- [:city, :country] do
      defp unquote(fn_name)(acc, %{unquote(section) => %{unquote(name) => results}}, payload) when is_list(results) do
        Enum.reduce(results, acc, fn(result, acc) ->
          unquote(fn_name)(acc, %{unquote(section) => %{unquote(name) => result}}, payload)
        end)
      end
      defp unquote(fn_name)(acc, %{unquote(section) => %{unquote(name) => unquote(args)}}, payload) do
        [%Snow.Model.Context{
          parent: payload,
          schema: %{
            "vendor": "com.camshaft.snow.web.geo",
            "name": unquote(to_string(name)),
            "format": "jsonschema",
            "version": "1-0-0"
          },
          data: unquote(data)
        } | acc]
      end
    end
    defp unquote(fn_name)(acc, _, _) do
      acc
    end
  end
end
