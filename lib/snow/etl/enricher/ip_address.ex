defmodule Snow.ETL.Enricher.IPAddress do
  def exec(stream) do
    stream
    |> Stream.map(&handle/1)
  end

  defp handle(%{user_ipaddress: ip} = model) when is_binary(ip) do
    ip
    |> Geolix.lookup()
    |> extract(model)
  end
  defp handle(model) do
    model
  end

  defp extract(nil, model) do
    model
  end
  defp extract(info, model) do
    model
    |> extract_country(info)
    |> extract_region(info)
    |> extract_region_name(info)
    |> extract_city(info)
    |> extract_zipcode(info)
    |> extract_location(info)
  end

  for section <- [:city, :country] do
    defp extract_country(model, %{unquote(section) => %{country: %{iso_code: iso_code}}}) do
      %{model | geo_country: iso_code}
    end
    defp extract_country(model, %{unquote(section) => %{registered_country: %{iso_code: iso_code}}}) do
      %{model | geo_country: iso_code}
    end

    ## TODO handle other regions that don't have iso_codes
    defp extract_region(model, %{unquote(section) => %{subdivisions: [%{iso_code: iso_code} | _]}}) when is_binary(iso_code) do
      %{model | geo_region: iso_code}
    end

    ## TODO handle other regions that don't have names
    defp extract_region_name(model, %{unquote(section) => %{subdivisions: [%{name: name} | _]}}) when is_binary(name) do
      %{model | geo_region_name: name}
    end
    defp extract_region_name(model, %{unquote(section) => %{subdivisions: [%{names: %{en: name}} | _]}}) when is_binary(name) do
      %{model | geo_region_name: name}
    end

    defp extract_city(model, %{unquote(section) => %{city: %{name: name}}}) when is_binary(name) do
      %{model | geo_city: name}
    end
    defp extract_city(model, %{unquote(section) => %{city: %{names: %{en: name}}}}) when is_binary(name) do
      %{model | geo_city: name}
    end

    defp extract_zipcode(model, %{unquote(section) => %{postal: %{code: code}}}) do
      %{model | geo_zipcode: code}
    end

    defp extract_location(model, %{unquote(section) => %{location: %{latitude: latitude, longitude: longitude, time_zone: time_zone}}}) do
      %{model | geo_latitude: latitude, geo_longitude: longitude, geo_timezone: time_zone}
    end
  end

  defp extract_country(model, _) do
    model
  end
  defp extract_region(model, _) do
    model
  end
  defp extract_region_name(model, _) do
    model
  end
  defp extract_city(model, _) do
    model
  end
  defp extract_zipcode(model, _) do
    model
  end
  defp extract_location(model, _) do
    model
  end
end
