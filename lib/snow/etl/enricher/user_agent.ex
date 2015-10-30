defmodule Snow.ETL.Enricher.UserAgent do
  def exec(stream) do
    stream
    |> Stream.map(&handle/1)
  end

  defp handle(%{useragent: useragent} = model) when is_binary(useragent) do
    useragent
    |> UAInspector.parse_client()
    |> extract(model)
  end
  defp handle(model) do
    model
  end

  defp extract(info, model) do
    model
    |> extract_os(info)
    |> extract_client(info)
    |> extract_device(info)
  end

  defp extract_os(model, %{os: %{name: name, version: version}}) do
    %{model | os_name: format_name(name, version),
              os_family: name}
  end
  defp extract_os(model, _) do
    model
  end

  defp extract_client(model, %{client: %{engine: engine, name: name, type: type, version: version}}) do
    %{model | br_name: format_name(name, version),
              br_version: version,
              br_family: name,
              br_type: type,
              br_renderengine: engine}
  end
  defp extract_client(model, _) do
    model
  end

  def format_name(nil, _), do: nil
  def format_name(name, nil), do: name
  def format_name(name, <<version :: size(1)-binary, ".", _ :: binary>>), do: "#{name} #{version}"
  def format_name(name, <<version :: size(2)-binary, ".", _ :: binary>>), do: "#{name} #{version}"
  def format_name(name, <<version :: size(3)-binary, ".", _ :: binary>>), do: "#{name} #{version}"
  def format_name(name, version), do: "#{name} #{version}"

  defp extract_device(model, %{device: %{type: "smartphone"}}) do
    %{model | dvce_type: "smartphone", dvce_ismobile: 1}
  end
  defp extract_device(model, %{device: %{type: :unknown}}) do
    model
  end
  defp extract_device(model, %{device: %{type: type}}) do
    %{model | dvce_type: type}
  end
  defp extract_device(model, _) do
    model
  end
end
