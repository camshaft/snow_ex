defmodule Snow.ETL.Enricher.Tag do
  def exec(stream, tags) do
    stream
    |> Stream.map(&(handle(&1, tags)))
  end

  defp handle(%{etl_tags: tags} = model, [additional_tag]) do
    %{model | etl_tags: [additional_tag | tags]}
  end
  defp handle(%{etl_tags: tags} = model, additional_tags) when is_list(additional_tags) do
    %{model | etl_tags: tags ++ additional_tags}
  end
  defp handle(%{etl_tags: tags} = model, additional_tag) do
    %{model | etl_tags: [additional_tag | tags]}
  end
end
