defmodule Snow do
  def test(file) do
    file
    |> File.stream!()
    |> Zlib.inflate_stream()
    |> Snow.Parser.Cloudfront.exec()
    |> Snow.Enricher.UserAgent.exec()
    |> Snow.Enricher.URL.exec()
    |> Snow.Enricher.Marketing.exec()
    |> Snow.Enricher.Referer.exec()
    # |> Snow.Enricher.IPAddress.exec()

    # |> route(fn(key) ->
    #   Zlib.deflate_stream()
    #   |> HTTPStream.put(key)
    # end)
  end
end
