defmodule Snow do
  def test(file) do
    file
    |> HTTPStream.get!()
    |> Zlib.gunzip_stream()
    |> Snow.Parser.Cloudfront.exec()
    |> Snow.Enricher.URL.exec()
    |> Snow.Enricher.Referer.exec()
    |> Snow.Enricher.Marketing.exec()
    |> Snow.Enricher.IPAddress.exec()
    |> Snow.Enricher.UserAgent.exec()
    # |> Snow.Enricher.Tag.exec(["event.filename"])
    |> Snow.Serializer.SnowplowRedshift.exec()
    |> Zlib.gzip_stream()
    |> Stream.into(File.stream!("event.gz"))
    |> Stream.run

    # |> route(fn(key) ->
    #   Zlib.deflate_stream()
    #   |> HTTPStream.put(key)
    # end)
  end
end
