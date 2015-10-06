defmodule SnowTest do
  use ExUnit.Case

  ## TODO build a stream "catch"

  test "" do
    # "http://s3.aws.com/foo/bar"
    # |> HTTPStream.get()
    # |> Zlib.inflate_stream()
    "../snow-lambda/test/parsers/sample.log"
    |> File.stream!()
    |> Snow.Parser.Cloudfront.exec()
    |> Snow.Enricher.UserAgent.exec()
    |> Snow.Enricher.URL.exec()
    |> Snow.Enricher.Marketing.exec()
    |> Snow.Enricher.Referer.exec()
    # |> Snow.Enricher.IPAddress.exec()
    # |> Snow.Enricher.Tag.exec([])
    # |> Snow.Shredder.Unstructured.exec()
    # |> Snow.Serializer.Snowplow.exec()
    # |> Zlib.deflate_stream()
    # |> HTTPStream.put("")
  end
end
