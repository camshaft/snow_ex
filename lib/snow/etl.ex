defmodule Snow.ETL do
  def test(file) do
    file
    |> from_file()
    |> cloudfront()
    |> standard()
    |> to_json()
    |> into(&(File.stream!(&1 <> ".gz")))
  end

  def from_file(path) do
    path
    |> File.stream!()
  end

  def cloudfront(stream) do
    stream
    |> Snow.ETL.Parser.Cloudfront.exec()
  end

  def standard(stream, schemas \\ %{}, tags \\ nil) do
    stream
    |> enrich()
    |> tag(tags)
    |> shred(schemas)
  end

  def enrich(stream) do
    stream
    |> Snow.ETL.Enricher.URL.exec()
    |> Snow.ETL.Enricher.Referer.exec()
    |> Snow.ETL.Enricher.Marketing.exec()
    |> Snow.ETL.Enricher.IPAddress.exec()
    |> Snow.ETL.Enricher.UserAgent.exec()
  end

  def shred(stream, schemas) do
    stream
    |> Snow.ETL.Shredder.Context.exec(schemas)
    |> Snow.ETL.Shredder.Unstructured.exec(schemas)
  end

  def to_json(stream) do
    stream
    |> Snow.ETL.Serializer.JSON.exec()
  end

  def tag(stream, tags) do
    stream
    |> Snow.ETL.Enricher.Tag.exec(tags)
  end

  def into(stream, factory) do
    stream
    |> Nile.route_into(&(&1), factory)
  end
end
