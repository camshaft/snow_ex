defmodule Snow.Enrich.Web.Browser do
  def exec(stream) do
    Stream.map(stream, &handle/1)
  end

  # fields = [
  #   :br_lang, :br_colordepth, :doc_size, :doc_charset, :br_viewport
  # ]

  # dimensions = [
  #   :doc_size, :br_viewport
  # ]

  defp handle(payload) do
    payload
    |> features()
  end

  fields = [
    :pdf, :quicktime, :realplayer,
    :windowsmedia, :director, :flash,
    :java, :gears, :silverlight
  ]

  field_nils = Enum.map(fields, &({:"br_features_#{&1}", nil}))
  field_vars = Enum.map(fields, &({:"br_features_#{&1}", Macro.var(&1, nil)}))
  field_short_vars = Enum.map(fields, &({to_string(&1), Macro.var(&1, nil)}))

  defp features(unquote({:%{}, [], [{:br_cookies, nil} | field_nils]}) = payload) do
    payload
  end
  defp features(unquote({:%{}, [], [{:br_cookies, Macro.var(:cookies, nil)} | field_vars]}) = payload) do
    Dict.put(payload, :derived_contexts, %Snow.Model.Context{
      parent: payload,
      schema: %{
        "vendor": "com.camshaft.snow.browser",
        "name": "features",
        "format": "jsonschema",
        "version": "1-0-0"
      },
      data: unquote({:%{}, [], [{"cookies", Macro.var(:cookies, nil)} | field_short_vars]})
    })
  end
end
