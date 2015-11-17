defmodule Snow.Enrich.Web.Browser do
  use Snow.Model.Context

  def exec(stream) do
    Stream.map(stream, &handle/1)
  end

  attributes = [
    br_lang: {:string, :language},
    br_colordepth: {:integer, :colordepth},
    br_viewport: {:integer, :viewport_width, :viewport_height},
    user_fingerprint: {:integer, :fingerprint}
  ]

  attributes_nils = Enum.map(attributes, fn
    ({name, {_, _, _}}) -> {name, {nil, nil}}
    ({name, _}) -> {name, nil}
  end)
  attributes_vars = Enum.map(attributes, fn
    ({name, {_, w, h}}) -> {name, {Macro.var(w, nil), Macro.var(h, nil)}}
    ({name, {_, var}}) -> {name, Macro.var(var, nil)}
  end)
  attributes_mappings = Enum.reduce(attributes, [], fn
    ({_, {type, w, h}}, acc) -> [{w, {:::, [], [Macro.var(w, nil), type]}},
                                 {h, {:::, [], [Macro.var(h, nil), type]}} | acc]
    ({_, {type, var}}, acc) -> [{var, {:::, [], [Macro.var(var, nil), type]}} | acc]
  end)

  features = [
    :cookies, :pdf, :quicktime, :realplayer,
    :windowsmedia, :director, :flash,
    :java, :gears, :silverlight
  ]

  features_nils = Enum.map(features, &({:"br_features_#{&1}", nil}))
  features_vars = Enum.map(features, &({:"br_features_#{&1}", Macro.var(&1, nil)}))
  features_mappings = Enum.map(features, &({:"feature_#{&1}", {:::, [], [Macro.var(&1, nil), :boolean]}}))

  defp handle(unquote({:%{}, [], features_nils ++ attributes_nils}) = payload) do
    payload
  end
  defp handle(unquote({:%{}, [], features_vars ++ attributes_vars}) = payload) do
    put_context(payload,
      [vendor: "web", name: "browser"],
      unquote(attributes_mappings ++ features_mappings))
  end
end
