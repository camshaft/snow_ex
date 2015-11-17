defmodule Snow.Payload do

  platforms =      [web: :web,
                    mob: :mobile,
                    pc: :desktop,
                    srv: :server,
                    app: :application,
                    tv: :television,
                    cnsl: :console,
                    iot: :internet_of_things]

  events =         [pv: :page_view,
                    pp: :page_ping,
                    tr: :transaction,
                    ti: :transaction_item,
                    se: :structured_event,
                    ue: :unstructured_event]

  #https://github.com/snowplow/snowplow/wiki/snowplow-tracker-protocol#1-common-parameters-platform-and-event-independent
  common =         [tna: {:name_tracker, :text},
                    aid: {:app_id, :text},
                    p: {:platform, platforms}]

  #https://github.com/snowplow/snowplow/wiki/snowplow-tracker-protocol#12-date--time-parameter
  datetime =       [dtm: {:dvce_created_tstamp, :integer},
                    stm: {:dvce_sent_tstamp, :integer},
                    ttm: {:true_tstamp, :integer},
                    tz: {:os_timezone, :text}]

  #https://github.com/snowplow/snowplow/wiki/snowplow-tracker-protocol#13-event--transaction-parameters
  events =         [e: {:event, events},
                    eid: {:event_id, :text}]

  #https://github.com/snowplow/snowplow/wiki/snowplow-tracker-protocol#14-snowplow-tracker-version
  tracker =        [tv: {:v_tracker, :text}]

  #https://github.com/snowplow/snowplow/wiki/snowplow-tracker-protocol#15-user-related-parameters
  user =           [duid: {:domain_userid, :text},
                    nuid: {:network_userid, :text},
                    tnuid: {:network_userid, :text},
                    uid: {:user_id, :text},
                    vid: {:domain_sessionidx, :integer},
                    sid: {:domain_sessionid, :text},
                    ip: {:user_ipaddress, :text}]

  #https://github.com/snowplow/snowplow/wiki/snowplow-tracker-protocol#16-device-related-properties
  device =         [res: {:device_resolution, :dimension}]

  #https://github.com/snowplow/snowplow/wiki/snowplow-tracker-protocol#21-web-specific-parameters
  web =            [url: {:page_url, :url},
                    ua: {:useragent, :text},
                    page: {:page_title, :text},
                    refr: {:page_referrer, :url},
                    fp: {:user_fingerprint, :integer},
                    ctype: {:connection_type, :text},
                    cookie: {:br_features_cookies, :boolean}, # note: we use br_features_cookies instead of br_cookies
                    lang: {:br_lang, :text},
                    f_pdf: {:br_features_pdf, :boolean},
                    f_qt: {:br_features_quicktime, :boolean},
                    f_realp: {:br_features_realplayer, :boolean},
                    f_wma: {:br_features_windowsmedia, :boolean},
                    f_dir: {:br_features_director, :boolean},
                    f_fla: {:br_features_flash, :boolean},
                    f_java: {:br_features_java, :boolean},
                    f_gears: {:br_features_gears, :boolean},
                    f_ag: {:br_features_silverlight, :boolean},
                    cd: {:br_colordepth, :integer},
                    ds: {:doc_size, :dimension},
                    cs: {:doc_charset, :text},
                    vp: {:br_viewport, :dimension}]

  #https://github.com/snowplow/snowplow/wiki/snowplow-tracker-protocol#22-internet-of-things-specific-parameters
  iot =            [mac: {:mac_address, :text}]

  #https://github.com/snowplow/snowplow/wiki/snowplow-tracker-protocol#32-page-pings
  page_pings =     [pp_mix: {:pp_xoffset_min, :integer},
                    pp_max: {:pp_xoffset_max, :integer},
                    pp_miy: {:pp_yoffset_min, :integer},
                    pp_may: {:pp_yoffset_max, :integer}]

  #https://github.com/snowplow/snowplow/wiki/snowplow-tracker-protocol#34-ad-impression-tracking
  ads =            [ad_ba: {:adi_bannerid, :text},
                    ad_ca: {:adi_campaignid, :text},
                    ad_ad: {:adi_advertiserid, :text},
                    ad_uid: {:adi_userid, :text}]

  #https://github.com/snowplow/snowplow/wiki/snowplow-tracker-protocol#35-ecommerce-tracking
  ecommerce_txn =  [tr_id: {:tr_orderid, :text},
                    tr_af: {:tr_affiliation, :text},
                    tr_tt: {:tr_total, :float},
                    tr_tx: {:tr_tax, :float},
                    tr_sh: {:tr_shipping, :float},
                    tr_ci: {:tr_city, :text},
                    tr_st: {:tr_state, :text},
                    tr_co: {:tr_country, :text},
                    tr_cu: {:tr_currency, :text}]

  #https://github.com/snowplow/snowplow/wiki/snowplow-tracker-protocol#352-transaction-item-parameters
  ecommerce_item = [ti_id: {:ti_orderid, :text},
                    ti_sk: {:ti_sku, :text},
                    ti_na: {:ti_name, :text},
                    ti_ca: {:ti_category, :text},
                    ti_pr: {:ti_price, :float},
                    ti_qu: {:ti_quantity, :integer},
                    ti_cu: {:ti_currency, :text}]

  #https://github.com/snowplow/snowplow/wiki/snowplow-tracker-protocol#36-social-tracking
  social =         [sa: {:social_action, :text},
                    sn: {:social_network, :text},
                    st: {:social_target, :text},
                    sp: {:social_pagepath, :text}]

  #https://github.com/snowplow/snowplow/wiki/snowplow-tracker-protocol#39-custom-structured-event-tracking
  structured =     [se_ca: {:se_category, :text},
                    se_ac: {:se_action, :text},
                    se_la: {:se_label, :text},
                    se_pr: {:se_property, :text},
                    se_va: {:se_value, :float}]

  #https://github.com/snowplow/snowplow/wiki/snowplow-tracker-protocol#310-custom-unstructured-event-tracking
  unstructured =   [ue_pr: {:unstruct_event, :json},
                    ue_px: {:unstruct_event, :base64_json}]

  #https://github.com/snowplow/snowplow/wiki/snowplow-tracker-protocol#4-custom-contexts
  contexts =       [co: {:context, :json},
                    cx: {:context, :base64_json}]

  collector =      ["$cv": {:v_collector, :text},
                    "$ct": {:collector_tstamp, :integer}]

  mappings =       [common,
                    datetime,
                    events,
                    tracker,
                    user,
                    device,
                    web,
                    iot,
                    page_pings,
                    ads,
                    ecommerce_txn,
                    ecommerce_item,
                    social,
                    structured,
                    unstructured,
                    contexts,
                    collector] |> Enum.concat

  def mappings() do
    unquote(mappings)
  end

  def platforms() do
    unquote(platforms)
  end

  def events() do
    unquote(events)
  end

  defaults = %{dimension: {nil, nil}}

  fields = mappings |> Enum.map(fn({_, {n, t}}) -> {n, Dict.get(defaults, t)} end) |> Enum.uniq()

  defstruct fields ++ [derived_contexts: [],
                       atomic_event: nil]

  def from_qs(qs) do
    qs
    |> URI.query_decoder()
    |> from_dict()
  end

  def from_dict(dict) do
    dict
    |> Enum.reduce(%__MODULE__{}, &map/2)
    |> Snow.Model.Event.from_payload()
  end

  def derive(stream, shred) do
    Stream.map(stream, fn(payload) ->
      put(payload, :derived_contexts, shred.(payload))
    end)
  end

  def derived_contexts(stream) do
    Nile.expand(stream, fn(%__MODULE__{atomic_event: event, derived_contexts: contexts}) ->
      [event | contexts]
    end)
  end

  for {from, {to, type}} <- mappings do
    from_s = to_string(from)
    case type do
      :text ->
        defp map({unquote(from_s), value}, payload) do
          if String.printable?(value) do
            %{payload | unquote(to) => value}
          else
            payload
          end
        end
      :integer ->
        defp map({unquote(from_s), value}, payload) when is_binary(value) do
          %{payload | unquote(to) => String.to_integer(value)}
        rescue
          ArgumentError ->
            payload
        end
        defp map({unquote(from_s), value}, payload) when is_integer(value) do
          %{payload | unquote(to) => value}
        end
      :float ->
        defp map({unquote(from_s), value}, payload) when is_binary(value) do
          %{payload | unquote(to) => String.to_float(value)}
        rescue
          ArgumentError ->
            payload
        end
        defp map({unquote(from_s), value}, payload) when is_float(value) do
          %{payload | unquote(to) => value}
        end
      :url ->
        defp map({unquote(from_s), value}, payload) do
          if String.printable?(value) do
            %{payload | unquote(to) => URI.parse(value)}
          else
            payload
          end
        end
      :boolean ->
        defp map({unquote(from_s), value}, payload) when value in ["1", "true"] do
          %{payload | unquote(to) => true}
        end
        defp map({unquote(from_s), value}, payload) when value in ["0", "false"] do
          %{payload | unquote(to) => false}
        end
      :dimension ->
        for ws <- 1..5, hs <- 1..5 do
          defp map({unquote(from_s), <<w :: size(unquote(ws))-binary, "x", h :: size(unquote(hs))-binary>>}, payload) do
            %{payload | unquote(to) => {String.to_integer(w), String.to_integer(h)}}
          end
        end
      :json ->
        defp map({unquote(from_s), json}, payload) do
          %{payload | unquote(to) => decode_json(json)}
        rescue
          Poison.SyntaxError ->
            put(payload, :derived_contexts, Snow.Model.BadRawEvent.syntax_error(json, payload))
        end
      :base64_json ->
        defp map({unquote(from_s), value}, payload) do
          %{payload | unquote(to) => value |> decode_base64() |> decode_json()}
        rescue
          Poison.SyntaxError ->
            put(payload, :derived_contexts, Snow.Model.BadRawEvent.syntax_error(decode_base64(value), payload))
        end
      _ when is_list(type) ->
        for {f, t} <- type do
          defp map({unquote(from_s), unquote(to_string(f))}, payload) do
            %{payload | unquote(to) => unquote(t)}
          end
        end
    end
  end

  defp map(_, payload) do
    payload
  end

  defp decode_json(value) do
    value |> Poison.decode!()
  end

  defp decode_base64(value) do
    (case byte_size(value) |> rem(4) do
      0 -> value
      1 -> value <> "==="
      2 -> value <> "=="
      3 -> value <> "="
    end)
    |> Base.url_decode64!()
  end

  use Dict

  def delete(payload, key) do
    Map.put(payload, key, nil)
  end

  def fetch(payload, key) do
    Map.fetch(payload, key)
  end

  def new do
    %__MODULE__{}
  end

  def put(payload, :derived_contexts, value) when value in [[], nil] do
    payload
  end
  def put(payload = %{derived_contexts: derived_contexts}, :derived_contexts, contexts) when is_list(contexts) do
    %{payload | derived_contexts: derived_contexts ++ contexts}
  end
  def put(payload, :derived_contexts, contexts) do
    put(payload, :derived_contexts, [contexts])
  end
  def put(payload, key, value) when key in unquote(Dict.keys(fields)) do
    Map.put(payload, key, value)
  end

  def put_event(payload = %{atomic_event: atomic_event}, key, value) do
    %{payload | atomic_event: Dict.put(atomic_event, key, value)}
  end

  def size(_) do
    unquote(length(fields))
  end

  def reduce(payload, acc, fun) do
    payload
    |> Map.delete(:__struct__)
    |> Enumerable.Map.reduce(acc, fun)
  end
end
