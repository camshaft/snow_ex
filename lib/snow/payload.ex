defmodule Snow.Payload do

  #https://github.com/snowplow/snowplow/wiki/snowplow-tracker-protocol#1-common-parameters-platform-and-event-independent
  common =         [tna: :name_tracker,
                    aid: :app_id,
                    p: :platform]

  #https://github.com/snowplow/snowplow/wiki/snowplow-tracker-protocol#12-date--time-parameter
  datetime =       [dtm: :dvce_created_tstamp,
                    stm: :dvce_sent_tstamp,
                    ttm: :true_tstamp,
                    tz: :os_timezone]

  #https://github.com/snowplow/snowplow/wiki/snowplow-tracker-protocol#13-event--transaction-parameters
  events =         [e: :event,
                    eid: :event_id]

  #https://github.com/snowplow/snowplow/wiki/snowplow-tracker-protocol#14-snowplow-tracker-version
  tracker =        [tv: :v_tracker]

  #https://github.com/snowplow/snowplow/wiki/snowplow-tracker-protocol#15-user-related-parameters
  user =           [duid: :domain_userid,
                    nuid: :network_userid,
                    tnuid: :network_userid,
                    uid: :user_id,
                    vid: :domain_sessionidx,
                    sid: :domain_sessionid,
                    ip: :user_ipaddress]

  #https://github.com/snowplow/snowplow/wiki/snowplow-tracker-protocol#16-device-related-properties
  device =         [res: :device_resolution]

  #https://github.com/snowplow/snowplow/wiki/snowplow-tracker-protocol#21-web-specific-parameters
  web =            [url: :page_url,
                    ua: :useragent,
                    page: :page_title,
                    refr: :page_referrer,
                    fp: :user_fingerprint,
                    ctype: :connection_type,
                    cookie: :br_cookies,
                    lang: :br_lang,
                    f_pdf: :br_features_pdf,
                    f_qt: :br_features_quicktime,
                    f_realp: :br_features_realplayer,
                    f_wma: :br_features_windowsmedia,
                    f_dir: :br_features_director,
                    f_fla: :br_features_flash,
                    f_java: :br_features_java,
                    f_gears: :br_features_gears,
                    f_ag: :br_features_silverlight,
                    cd: :br_colordepth,
                    ds: :doc_size,
                    cs: :doc_charset,
                    vp: :br_viewport]

  #https://github.com/snowplow/snowplow/wiki/snowplow-tracker-protocol#22-internet-of-things-specific-parameters
  iot =            [mac: :mac_address]

  #https://github.com/snowplow/snowplow/wiki/snowplow-tracker-protocol#32-page-pings
  page_pings =     [pp_mix: :pp_xoffset_min,
                    pp_max: :pp_xoffset_max,
                    pp_miy: :pp_yoffset_min,
                    pp_may: :pp_yoffset_max]

  #https://github.com/snowplow/snowplow/wiki/snowplow-tracker-protocol#34-ad-impression-tracking
  ads =            [ad_ba: :adi_bannerid,
                    ad_ca: :adi_campaignid,
                    ad_ad: :adi_advertiserid,
                    ad_uid: :adi_userid]

  #https://github.com/snowplow/snowplow/wiki/snowplow-tracker-protocol#35-ecommerce-tracking
  ecommerce_txn =  [tr_id: :tr_orderid,
                    tr_af: :tr_affiliation,
                    tr_tt: :tr_total,
                    tr_tx: :tr_tax,
                    tr_sh: :tr_shipping,
                    tr_ci: :tr_city,
                    tr_st: :tr_state,
                    tr_co: :tr_country,
                    tr_cu: :tr_currency]

  #https://github.com/snowplow/snowplow/wiki/snowplow-tracker-protocol#352-transaction-item-parameters
  ecommerce_item = [ti_id: :ti_orderid,
                    ti_sk: :ti_sku,
                    ti_na: :ti_name,
                    ti_ca: :ti_category,
                    ti_pr: :ti_price,
                    ti_qu: :ti_quantity,
                    ti_cu: :ti_currency]

  #https://github.com/snowplow/snowplow/wiki/snowplow-tracker-protocol#36-social-tracking
  social =         [sa: :social_action,
                    sn: :social_network,
                    st: :social_target,
                    sp: :social_pagepath]

  #https://github.com/snowplow/snowplow/wiki/snowplow-tracker-protocol#39-custom-structured-event-tracking
  structured =     [se_ca: :se_category,
                    se_ac: :se_action,
                    se_la: :se_label,
                    se_pr: :se_property,
                    se_va: :se_value]

  #https://github.com/snowplow/snowplow/wiki/snowplow-tracker-protocol#310-custom-unstructured-event-tracking
  unstructured =   [ue_pr: :unstruct_event,
                    ua_px: :unstruct_event]

  #https://github.com/snowplow/snowplow/wiki/snowplow-tracker-protocol#4-custom-contexts
  contexts =       [co: :context,
                    cx: :context]

  collector =      ["$cv": :v_collector,
                    "$ct": :collector_tstamp]

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

  platforms =      [web: :web,
                    mob: :mobile,
                    pc: :desktop,
                    srv: :server,
                    app: :application,
                    tv: :television,
                    cnsl: :console,
                    iot: :internet_of_things]

  def platforms() do
    unquote(platforms)
  end

  events =         [pv: :page_view,
                    pp: :page_ping,
                    tr: :transaction,
                    ti: :transaction_item,
                    se: :structured_event,
                    ue: :unstructured_event]

  def events() do
    unquote(events)
  end

  base64 =         [ua_px: :unstruct_event,
                    cx: :context]

  fields = mappings |> Keyword.values() |> Enum.uniq()

  defstruct Enum.map(fields, &({&1, nil})) ++ [derived_contexts: [],
                                               atomic_event: nil]

  def from_qs(qs) do
    qs
    |> URI.query_decoder()
    |> from_stream()
  end

  def from_stream(stream) do
    stream
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

  for {from, to} <- mappings do
    cond do
      from in [:p, :e | Keyword.keys(base64)] ->
        nil
      from in [:url, :refr] ->
        defp map({unquote(to_string(from)), value}, payload) do
          %{payload | unquote(to) => URI.parse(value)}
        end
      true ->
        defp map({unquote(to_string(from)), value}, payload) do
          %{payload | unquote(to) => value}
        end
    end
  end

  for {from, to} <- platforms do
    defp map({"p", unquote(to_string(from))}, payload) do
      %{payload | platform: unquote(to)}
    end
  end

  for {from, to} <- events do
    defp map({"e", unquote(to_string(from))}, payload) do
      %{payload | event: unquote(to)}
    end
  end

  for {from, to} <- base64 do
    defp map({unquote(to_string(from)), value}, acc) do
      value = case byte_size(value) |> rem(4) do
        0 -> value
        1 -> value <> "==="
        2 -> value <> "=="
        3 -> value <> "="
      end
      %{acc | unquote(to) => Base.url_decode64!(value)}
    end
  end

  for {from, {width, height}} <- dimensions do
    for ws <- 1..5, hs <- 1..5 do
      defp map({unquote(to_string(from)), <<w :: size(unquote(ws))-binary, "x", h :: size(unquote(hs))-binary>>}, acc) do
        %{acc | unquote(width) => w, unquote(height) => h}
      end
    end
  end

  defp map(_, payload) do
    payload
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
  def put(payload, key, value) when key in unquote(fields) do
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
