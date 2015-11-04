defmodule Snow.Model do
  fields = [app_id: "aid",
            platform: "p",

            # Date/time
            collector_tstamp: "$ct",
            dvce_tstamp: "dtm",
            dvce_sent_tstamp: "stm",
            os_timezone: "tz",
            etl_tstamp: nil,
            derived_tstamp: nil,

            # Transaction
            event: nil,
            event_id: "eid",
            txn_id: "tid",

            # Snowplow version fields
            v_tracker: "tv",
            v_collector: "$cv",
            v_etl: nil,
            name_tracker: "tna",
            etl_tags: nil,

            # User-related fields
            user_id: "uid",
            domain_userid: "duid",
            network_userid: ["tnuid", "nuid"],
            user_ipaddress: "ip",
            domain_sessionidx: "vid",
            domain_sessionid: "sid",

            # Device and operating system fields
            useragent: "ua",
            dvce_type: nil,
            dvce_ismobile: nil,
            dvce_screenheight: nil,
            dvce_screenwidth: nil,
            os_name: nil,
            os_family: nil,
            os_manufacturer: nil,

            # Location
            geo_country: nil,
            geo_region: nil,
            geo_city: nil,
            geo_zipcode: nil,
            geo_latitude: nil,
            geo_longitude: nil,
            geo_region_name: nil,
            geo_timezone: nil,

            # IP address
            ip_isp: nil,
            ip_organization: nil,
            ip_domain: nil,
            ip_netspeed: nil,

            # Web-specific
            page_url: "url",
            page_urlscheme: nil,
            page_urlhost: nil,
            page_urlport: nil,
            page_urlpath: nil,
            page_urlquery: nil,
            page_urlfragment: nil,
            page_referrer: "refr",
            page_title: "page",

            refr_urlscheme: nil,
            refr_urlhost: nil,
            refr_urlport: nil,
            refr_urlpath: nil,
            refr_urlquery: nil,
            refr_urlfragment: nil,
            refr_medium: nil,
            refr_source: nil,
            refr_term: nil,
            refr_domain_userid: nil,
            refr_dvce_tstamp: nil,

            doc_charset: "cs",
            doc_width: nil,
            doc_height: nil,

            mkt_medium: nil,
            mkt_source: nil,
            mkt_term: nil,
            mkt_content: nil,
            mkt_campaign: nil,
            mkt_clickid: nil,
            mkt_network: nil,

            user_fingerprint: "fp",
            connection_type: "ctype",
            cookie: nil,
            br_name: nil,
            br_version: nil,
            br_family: nil,
            br_type: nil,
            br_renderengine: nil,
            br_lang: "lang",
            br_features_pdf: "f_pdf",
            br_features_flash: "f_fla",
            br_features_java: "f_java",
            br_features_director: nil,
            br_features_quicktime: "f_qt",
            br_features_realplayer: "f_realp",
            br_features_windowsmedia: "f_wma",
            br_features_gears: "f_gears",
            br_features_silverlight: "f_ag",
            br_cookies: "cookie",
            br_colordepth: "cd",
            br_viewheight: nil,
            br_viewwidth: nil,

            # Page pings
            pp_xoffset_min: "pp_mix",
            pp_xoffset_max: "pp_max",
            pp_yoffset_min: "pp_miy",
            pp_yoffset_max: "pp_may",

            # Ecommerce
            tr_orderid: "tr_id",
            tr_affiliation: "tr_af",
            tr_total: "tr_tt",
            tr_tax: "tr_tx",
            tr_shipping: "tr_sh",
            tr_city: "tr_ci",
            tr_state: "tr_st",
            tr_country: "tr_co",
            tr_currency: "tr_cu",

            # Custom structure
            se_category: ["se_ca", "ev_ca"],
            se_action: ["se_ac", "ev_ac"],
            se_label: ["se_la", "ev_la"],
            se_property: ["se_pr", "ev_pr"],
            se_value: ["se_va", "ev_va"],

            unstruct_event: "ue_pr",

            # Contexts
            context: "co",
            derived_contexts: nil]

  json = [:etl_tags, :context]

  events = [pv: :page_view,
            pp: :page_ping,
            tr: :transaction,
            ti: :transaction_item,
            se: :struct,
            ue: :unstruct]

  dimensions = [res: {:dvce_screenwidth, :dvce_screenheight},
                ds: {:doc_width, :doc_height},
                vp: {:br_viewwidth, :br_viewheight}]

  base64 = [ue_px: :unstruct_event,
            cx: :context]

  defstruct Enum.map(fields, fn({key, _}) -> {key, nil} end)

  def from_string(qs) do
    qs
    |> URI.query_decoder()
    |> from_obj()
  end

  def from_obj(obj) do
    obj
    |> Enum.reduce(%__MODULE__{etl_tags: [], context: []}, &map/2)
    |> handle_missing_event_id()
  end

  defp handle_missing_event_id(model = %{event_id: nil}) do
    %{model | event_id: :crypto.rand_bytes(20) |> Base.encode64()}
  end
  defp handle_missing_event_id(model) do
    model
  end

  ## normal fields
  for {field, from} <- fields, from do
    from = if is_list(from), do: from, else: [from]
    for s <- from do
      defp map({unquote(to_string(s)), value}, acc) do
        cond do
          is_binary(value) && String.printable?(value) ->
            %{acc | unquote(field) => value}
          is_binary(value) ->
            acc
          true ->
            %{acc | unquote(field) => value}
        end
      end
    end
  end

  ## event
  for {from, to} <- events do
    defp map({"e", unquote(to_string(from))}, acc) do
      %{acc | event: unquote(to_string(to))}
    end
  end
  defp map({"e", from}, acc) do
    %{acc | event: from}
  end

  ## dimension
  for {from, {width, height}} <- dimensions do
    for ws <- 1..5, hs <- 1..5 do
      defp map({unquote(to_string(from)), <<w :: size(unquote(ws))-binary, "x", h :: size(unquote(hs))-binary>>}, acc) do
        %{acc | unquote(width) => w, unquote(height) => h}
      end
    end
  end

  ## base64
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

  defp map(_, acc) do
    acc
  end

  defimpl Poison.Encoder, for: Snow.Model do
    def encode(model, options) do
      :maps.fold(fn
        (_, nil, acc) ->
          acc
        (k, [], acc) when k in unquote(json) ->
          acc
        (k, tags, acc) when k in unquote(json) ->
          Map.put(acc, k, to_string(Poison.encode!(tags, options)))
        (k, v, acc) when k in unquote(Enum.map(fields, &(elem(&1, 0)))) ->
          Map.put(acc, k, v)
        (_, _, acc) ->
          acc
      end, %{}, model)
      |> Poison.encode!(options)
    end
  end
end
