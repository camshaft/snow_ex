use Mix.Config

config :ref_inspector,
  yaml: "priv/referers.yml"

config :ua_inspector,
  database_path: "priv/ua_inspector"

config :geolix,
  databases: [
    { :city,    "priv/ip.city.mmdb.gz" },
    { :country, "priv/ip.city.mmdb.gz" }
  ]
