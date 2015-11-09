defmodule Snow.Mixfile do
  use Mix.Project

  def project do
    [app: :snow,
     version: "0.0.1",
     elixir: "~> 1.0",
     build_embedded: Mix.env == :prod,
     start_permanent: Mix.env == :prod,
     deps: deps]
  end

  def application do
    [applications: [:geolix,
                    :httpoison,
                    :logger,
                    :ref_inspector,
                    :ua_inspector,
                    :yamerl]]
  end

  defp deps do
    [
      { :geolix, "~> 0.8" },
      { :httpoison, github: "edgurgel/httpoison", ref: "49ac32fa3f424b20749d55e86cffc37d55efc00a" },
      { :nile, "~> 0.1.0 "},
      { :poison, "~> 1.5.0" },
      { :cowboy, "~> 1.0.0" },
      { :plug, "~> 1.0" },
      { :msgpax, "~> 0.8" },
      { :ref_inspector, github: "camshaft/ref_inspector" },
      { :ua_inspector, github: "camshaft/ua_inspector" },
      { :yamerl, github: "yakaz/yamerl" },
    ]
  end
end
