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
    [applications: [:logger,
                    :httpoison]]
  end

  defp deps do
    [
      # {:httpoison, "~> 0.7.2"}
      {:httpoison, github: "edgurgel/httpoison", ref: "49ac32fa3f424b20749d55e86cffc37d55efc00a"},
      {:poison, "~> 1.5.0"}
    ]
  end
end
