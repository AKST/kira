defmodule Kira.MixProject do
  use Mix.Project

  def project do
    [
      app: :kira,
      name: "Kira",
      package: package(),
      description: description(),

      version: "0.1.0",
      elixir: "~> 1.7",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  def application, do: []

  defp description do
    "A conncurrent task scheduler with rollback capabilities"
  end

  defp package() do
    [
      name: "kira",
      files: ~w(
        lib mix.exs README.md CONTRIBUTING.md LICENSE
        CHANGELOG.md test
      ),
      maintainers: ["Angus Karl Stewart Thomsen"],
      licenses: ["MIT"],
      links: %{
        "Github" => "https://github.com/AKST/kira",
      },
    ]
  end

  defp deps, do: []
end
