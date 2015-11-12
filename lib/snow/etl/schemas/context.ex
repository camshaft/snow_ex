defmodule Snow.ETL.Schemas.Context do
  @derives [Poison.Encoder]
  defstruct schema: %{},
            hierarchy: %{},
            data: %{}
end
