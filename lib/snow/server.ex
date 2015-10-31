defmodule Snow.Server do
  defmacro __using__(opts) do

    collector = opts[:collector]

    gif = File.read!(__DIR__ <> "/server/i.gif")

    quote do
      use Plug.Router

      get "/i" do
        c = var!(conn)
        ua = case Plug.Conn.get_req_header(c, "user-agent") do
          [v | _] ->
            v
          [] ->
            nil
        end
        unquote(collector).collect(c.query_string, c.remote_ip, ua)

        c
        |> Plug.Conn.put_resp_content_type("image/gif")
        |> Plug.Conn.send_resp(200, unquote(gif))
      end
    end
  end
end
