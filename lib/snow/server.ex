defmodule Snow.Server do
  defmacro __using__(opts) do

    collector = opts[:collector]

    gif = << 71 , 73 , 70 , 56,
             57 , 97 , 1  , 0,
             1  , 0  , 240, 0,
             0  , 255, 255, 255,
             0  , 0  , 0  , 33,
             249, 4  , 1  , 0,
             0  , 0  , 0  , 44,
             0  , 0  , 0  , 0,
             1  , 0  , 1  , 0,
             0  , 2  , 2  , 68,
             1  , 0  , 59>>

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
        event_id = case Plug.Conn.get_req_header(c, "x-request-id") do
          [id | _] ->
            id
          [] ->
            :crypto.rand_bytes(21)
            |> Base.encode64
        end
        unquote(collector).collect(c.query_string, c.remote_ip, ua, event_id)

        c
        |> Plug.Conn.put_resp_content_type("image/gif")
        |> Plug.Conn.send_resp(200, unquote(gif))
      end
    end
  end
end
