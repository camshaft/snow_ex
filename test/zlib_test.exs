defmodule ZlibTest do
  use ExUnit.Case

  test "round trip" do
    out = sample
    |> Zlib.gzip_stream()
    |> Zlib.gunzip_stream()
    |> Enum.join("")
    assert out == Enum.join(sample, "")
  end

  defp sample() do
    ["""
     Lorem ipsum dolor sit amet, consectetur adipiscing elit. Sed molestie leo non dolor fermentum convallis. Praesent mattis aliquet suscipit. Mauris fermentum scelerisque ex feugiat sagittis. Etiam dolor ipsum, pretium id urna in, malesuada auctor dui. Morbi venenatis, elit non finibus feugiat, magna ipsum dignissim turpis, in mollis ipsum leo et dolor. Sed in viverra purus, sit amet facilisis odio. Donec feugiat magna quis mauris hendrerit cursus. Pellentesque et ex sit amet turpis tincidunt consequat at quis enim. Interdum et malesuada fames ac ante ipsum primis in faucibus. Nulla eget lectus quis justo imperdiet tristique. Morbi tincidunt ultricies ipsum ac tempor. Morbi maximus, ligula at vulputate semper, eros elit congue urna, nec iaculis urna diam ac neque.

     Sed porttitor turpis nec magna tincidunt consectetur. Vestibulum sed quam consectetur, viverra magna vel, vulputate odio. Aliquam posuere, lacus vitae placerat mattis, ipsum massa cursus leo, sed luctus tortor lorem mattis ipsum. Nulla nec feugiat arcu. Praesent auctor a magna in faucibus. Vivamus porta odio ut tortor semper, luctus dapibus quam venenatis. Nunc cursus eu libero et venenatis.

     Nam tincidunt id sapien nec vestibulum. Vivamus efficitur nisl ut nibh aliquet tincidunt. Praesent dolor lectus, pulvinar vitae odio nec, egestas laoreet nisl. Donec justo erat, scelerisque sit amet viverra in, efficitur sed lacus. Praesent iaculis luctus sapien, in viverra urna lobortis ac. Phasellus rhoncus congue eros, sit amet interdum nisl porttitor a. Morbi non justo eu mauris auctor lobortis.

     In hac habitasse platea dictumst. Morbi ultrices vestibulum velit, vel tincidunt magna pretium eget. In semper scelerisque eros, eget luctus ex mollis eu. Nunc in enim tempor, varius magna eget, mollis quam. Nulla quis dapibus nunc. Aenean ultricies ante dolor, sed porttitor ex congue non. Aliquam ut commodo tortor. Nulla mattis maximus erat, ut finibus tortor. Nulla facilisi. Donec ornare tortor mauris, sed placerat sapien lobortis id. In sed lectus elementum, cursus dolor non, pretium nunc. Nam sed eros tortor. Phasellus risus lacus, mattis ac mollis in, venenatis eu leo.

     Pellentesque scelerisque libero ex, at tempus nisl pretium eu. Proin lobortis venenatis erat, quis scelerisque dolor elementum quis. Suspendisse eget semper elit, in varius ligula. Sed pretium, diam nec pulvinar suscipit, sapien dolor commodo leo, tincidunt suscipit diam quam at diam. Morbi congue ligula sed ligula iaculis, a malesuada sem feugiat. Praesent convallis eros et augue malesuada, non vestibulum dolor rutrum. Pellentesque habitant morbi tristique senectus et netus et malesuada fames ac turpis egestas. Donec dignissim, sem quis laoreet rhoncus, sem velit euismod nisl, ultricies consectetur metus dolor a nulla. Sed in mi interdum, suscipit diam ac, vestibulum massa. Nunc rutrum nisl vitae sem commodo, eu ornare nulla venenatis. Nunc ac posuere ante.
     """]
    |> Stream.cycle()
    |> Stream.take(100)
  end
end
