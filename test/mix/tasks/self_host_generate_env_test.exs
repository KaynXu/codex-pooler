defmodule CodexPooler.SelfHostGenerateEnvTest do
  use CodexPooler.UnixIntegrationCase,
    async: false,
    tools: ~w(env openssl sh)

  @moduletag :tmp_dir

  test "explicit empty lowercase proxy variables disable uppercase fallbacks", %{tmp_dir: tmp_dir} do
    target = Path.join(tmp_dir, ".env")

    assert {_output, 0} =
             System.cmd(
               "env",
               [
                 "http_proxy=",
                 "HTTP_PROXY=http://ignored-http.example:8080",
                 "https_proxy=",
                 "HTTPS_PROXY=http://ignored-https.example:8080",
                 "no_proxy=",
                 "NO_PROXY=ignored.example",
                 "sh",
                 "scripts/self-host/generate-env.sh",
                 target
               ],
               stderr_to_stdout: true
             )

    contents = File.read!(target)
    assert contents =~ "\nhttp_proxy=\n"
    assert contents =~ "\nhttps_proxy=\n"
    assert contents =~ "\nno_proxy=\n"
    refute contents =~ "ignored-http.example"
    refute contents =~ "ignored-https.example"
    refute contents =~ "ignored.example"
    assert Bitwise.band(File.stat!(target).mode, 0o777) == 0o600
  end
end
