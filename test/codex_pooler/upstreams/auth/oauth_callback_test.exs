defmodule CodexPooler.Upstreams.Auth.OAuthCallbackTest do
  use ExUnit.Case, async: true

  import ExUnit.CaptureLog

  alias CodexPooler.Upstreams.Auth.OAuthCallback

  @callback_origin "http://localhost:1455/auth/callback"
  @safe_codes [
    :invalid_callback_url,
    :invalid_callback_origin,
    :missing_state,
    :duplicate_callback_param,
    :missing_callback_result,
    :provider_denied,
    :invalid_state,
    :expired_flow,
    :flow_not_pending,
    :stale_flow,
    :token_exchange_failed,
    :identity_mismatch,
    :identity_conflict,
    :unauthorized_pool
  ]

  describe "parse/1" do
    test "accepts localhost browser callback URLs with one state and one code" do
      assert {:ok, %{state: "state_123", code: "authorization-code-456"}} =
               OAuthCallback.parse("#{@callback_origin}?state=state_123&code=authorization-code-456")
    end

    test "accepts provider success callbacks that include returned scopes" do
      assert {:ok, %{state: "state_123", code: "authorization-code.with_safe-chars"}} =
               OAuthCallback.parse(
                 "#{@callback_origin}?code=authorization-code.with_safe-chars&" <>
                   "scope=openid+profile+email+offline_access+api.connectors.read+api.connectors.invoke&" <>
                   "state=state_123"
               )
    end

    test "ignores future provider query parameters outside state code and error" do
      raw_provider_value = "raw-provider-extra-value-must-not-leak"

      assert {:ok, %{state: "state_123", code: "authorization-code-456"} = parsed} =
               OAuthCallback.parse(
                 "#{@callback_origin}?state=state_123&code=authorization-code-456&" <>
                   "scope=openid+profile&access_token=#{raw_provider_value}&" <>
                   "provider_extra=#{raw_provider_value}&provider_extra=ignored"
               )

      refute Map.has_key?(parsed, :scope)
      refute inspect(parsed) =~ raw_provider_value
    end

    test "accepts 127.0.0.1 as the manual browser callback host" do
      assert {:ok, %{state: "state_123", code: "authorization-code-456"}} =
               OAuthCallback.parse("http://127.0.0.1:1455/auth/callback?state=state_123&code=authorization-code-456")
    end

    test "provider-denied callbacks keep only safe code, state, and message" do
      raw_provider_value = "raw-provider-error-token-must-not-leak"

      assert {:error,
              %{
                code: :provider_denied,
                message: "OpenAI denied the OAuth request",
                state: "state_123"
              } = error} =
               OAuthCallback.parse("#{@callback_origin}?state=state_123&error=access_denied&error_description=#{raw_provider_value}")

      refute inspect(error) =~ "access_denied"
      refute inspect(error) =~ raw_provider_value
    end

    test "rejects malformed and relative callback URLs" do
      for callback_url <- ["not-a-url", "/auth/callback?state=state_123&code=code_123", nil] do
        assert {:error, %{code: :invalid_callback_url}} = OAuthCallback.parse(callback_url)
      end
    end

    test "rejects invalid callback origins and paths" do
      invalid_urls = [
        "https://localhost:1455/auth/callback?state=state_123&code=code_123",
        "http://example.com:1455/auth/callback?state=state_123&code=code_123",
        "http://localhost:1456/auth/callback?state=state_123&code=code_123",
        "http://localhost/auth/callback?state=state_123&code=code_123",
        "http://localhost:1455/wrong/path?state=state_123&code=code_123",
        "http://user@localhost:1455/auth/callback?state=state_123&code=code_123"
      ]

      for callback_url <- invalid_urls do
        assert {:error, %{code: :invalid_callback_origin}} =
                 OAuthCallback.parse(callback_url)
      end
    end

    test "rejects fragments carrying callback data" do
      assert {:error, %{code: :invalid_callback_url}} =
               OAuthCallback.parse("#{@callback_origin}#state=state_123&code=code_123")

      assert {:error, %{code: :invalid_callback_url}} =
               OAuthCallback.parse("#{@callback_origin}?state=state_123&code=code_123#access_token=raw")
    end

    test "rejects missing state" do
      assert {:error, %{code: :missing_state}} =
               OAuthCallback.parse("#{@callback_origin}?code=code_123")

      assert {:error, %{code: :missing_state}} =
               OAuthCallback.parse("#{@callback_origin}?state=&code=code_123")
    end

    test "rejects duplicate control query keys before interpreting callback data" do
      duplicate_urls = [
        "#{@callback_origin}?state=state_123&state=state_456&code=code_123",
        "#{@callback_origin}?state=state_123&code=code_123&code=code_456",
        "#{@callback_origin}?state=state_123&error=access_denied&error=server_error"
      ]

      for callback_url <- duplicate_urls do
        assert {:error, %{code: :duplicate_callback_param}} =
                 OAuthCallback.parse(callback_url)
      end
    end

    test "rejects missing, blank, mixed, and unsupported callback results" do
      invalid_result_urls = [
        "#{@callback_origin}?state=state_123",
        "#{@callback_origin}?state=state_123&code=",
        "#{@callback_origin}?state=state_123&error=",
        "#{@callback_origin}?state=state_123&code=code_123&error=access_denied"
      ]

      for callback_url <- invalid_result_urls do
        assert {:error, %{code: :missing_callback_result}} =
                 OAuthCallback.parse(callback_url)
      end

      assert {:ok, %{state: "state_123", code: "code_123"}} =
               OAuthCallback.parse("#{@callback_origin}?state=state_123&code=code_123&extra=raw")
    end

    test "does not expose raw callback URL data in returned errors or logs" do
      raw_value = "raw-callback-token-must-not-leak"

      log =
        capture_log(fn ->
          assert {:error, error} =
                   OAuthCallback.parse("#{@callback_origin}?state=state_123&code=code_123&access_token=#{raw_value}##{raw_value}")

          refute inspect(error) =~ raw_value
          refute inspect(error) =~ "access_token"
        end)

      refute log =~ raw_value
      refute log =~ "access_token"
    end
  end

  describe "safe failure codes" do
    test "covers every stable manual callback failure code" do
      assert OAuthCallback.failure_codes() == @safe_codes

      for code <- @safe_codes do
        assert %{code: ^code, message: message} = OAuthCallback.safe_error(code)
        assert is_binary(message)
        assert message != ""
      end
    end

    test "unknown failure codes collapse to invalid callback URL" do
      assert %{code: :invalid_callback_url, message: "OAuth callback URL is invalid"} =
               OAuthCallback.safe_error(:unknown_failure)
    end
  end
end
