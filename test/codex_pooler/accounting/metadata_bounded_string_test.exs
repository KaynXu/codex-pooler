defmodule CodexPooler.Accounting.MetadataBoundedStringTest do
  use ExUnit.Case, async: true

  alias CodexPooler.Accounting.Metadata

  # findings#238: the one shared bound for provider-controlled strings that
  # are persisted or promoted to codes. Cleartext inside the caller's pattern
  # and byte length, a 12-character SHA-256 fingerprint otherwise, absent when
  # blank or not a binary; never erased.
  @pattern ~r/\A[A-Za-z0-9_.:-]+\z/
  @max_bytes 32

  test "keeps a trimmed in-pattern value within the byte bound in cleartext" do
    assert Metadata.bounded_string("  req_clear.01:abc-XYZ \n", @pattern, @max_bytes) ==
             "req_clear.01:abc-XYZ"

    exact = String.duplicate("a", @max_bytes)
    assert Metadata.bounded_string(exact, @pattern, @max_bytes) == exact
  end

  test "fingerprints values outside the pattern or over the bound, each distinctly" do
    outside = [
      String.duplicate("a", @max_bytes + 1),
      "has spaces inside",
      "trailing_control",
      "provider/segment",
      <<255, 254>>
    ]

    fingerprints = Enum.map(outside, &Metadata.bounded_string(&1, @pattern, @max_bytes))

    assert Enum.all?(fingerprints, &(&1 =~ ~r/\Asha256_[0-9a-f]{12}\z/))
    assert length(Enum.uniq(fingerprints)) == length(fingerprints)

    for {value, fingerprint} <- Enum.zip(outside, fingerprints) do
      assert fingerprint ==
               "sha256_" <>
                 (:crypto.hash(:sha256, String.trim(value))
                  |> Base.encode16(case: :lower)
                  |> String.slice(0, 12))
    end
  end

  test "blank and non-binary values are absent" do
    assert Metadata.bounded_string("", @pattern, @max_bytes) == nil
    assert Metadata.bounded_string("   \t", @pattern, @max_bytes) == nil
    assert Metadata.bounded_string(nil, @pattern, @max_bytes) == nil
    assert Metadata.bounded_string(42, @pattern, @max_bytes) == nil
    assert Metadata.bounded_string(%{"value" => "x"}, @pattern, @max_bytes) == nil
  end

  test "bounded_model_identifier is the 80-byte identifier class of the shared bound" do
    assert Metadata.bounded_model_identifier("gpt-5.3-codex-spark") == "gpt-5.3-codex-spark"
    assert Metadata.bounded_model_identifier("  ") == nil

    overlong = "gpt-" <> String.duplicate("x", 80)
    assert Metadata.bounded_model_identifier(overlong) =~ ~r/\Asha256_[0-9a-f]{12}\z/
    assert Metadata.bounded_model_identifier("model name") =~ ~r/\Asha256_[0-9a-f]{12}\z/
  end
end
