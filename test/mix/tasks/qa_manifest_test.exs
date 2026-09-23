defmodule CodexPooler.MixTasks.QaManifestTest do
  use CodexPooler.UnixIntegrationCase, async: true, tools: ~w(perl)

  @helper Path.expand("../../../dev_support/bin/qa-manifest", __DIR__)

  setup do
    root = Path.join(System.tmp_dir!(), "qa-manifest-#{System.pid()}-#{System.unique_integer([:positive])}")
    on_exit(fn -> File.rm_rf!(root) end)
    File.mkdir_p!(root)
    %{root: root}
  end

  test "batches file metadata, nonempty hashes, dangling links and absent tracked files", %{root: root} do
    File.write!(Path.join(root, "first.ex"), "first\n")
    File.write!(Path.join(root, "with space.ex"), "second\n")
    File.chmod!(Path.join(root, "first.ex"), 0o640)
    File.chmod!(Path.join(root, "with space.ex"), 0o755)
    File.ln_s!("missing.ex", Path.join(root, "link.ex"))

    assert {output, 0} = manifest("source", root, ["first.ex", "with space.ex", "link.ex", "deleted.ex"])

    assert output == "file\t640:6\t#{sha("first\n")}\tfirst.ex\nfile\t755:7\t#{sha("second\n")}\twith space.ex\nsymlink\tmissing.ex\tlink.ex\nabsent\tdeleted.ex\n"
  end

  test "batches real BEAM file bytes with build-relative paths", %{root: root} do
    ebin = Path.join(root, "lib/sample/ebin")
    File.mkdir_p!(ebin)
    first = Path.join(ebin, "first.beam")
    second = Path.join(ebin, "second.beam")
    File.write!(first, <<0, 1, 255>>)
    File.write!(second, <<2, 3, 254>>)

    assert {output, 0} = manifest("beams", root, [first, second])
    assert output == "#{sha(<<0, 1, 255>>)}\tlib/sample/ebin/first.beam\n#{sha(<<2, 3, 254>>)}\tlib/sample/ebin/second.beam\n"
  end

  test "hashes multiple files in argument order", %{root: root} do
    first = Path.join(root, "first")
    second = Path.join(root, "second")
    File.write!(first, "one")
    File.write!(second, "two")
    assert {output, 0} = helper(["hashes", second, first])
    assert output == "#{sha("two")} #{sha("one")}\n"
  end

  test "empty path streams produce empty manifests", %{root: root} do
    assert {"", 0} = manifest("source", root, [])
    assert {"", 0} = manifest("beams", root, [])
  end

  test "missing hash and BEAM inputs fail without partial rows", %{root: root} do
    first = Path.join(root, "first")
    missing = Path.join(root, "missing")
    File.write!(first, "one")

    {output, code} = helper(["hashes", first, missing])
    assert code != 0
    assert output =~ "file open failed"
    refute output =~ sha("one")

    {output, code} = manifest("beams", root, [first, missing])
    assert code != 0
    assert output =~ "file inspection failed"
    refute output =~ sha("one")
  end

  test "file inspection errors are not reported as absent paths", %{root: root} do
    File.write!(Path.join(root, "file"), "one")
    {output, code} = manifest("source", root, ["file/child"])
    assert code != 0
    assert output =~ "file inspection failed"
    refute output =~ "absent"
  end

  test "directories cannot be hashed as successful empty files", %{root: root} do
    {output, code} = helper(["hashes", root])
    assert code != 0
    assert output =~ "not a regular file"
  end

  test "input read errors fail instead of producing an empty manifest", %{root: root} do
    {output, code} = System.cmd("/bin/bash", ["-c", "exec perl \"$1\" source \"$2\" < \"$2\"", "qa-manifest-test", @helper, root], stderr_to_stdout: true)
    assert code != 0
    assert output =~ "manifest input read failed"
  end

  test "rejects unterminated and ambiguous manifest input", %{root: root} do
    for input <- ["file", "\0", "tab\tfile\0", "line\nfile\0", "../outside\0", "/absolute\0"] do
      {output, code} = manifest_input("source", root, input)
      assert code != 0
      assert output =~ "qa-manifest:"
    end
  end

  test "rejects ambiguous symlink targets and BEAM paths outside the selected root", %{root: root} do
    File.ln_s!("line\nfile", Path.join(root, "link"))
    {output, code} = manifest("source", root, ["link"])
    assert code != 0
    assert output =~ "invalid manifest field"

    {output, code} = manifest("beams", root, ["#{root}-sibling/file.beam"])
    assert code != 0
    assert output =~ "outside build root"
  end

  defp manifest(mode, root, paths) do
    input = Enum.map_join(paths, "", &(&1 <> <<0>>))
    manifest_input(mode, root, input)
  end

  defp manifest_input(mode, root, input) do
    input_file = Path.join(root, "input-#{System.unique_integer([:positive])}")
    File.write!(input_file, input)

    System.cmd("/bin/bash", ["-c", "exec perl \"$1\" \"$2\" \"$3\" < \"$4\"", "qa-manifest-test", @helper, mode, root, input_file], stderr_to_stdout: true)
  end

  defp helper(args), do: System.cmd("perl", [@helper | args], stderr_to_stdout: true)
  defp sha(bytes), do: :crypto.hash(:sha256, bytes) |> Base.encode16(case: :lower)
end
