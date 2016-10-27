defmodule Exercise do
  @moduledoc """
  Run from iex:
    > iex -S mix

  First time, do "Exercise.prep_file/2" - this will split the file into
  a number of files for parallel processing. It also prepends a sort key to
  each record in each of the split-off files.
  Ex: Exercise.prep_file("/path/to/input", "path to a directory for the new files")
  Cautiion: prep_file will first remove the output directory and any thing in it,
  so don't run it with a path whose contents your really care about.

  Then, run "Exercise.runit/2" with the path of the directory specified in the
  previous step, and the path to the output file.
  Ex: Exercise.runit("/where/the/files/live", "/tmp/output.txt")
  """
  import Benchwarmer
  alias Experimental.Flow

  require Logger

  @stages 4
  def benchmark do
    IO.inspect("Benchmarking non-flow version...")
    Benchwarmer.benchmark(fn -> non_flow("/home/mmartin/Documents/words.txt", "/tmp/out2.non_flow") end)
    IO.inspect("Benchmarking flow version...")
    Benchwarmer.benchmark(fn -> runit("/tmp/exercise", "/tmp/out2.flow") end)
  end

  def prep_file(in_path, out_path) do
    {:ok, f} = File.open(in_path)
    {:ok, %{size: size}} = File.stat(in_path)
    :ok = split_file(f, out_path, size / @stages)
    File.close(f)
  end

  def runit(path, file_path) do
    streams = for file <- File.ls!(path) do
      File.stream!("/tmp/exercise/#{file}", read_ahead: 100_000)
    end
    fn -> exercise(streams, file_path) end
    |> :timer.tc
    |> elem(0)
    |> Kernel./(1_000_000)
  end

  def exercise(stream_list, file_path) do
    Flow.from_enumerables(stream_list)
    |> Flow.partition()
    |> Flow.map(&(munge_string(&1)))
    |> Enum.sort()
    |> Enum.to_list()
    |> Stream.map(&(String.slice(&1, 9..-1)))
    |> Stream.into( File.stream!(file_path))
    |> Stream.run()
  end

  defp munge_string(str) do
    str = String.rstrip(str) # remove newline
    str <> "," <> Integer.to_string(String.length(str) - 9) <> "\n"
  end

  def non_flow(path_in, path_out) do
    File.stream!(path_in)
    |> Stream.map(&(String.rstrip(&1) <> "," <> Integer.to_string(String.length(String.rstrip(&1))) <> "\n"))
    |>Stream.into(File.stream!(path_out))
    |> Stream.run()
  end

  def position_file(f) do
    blocks = get_blocks(f, 0, 64*1024)
  end

  defp get_blocks(f, curpos, blksize) do
    get_blocks(f, curpos, blksize, [])
  end

defp get_blocks(f, curpos, blksize, list_of_blocks) do
    newpos = curpos + blksize
    :file.position(f, newpos)
    state = read_to_newline(f)
  Logger.debug("State: #{inspect state}")
  case state do
      {:more, residual} ->
        list_of_blocks = [{curpos, blksize + residual}] ++ list_of_blocks
        newpos = curpos + blksize + residual
        get_blocks(f, newpos, blksize, list_of_blocks)
      {:done, residual} ->
        {:ok, [{curpos, blksize + residual}] ++ list_of_blocks}
      {:error, error} ->
        {:error, error}
    end
  end

  defp read_to_newline(f, acc \\ 0) do
  c = :file.read(f, 1)
    case c do
      {:ok, "\n"} ->
        {:more, acc + 1}
      {:ok, _} ->
      read_to_newline(f, acc + 1)
      :eof ->
        {:done, acc}
      {:error, error} ->
        Logger.debug("File read error: #{inspect error}")
        {:error, error}
    end
  end

  def split_file(f, path, size) do
    {:ok, _} = File.rm_rf(path)
    :ok = File.mkdir_p(path)
    {:ok, blocks} = get_blocks(f, 0, size)
    split_files(f, path, Enum.reverse(blocks))
  end
  defp split_files(_f, _path, [], _c, _s) do
    :ok
  end
  defp split_files(infile, path, [block|blocks], count \\ 0, serial \\ 0) do
    {pos, len} = block
    {:ok, outfile} = if String.ends_with?(path, "/") do
      File.open(path <> Integer.to_string(count) <> ".txt", [:write])
    else
      File.open(path <> "/" <> Integer.to_string(count) <> ".txt", [:write])
    end
    :file.position(infile, pos)
    state = :file.read(infile, len)
    result = case state do
      {:ok, data} ->
        data = String.split(data, "\n")
        output_data(outfile, data, serial)
      eof ->
        {:eof, serial}
    end
    case result do
      {:ok, serial}->
        split_files(infile, path, blocks, count+1, serial)
    end
  end

  defp output_data(outfile, [], serial) do
    {:ok, serial}
  end
  defp output_data(outfile, [line|lines], serial) do

    if String.length(line) < 1 do
      output_data(outfile, lines, serial)
    else
      serial = serial + 1
      line = String.rstrip(List.to_string(:io_lib.format("~8..0B~n", [serial]))) <> " " <> line <> "\n"
      len = String.length(line)
      rc = :file.write(outfile, line)
      case rc do
        :ok ->
          output_data(outfile, lines, serial)
        {:error, reason} ->
          Logger.debug("Error: #{inspect reason}")
          {:error, reason}
      end
    end
  end

end
