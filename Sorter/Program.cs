using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;

internal sealed class Record
{
    public required string Text; // строковая часть
    public int Number;           // числовая часть

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static bool TryParse(ReadOnlySpan<char> line, out Record? rec)
    {
        // формат: "<int>. <text>"
        int dot = line.IndexOf('.');
        if (dot <= 0 || dot + 2 >= line.Length || line[dot + 1] != ' ')
        { rec = null; return false; }
        if (!int.TryParse(line[..dot], NumberStyles.None, CultureInfo.InvariantCulture, out int num))
        { rec = null; return false; }

        var text = new string(line[(dot + 2)..]); // одна аллокация
        rec = new Record { Text = text, Number = num };
        return true;
    }
}

internal sealed record Options(
    string InPath,
    string OutPath,
    string TempDir,
    int RunSizeMb,
    int Threads,
    int FanIn
);

internal static class OptionsParser
{
    private const int DefaultMemMb = 1024;   // если --runsize не задан, возьмём из --mem
    private const int DefaultFanIn = 128;    // сколько файлов сливаем за один проход

    public static bool TryParse(string[] args, out Options? opt, out string? error)
    {
        opt = null; error = null;

        string? inPath = GetArg(args, "--in");
        string? outPath = GetArg(args, "--out");
        if (string.IsNullOrWhiteSpace(inPath)) { error = "--in required"; return false; }
        if (string.IsNullOrWhiteSpace(outPath)) { error = "--out required"; return false; }

        string tempDir = GetArg(args, "--temp") ??
                         Path.Combine(Path.GetDirectoryName(Path.GetFullPath(outPath))!, "runs");

        int memMb = (int)(ParseLong(GetArg(args, "--mem")) ?? DefaultMemMb);
        int runSizeMb = (int)(ParseLong(GetArg(args, "--runsize")) ?? memMb);
        int threads = (int)(ParseLong(GetArg(args, "--threads")) ?? Math.Max(1, Environment.ProcessorCount / 2));
        int fanIn = (int)(ParseLong(GetArg(args, "--fanin")) ?? DefaultFanIn);

        if (fanIn < 2) { error = "--fanin must be >= 2"; return false; }

        opt = new Options(
            InPath: inPath!,
            OutPath: outPath!,
            TempDir: tempDir,
            RunSizeMb: runSizeMb,
            Threads: threads,
            FanIn: fanIn
        );
        return true;
    }

    private static string? GetArg(string[] a, string name)
        => a.SkipWhile(x => !string.Equals(x, name, StringComparison.OrdinalIgnoreCase)).Skip(1).FirstOrDefault();

    private static long? ParseLong(string? s)
        => long.TryParse(s, NumberStyles.Integer, CultureInfo.InvariantCulture, out var v) ? v : null;
}

internal sealed class RecordComparer : IComparer<Record>
{
    private static readonly StringComparer Ord = StringComparer.Ordinal;
    public int Compare(Record? x, Record? y)
    {
        if (ReferenceEquals(x, y)) return 0;
        if (x is null) return -1;
        if (y is null) return 1;
        int c = Ord.Compare(x.Text, y.Text);
        return c != 0 ? c : x.Number.CompareTo(y.Number);
    }
}

internal static class Program
{
    // ============================ Конфигурация ============================
    private const int IoBufferSizeBytes = 1 << 20; // 1 MiB буферы I/O
    private const int ProgressIntervalMs = 500;     // обновление статуса
    private static readonly Encoding Utf8NoBom = new UTF8Encoding(encoderShouldEmitUTF8Identifier: false);

    // ================================= Main ===============================
    private static int Main(string[] args)
    {
        if (args.Length == 0 || args.Contains("--help") || args.Contains("-h"))
        {
            PrintUsage();
            return 1;
        }

        if (!OptionsParser.TryParse(args, out var opt, out var err))
        {
            Console.Error.WriteLine("ERROR: " + err);
            PrintUsage();
            return 2;
        }

        var cts = new CancellationTokenSource();
        Console.CancelKeyPress += (_, e) => { e.Cancel = true; cts.Cancel(); };

        try
        {
            ValidateAndInit(opt!);

            PrintHeader(opt!);
            var swTotal = Stopwatch.StartNew();
            List<string> runs = new();

            try
            {
                runs = CreateRuns(opt!, cts.Token);
                Console.WriteLine($"\nCreated {runs.Count} runs in {swTotal.Elapsed}.");

                MergeAllRuns(opt!, runs, cts.Token);
                Console.WriteLine($"\nMerged into {opt!.OutPath} in {swTotal.Elapsed} (total).");
            }
            finally
            {
                CleanupTemp(opt!, runs);
            }

            return 0;
        }
        catch (OperationCanceledException)
        {
            Console.Error.WriteLine("\nCancelled.");
            return 3;
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine("ERROR: " + ex.Message);
            return 2;
        }
        finally
        {
            cts.Dispose();
        }
    }

    // =========================== Инициализация ============================
    private static void ValidateAndInit(Options opt)
    {
        if (!File.Exists(opt.InPath))
            throw new FileNotFoundException("Input file not found", opt.InPath);
        Directory.CreateDirectory(opt.TempDir);
    }

    private static void PrintHeader(Options opt)
    {
        Console.WriteLine($"Input   : {opt.InPath}");
        Console.WriteLine($"Output  : {opt.OutPath}");
        Console.WriteLine($"Temp    : {opt.TempDir}");
        Console.WriteLine($"Threads : {opt.Threads}");
        Console.WriteLine($"RunSize : {opt.RunSizeMb} MB");
        Console.WriteLine($"Fan-in  : {opt.FanIn}\n");
    }

    private static void CleanupTemp(Options opt, List<string> runs)
    {
        foreach (var r in runs) { try { File.Delete(r); } catch { } }
        try { Directory.Delete(opt.TempDir, recursive: true); } catch { }
    }

    // ======================== Фаза 1: READ / RUNS =========================
    private static List<string> CreateRuns(Options opt, CancellationToken token)
    {
        var comparer = new RecordComparer();
        var pending = new BlockingCollection<List<Record>>(boundedCapacity: Math.Max(1, opt.Threads));
        var runFiles = new ConcurrentBag<string>();

        long inputBytes = new FileInfo(opt.InPath).Length;
        long targetBytes = (long)opt.RunSizeMb << 20; // MB -> bytes (long)

        var swUi = Stopwatch.StartNew();
        long lastUi = 0;
        long linesTotal = 0;
        int runsCreated = 0;

        // писатели раннов: сортируют и сбрасывают
        var writers = Enumerable.Range(0, opt.Threads).Select(_ => Task.Run(() =>
        {
            foreach (var batch in pending.GetConsumingEnumerable(token))
            {
                batch.Sort(comparer);

                string runPath = Path.Combine(opt.TempDir, $"run_{Guid.NewGuid():N}.txt");
                using var fs = new FileStream(runPath, FileMode.Create, FileAccess.Write, FileShare.Read,
                                              bufferSize: IoBufferSizeBytes, options: FileOptions.SequentialScan);
                using var sw = new StreamWriter(fs, Utf8NoBom, bufferSize: IoBufferSizeBytes);

                foreach (var r in batch)
                {
                    sw.Write(r.Number.ToString(CultureInfo.InvariantCulture));
                    sw.Write(". ");
                    sw.WriteLine(r.Text);
                }
                sw.Flush(); fs.Flush(flushToDisk: true);

                runFiles.Add(runPath);
                Interlocked.Increment(ref runsCreated);
                batch.Clear();
            }
        }, token)).ToArray();

        // читатель: режет по фактически прочитанным байтам (fs.Position)
        using var fsIn = new FileStream(opt.InPath, FileMode.Open, FileAccess.Read, FileShare.Read,
                                        bufferSize: IoBufferSizeBytes, options: FileOptions.SequentialScan);
        using var sr = new StreamReader(fsIn, Encoding.UTF8, detectEncodingFromByteOrderMarks: true, bufferSize: IoBufferSizeBytes);

        List<Record> current = new(capacity: 1_000_000);
        long runStartPos = fsIn.Position;
        string? line;

        while ((line = sr.ReadLine()) is not null)
        {
            token.ThrowIfCancellationRequested();

            if (Record.TryParse(line.AsSpan(), out var rec))
            {
                current.Add(rec!);
                linesTotal++;
            }
            // битые строки просто пропускаем; при желании можно добавить политику warn/fail

            long consumed = fsIn.Position - runStartPos; // точный размер текущего ранна
            if (consumed >= targetBytes)
            {
                pending.Add(current, token);
                current = new List<Record>(capacity: current.Count);
                runStartPos = fsIn.Position;
            }

            if (swUi.ElapsedMilliseconds - lastUi >= ProgressIntervalMs)
            {
                lastUi = swUi.ElapsedMilliseconds;
                double pct = inputBytes > 0 ? (fsIn.Position * 100.0 / inputBytes) : 0.0;
                PrintStatus("READ ",
                    $"pos {fsIn.Position:N0}/{inputBytes:N0} ({pct:F1}%)",
                    $"runs {runsCreated:N0}",
                    $"lines {linesTotal:N0}",
                    $"run~ {(consumed >> 20):N0} MB");
            }
        }

        if (current.Count > 0)
            pending.Add(current, token);

        pending.CompleteAdding();
        Task.WaitAll(writers, token);

        PrintStatusLine("READ ", $"pos {fsIn.Position:N0}/{inputBytes:N0} (100.0%)", $"runs {runsCreated:N0}", $"lines {linesTotal:N0}", "run~ 0 MB");
        return runFiles.ToList();
    }

    // =================== Фаза 2: MERGE (многоэтапно) ======================
    private static void MergeAllRuns(Options opt, List<string> runs, CancellationToken token)
    {
        if (runs.Count == 0)
        {
            File.WriteAllText(opt.OutPath, string.Empty, Utf8NoBom);
            return;
        }

        if (runs.Count == 1)
        {
            File.Copy(runs[0], opt.OutPath, overwrite: true);
            return;
        }

        var work = new List<string>(runs);
        string tempRoot = Path.Combine(Path.GetDirectoryName(Path.GetFullPath(opt.OutPath))!, "merge_" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(tempRoot);

        try
        {
            while (work.Count > opt.FanIn)
            {
                var next = new List<string>((work.Count + opt.FanIn - 1) / opt.FanIn);

                foreach (var batch in work.Chunk(opt.FanIn))
                {
                    token.ThrowIfCancellationRequested();
                    string mid = Path.Combine(tempRoot, $"pass_{Guid.NewGuid():N}.txt");
                    MergeBatch(batch.ToList(), mid, token);
                    next.Add(mid);
                }

                foreach (var r in work) { try { File.Delete(r); } catch { } }
                work = next;
            }

            MergeBatch(work, opt.OutPath, token);

            foreach (var r in work)
                if (!string.Equals(r, opt.OutPath, StringComparison.OrdinalIgnoreCase))
                    try { File.Delete(r); } catch { }
        }
        finally
        {
            try { Directory.Delete(tempRoot, recursive: true); } catch { }
        }
    }

    private static void MergeBatch(List<string> runs, string outPath, CancellationToken token)
    {
        var comparer = new RecordComparer();
        long totalBytes = 0;
        foreach (var p in runs) totalBytes += new FileInfo(p).Length;

        using var readers = OpenReaders(runs);

        var pq = new PriorityQueue<RunReader, Record>(runs.Count, new RecordPriorityComparer(comparer));
        foreach (var rr in readers)
            if (rr.Current is not null)
                pq.Enqueue(rr, rr.Current);

        long outBytes = 0, outLines = 0;
        var swUi = Stopwatch.StartNew(); long lastUi = 0;

        using var fsOut = new FileStream(outPath, FileMode.Create, FileAccess.Write, FileShare.Read,
                                         bufferSize: IoBufferSizeBytes, options: FileOptions.SequentialScan);
        using var sw = new StreamWriter(fsOut, Utf8NoBom, bufferSize: IoBufferSizeBytes);

        while (pq.TryDequeue(out var rr, out var rec))
        {
            token.ThrowIfCancellationRequested();

            string num = rec.Number.ToString(CultureInfo.InvariantCulture);
            sw.Write(num); sw.Write(". "); sw.WriteLine(rec.Text);

            outLines++;
            outBytes += Utf8NoBom.GetByteCount(num) + 2 + Utf8NoBom.GetByteCount(rec.Text) + 1;

            if (rr.Advance() && rr.Current is not null)
                pq.Enqueue(rr, rr.Current);

            if (swUi.ElapsedMilliseconds - lastUi >= ProgressIntervalMs)
            {
                lastUi = swUi.ElapsedMilliseconds;
                double pct = totalBytes > 0 ? (fsOut.Position * 100.0 / totalBytes) : 0.0;
                PrintStatus("MERGE",
                    $"out {fsOut.Position:N0}/{totalBytes:N0} (~{pct:F1}%)",
                    $"queue {pq.Count:N0}",
                    $"lines {outLines:N0}",
                    $"rate {Throughput(outBytes, swUi.Elapsed):N1} MB/s");
            }
        }

        sw.Flush(); fsOut.Flush(flushToDisk: true);
        PrintStatusLine("MERGE", $"out {fsOut.Position:N0}/{totalBytes:N0} (~100.0%)", $"queue 0", $"lines {outLines:N0}", $"rate {Throughput(outBytes, swUi.Elapsed):N1} MB/s");
    }

    // ============================ Helpers (IO) ============================
    private sealed class RunReader : IDisposable
    {
        private readonly StreamReader _sr;
        public Record? Current;
        public readonly string Path;

        public RunReader(string path)
        {
            Path = path;
            _sr = new StreamReader(
                new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read,
                               bufferSize: IoBufferSizeBytes, options: FileOptions.SequentialScan),
                Encoding.UTF8, detectEncodingFromByteOrderMarks: true, bufferSize: IoBufferSizeBytes);
            Advance();
        }

        public bool Advance()
        {
            while (true)
            {
                string? line = _sr.ReadLine();
                if (line is null) { Current = null; return false; }
                if (Record.TryParse(line.AsSpan(), out var rec)) { Current = rec; return true; }
                // битую строку пропускаем; при желании можно добавить политику warn/fail
            }
        }

        public void Dispose() => _sr.Dispose();
    }

    private sealed class RunReaderSet : List<RunReader>, IDisposable
    {
        public void Dispose() { foreach (var rr in this) rr.Dispose(); }
    }

    private static RunReaderSet OpenReaders(List<string> runs)
    {
        var set = new RunReaderSet();
        try
        {
            foreach (var p in runs) set.Add(new RunReader(p));
            return set;
        }
        catch
        {
            set.Dispose();
            throw;
        }
    }

    private sealed class RecordPriorityComparer : IComparer<Record>
    {
        private readonly RecordComparer _inner;
        public RecordPriorityComparer(RecordComparer inner) => _inner = inner;
        public int Compare(Record? x, Record? y) => _inner.Compare(x!, y!);
    }

    // ============================ Helpers (UI) =============================
    private static void PrintStatus(string phase, params string[] parts)
        => Console.Write($"\r[{phase}] " + string.Join(" | ", parts));

    private static void PrintStatusLine(string phase, params string[] parts)
        => Console.WriteLine($"\r[{phase}] " + string.Join(" | ", parts));

    private static double Throughput(long bytes, TimeSpan elapsed)
        => elapsed.TotalSeconds > 0 ? (bytes / 1_000_000.0) / elapsed.TotalSeconds : 0.0;

    private static void PrintUsage()
    {
        Console.WriteLine("Usage:");
        Console.WriteLine("  --in <path>    : input file path");
        Console.WriteLine("  --out <path>   : output file path");
        Console.WriteLine("  --temp <dir>   : temp dir for runs (default: <out_dir>\\runs)");
        Console.WriteLine("  --mem <MB>     : memory budget per run (MB). Default 1024");
        Console.WriteLine("  --runsize <MB> : in-memory run size (MB). Default = --mem");
        Console.WriteLine("  --threads <N>  : parallel writers for run creation");
        Console.WriteLine("  --fanin <N>    : max files merged at once (multi-pass). Default 128");
        Console.WriteLine();
        Console.WriteLine("Order: first by string (Ordinal), then by number ascending.");
    }
}
