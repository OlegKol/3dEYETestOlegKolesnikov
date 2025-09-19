using System;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Running;
using BenchmarkDotNet.Jobs;
using BenchmarkDotNet.Configs;

public class Program
{
    public static void Main(string[] args)
    {
        var cfg = DefaultConfig.Instance.AddJob(Job.Default
            .WithWarmupCount(1)
            .WithIterationCount(3)
            .WithLaunchCount(1));
        BenchmarkSwitcher.FromAssembly(typeof(Program).Assembly).Run(args, cfg);
    }
}

public sealed class ParsingBench
{
    private string[] _lines = Array.Empty<string>();

    [GlobalSetup]
    public void Setup()
    {
        var rnd = new Random(42);
        _lines = Enumerable.Range(0, 200_000).Select(i =>
        {
            int num = rnd.Next();
            string fruit = Fruits[rnd.Next(Fruits.Length)];
            return $"{num}. {fruit}";
        }).ToArray();
    }

    [Benchmark]
    public int Parse_All()
    {
        int ok = 0;
        foreach (var line in _lines)
            if (TryParse(line.AsSpan(), out _)) ok++;
        return ok;
    }

    private static readonly string[] Fruits = new[]
    {
        "Apple","Banana","Cherry","Date","Elderberry","Fig","Grape","Honeydew",
        "Kiwi","Lemon","Mango","Nectarine","Orange","Papaya","Quince","Raspberry",
        "Strawberry","Tangerine","Ugli","Vanilla","Watermelon","Xigua","Yam","Zucchini"
    };

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static bool TryParse(ReadOnlySpan<char> line, out (string Text, int Number) rec)
    {
        int dot = line.IndexOf('.');
        if (dot <= 0 || dot + 2 >= line.Length || line[dot + 1] != ' ')
        { rec = default; return false; }
        if (!int.TryParse(line[..dot], NumberStyles.None, CultureInfo.InvariantCulture, out int num))
        { rec = default; return false; }
        var text = new string(line[(dot + 2)..]);
        rec = (text, num);
        return true;
    }
}

public sealed class ComparerBench
{
    private (string Text, int Number)[] _records = Array.Empty<(string,int)>();


    [GlobalSetup]
    public void Setup()
    {
        var rnd = new Random(123);
        _records = Enumerable.Range(0, 200_000).Select(i =>
        {
            int num = rnd.Next();
            string text = rnd.NextDouble() < 0.5 ? "Apple" : "Banana";
            return (text, num);
        }).ToArray();
    }

    [Benchmark]
    public int Compare_Sequential()
    {
        int cmpSum = 0;
        for (int i = 1; i < _records.Length; i++)
        {
            cmpSum += Compare(_records[i-1], _records[i]);
        }
        return cmpSum;
    }

    private static int Compare((string Text, int Number) x, (string Text, int Number) y)
    {
        int c = string.CompareOrdinal(x.Text, y.Text);
        if (c != 0) return c;
        return x.Number.CompareTo(y.Number);
    }
}

public sealed class EndToEndBench
{
    private string _repo = null!;
    private string _input = null!;
    private string _output = null!;
    private string _temp = null!;

    [Params(20)] // MB
    public int SizeMb { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        _repo = GetRepoRoot();
        string work = Path.Combine(Path.GetTempPath(), "three_eye_bench_" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(work);
        _input = Path.Combine(work, "in.txt");
        _output = Path.Combine(work, "out.txt");
        _temp = Path.Combine(work, "temp");
    }

    [Benchmark(Description = "Generator.exe")]
    public int Generate()
    {
        string gen = Path.Combine(_repo, "Generator", "bin", "Release", "net8.0", "Generator.exe");
        if (!File.Exists(gen)) return -1;
        return Run(gen, $"--out \"{_input}\" --size {SizeMb}MB --threads {Math.Max(1, Environment.ProcessorCount/2)}");
    }

    [Benchmark(Description = "Sorter.exe")]
    public int Sort()
    {
        string sort = Path.Combine(_repo, "Sorter", "bin", "Release", "net8.0", "Sorter.exe");
        if (!File.Exists(sort)) return -1;
        return Run(sort, $"--in \"{_input}\" --out \"{_output}\" --temp \"{_temp}\" --mem 512 --threads {Math.Max(1, Environment.ProcessorCount/2)}");
    }

    private static int Run(string exe, string args)
    {
        var psi = new ProcessStartInfo(exe, args)
        {
            UseShellExecute = false,
            CreateNoWindow = true
        };
        using var p = Process.Start(psi)!;
        p.WaitForExit();
        return p.ExitCode;
    }

    private static string GetRepoRoot()
        => Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, "..", "..", "..", ".."));
}