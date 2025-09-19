using System.Diagnostics;
using System.Text;

internal static class Program
{
    static int Main(string[] args)
    {
        Console.WriteLine("Performance smoke test (no external packages).");
        string mode = args.FirstOrDefault() ?? "50MB";
        long targetBytes = mode.EndsWith("MB", StringComparison.OrdinalIgnoreCase)
            ? long.Parse(mode[..^2]) * (1L<<20)
            : 50L * (1L<<20);

        string work = Path.Combine(Path.GetTempPath(), "three_eye_perf_" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(work);
        string input = Path.Combine(work, "in.txt");
        string output = Path.Combine(work, "out.txt");
        string temp = Path.Combine(work, "temp");

        string repo = GetRepoRoot();
        string gen = Path.Combine(repo, "Generator", "bin", "Release", "net8.0", "Generator.exe");
        string sort = Path.Combine(repo, "Sorter", "bin", "Release", "net8.0", "Sorter.exe");
        if (!File.Exists(gen) || !File.Exists(sort))
        {
            Console.WriteLine("Build the solution in Release first (Generator & Sorter).");
            return 2;
        }

        var sw = Stopwatch.StartNew();
        int ec1 = Run(gen, $"--out \"{input}\" --size {targetBytes>>20}MB --threads {Math.Max(1, Environment.ProcessorCount/2)}");
        if (ec1 != 0) return ec1;
        sw.Stop();
        var genTime = sw.Elapsed;
        long inputSize = new FileInfo(input).Length;

        sw.Restart();
        int ec2 = Run(sort, $"--in \"{input}\" --out \"{output}\" --temp \"{temp}\" --mem 1024 --threads {Math.Max(1, Environment.ProcessorCount/2)}");
        if (ec2 != 0) return ec2;
        sw.Stop();
        var sortTime = sw.Elapsed;
        long outSize = new FileInfo(output).Length;

        Console.WriteLine($"Generated {inputSize/(1<<20)} MB in {genTime}. Throughput ~ {inputSize/1e6/genTime.TotalSeconds:F1} MB/s");
        Console.WriteLine($"Sorted {outSize/(1<<20)} MB in {sortTime}.   Throughput ~ {outSize/1e6/sortTime.TotalSeconds:F1} MB/s");

        // Quick correctness check on a sample
        SampleCheck(output, sample: 200_000);
        Console.WriteLine("Sample order check: OK");

        return 0;
    }

    static int Run(string exe, string args)
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

    static string GetRepoRoot()
    {
        // Assume Performance/bin/Release/... current dir
        string cur = AppContext.BaseDirectory;
        // Go up to solution root
        return Path.GetFullPath(Path.Combine(cur, "..", "..", "..", ".."));
    }

    static void SampleCheck(string file, int sample)
    {
        int taken = 0;
        string? prevText = null;
        int prevNum = 0;
        foreach (var line in File.ReadLines(file, Encoding.UTF8))
        {
            if (taken++ > sample) break;
            int dot = line.IndexOf('.');
            if (dot <= 0 || dot + 2 >= line.Length || line[dot + 1] != ' ')
                throw new Exception("Bad line: " + line);
            int num = int.Parse(line[..dot]);
            string text = line[(dot + 2)..];
            if (prevText is not null)
            {
                int cmp = string.CompareOrdinal(prevText, text);
                if (!(cmp < 0 || (cmp == 0 && num >= prevNum)))
                    throw new Exception("Order violated");
            }
            prevText = text;
            prevNum = num;
        }
    }
}