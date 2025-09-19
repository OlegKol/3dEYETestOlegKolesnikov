using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Diagnostics;
using System.Globalization;
using System.Text;

namespace ThreeDEyeSorting.Tests
{
    [TestClass]
    public class SorterTests
    {
        private static string GetRepoRoot() => Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, "..", "..", "..", ".."));

        public TestContext TestContext { get; set; } = null!;

        private static string Exe(string relative) => Path.Combine(GetRepoRoot(), relative);

        [TestMethod]
        public void SmallFile_SortsCorrectly()
        {
            string work = Path.Combine(Path.GetTempPath(), "three_eye_test_" + Guid.NewGuid().ToString("N"));
            Directory.CreateDirectory(work);
            string input = Path.Combine(work, "in.txt");
            string output = Path.Combine(work, "out.txt");
            string temp = Path.Combine(work, "temp");

            Run(Exe("Generator/bin/Release/net8.0/Generator.exe"), $"--out \"{input}\" --lines 50000 --threads 2");
            Run(Exe("Sorter/bin/Release/net8.0/Sorter.exe"), $"--in \"{input}\" --out \"{output}\" --temp \"{temp}\" --mem 128 --threads 2");

            Assert.IsTrue(File.Exists(output));
            AssertSorted(output);
        }

        [TestMethod]
        public void Handles_Duplicates_And_Ties()
        {
            string work = Path.Combine(Path.GetTempPath(), "three_eye_test_" + Guid.NewGuid().ToString("N"));
            Directory.CreateDirectory(work);
            string input = Path.Combine(work, "dup.txt");
            string output = Path.Combine(work, "dup_sorted.txt");
            string temp = Path.Combine(work, "t");

            // Craft data with exact duplicate strings and varying numbers
            File.WriteAllLines(input, new[] {
                "2. Apple",
                "1. Banana",
                "3. Apple",
                "5. Apple is tasty",
                "4. Apple",
                "2. Banana"
            }, Encoding.UTF8);

            Run(Exe("Sorter/bin/Release/net8.0/Sorter.exe"), $"--in \"{input}\" --out \"{output}\" --temp \"{temp}\" --mem 16 --threads 1");
            var lines = File.ReadAllLines(output, Encoding.UTF8);
            CollectionAssert.AreEqual(new[] {
                "2. Apple",
                "3. Apple",
                "4. Apple",
                "5. Apple is tasty",
                "1. Banana",
                "2. Banana"
            }, lines);
        }

        private static void Run(string exe, string args)
        {
            if (!File.Exists(exe)) Assert.Inconclusive($"Executable not found: {exe}. Build in Release first.");
            var psi = new ProcessStartInfo(exe, args)
            {
                UseShellExecute = false,
                CreateNoWindow = true
            };
            using var p = Process.Start(psi)!;
            p.WaitForExit();
            Assert.AreEqual(0, p.ExitCode, $"Process failed: {exe} {args}");
        }

        private static void AssertSorted(string file)
        {
            string? prevText = null;
            int prevNum = 0;
            foreach (var line in File.ReadLines(file, Encoding.UTF8))
            {
                int dot = line.IndexOf('.');
                Assert.IsTrue(dot > 0 && dot + 2 < line.Length && line[dot + 1] == ' ', $"Bad line: {line}");
                int num = int.Parse(line[..dot], CultureInfo.InvariantCulture);
                string text = line[(dot + 2)..];

                if (prevText is not null)
                {
                    int cmp = string.CompareOrdinal(prevText, text);
                    Assert.IsTrue(cmp < 0 || (cmp == 0 && num >= prevNum), $"Ordering violated: '{prevText}',{prevNum} -> '{text}',{num}");
                }
                prevText = text;
                prevNum = num;
            }
        }
    }
}