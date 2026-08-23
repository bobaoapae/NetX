using System.Buffers;
using BenchmarkDotNet.Attributes;
using CommunityToolkit.HighPerformance.Buffers;

namespace NetX.Benchmarks;

/// <summary>
/// Compares the receive-copy paths for a frame that has already been assembled in
/// a segmented <see cref="ReadOnlySequence{T}"/> by the receive pipe.
///
/// The legacy path copied the sequence into a receive scratch owner and then copied
/// the scratch owner into the final message owner. The current path copies the
/// sequence directly into the final message owner.
/// </summary>
[ShortRunJob]
[MemoryDiagnoser]
public class ReceiveCopyBenchmarks
{
    [Params(1_024, 1_048_576)]
    public int PayloadSize { get; set; }

    private ReadOnlySequence<byte> _segmentedPayload;
    private byte[] _expected = null!;
    private MemoryOwner<byte> _receiveScratch = null!;

    [GlobalSetup]
    public void Setup()
    {
        _expected = new byte[PayloadSize];
        _receiveScratch = MemoryOwner<byte>.Allocate(PayloadSize);

        var first = default(PayloadSegment);
        PayloadSegment? last = null;
        var offset = 0;
        var pattern = PayloadSize <= 1_024
            ? new[] { 128, 256, 640 }
            : new[] { 4_096, 8_192, 16_384, 32_768 };

        for (var patternIndex = 0; offset < PayloadSize; patternIndex++)
        {
            var segmentLength = Math.Min(pattern[patternIndex % pattern.Length], PayloadSize - offset);
            var segmentBytes = new byte[segmentLength];

            FillPayload(segmentBytes, offset);
            segmentBytes.AsSpan().CopyTo(_expected.AsSpan(offset));

            var segment = new PayloadSegment(segmentBytes, offset);
            if (last is null)
            {
                first = segment;
            }
            else
            {
                last.Append(segment);
            }

            last = segment;
            offset += segmentLength;
        }

        _segmentedPayload = new ReadOnlySequence<byte>(first!, 0, last!, last!.Memory.Length);
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        _receiveScratch.Dispose();
    }

    /// <summary>
    /// Legacy receive path: ReadOnlySequence -&gt; receive scratch owner -&gt; final owner.
    /// </summary>
    [Benchmark(Baseline = true)]
    public int DoubleCopy()
    {
        using var finalOwner = MemoryOwner<byte>.Allocate(PayloadSize);

        _segmentedPayload.CopyTo(_receiveScratch.Span);
        _receiveScratch.Span.CopyTo(finalOwner.Span);

        return Validate(finalOwner.Span);
    }

    /// <summary>
    /// Current receive path: ReadOnlySequence -&gt; final owner in one copy.
    /// </summary>
    [Benchmark]
    public int SingleCopy()
    {
        using var finalOwner = MemoryOwner<byte>.Allocate(PayloadSize);

        _segmentedPayload.CopyTo(finalOwner.Span);

        return Validate(finalOwner.Span);
    }

    private int Validate(ReadOnlySpan<byte> actual)
    {
        if (actual.Length != _expected.Length)
        {
            throw new InvalidOperationException(
                $"Receive copy returned {actual.Length} bytes; expected {_expected.Length}.");
        }

        var checksum = 17;
        for (var index = 0; index < actual.Length; index++)
        {
            if (actual[index] != _expected[index])
            {
                throw new InvalidOperationException($"Receive copy mismatch at byte {index}.");
            }

            checksum = unchecked((checksum * 31) + actual[index]);
        }

        return checksum;
    }

    private static void FillPayload(Span<byte> destination, int absoluteOffset)
    {
        for (var index = 0; index < destination.Length; index++)
        {
            destination[index] = unchecked((byte)((absoluteOffset + index) * 31 + 17));
        }
    }

    private sealed class PayloadSegment : ReadOnlySequenceSegment<byte>
    {
        public PayloadSegment(ReadOnlyMemory<byte> memory, long runningIndex)
        {
            Memory = memory;
            RunningIndex = runningIndex;
        }

        public void Append(PayloadSegment segment)
        {
            Next = segment;
        }
    }
}
