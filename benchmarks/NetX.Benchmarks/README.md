# NetX receive-copy benchmark

Este benchmark compara os dois caminhos de cópia do receive:

- `DoubleCopy`: `ReadOnlySequence<byte>` → scratch de receive reutilizado → `MemoryOwner<byte>` final.
- `SingleCopy`: `ReadOnlySequence<byte>` → `MemoryOwner<byte>` final.

Os dados de entrada são sequências segmentadas determinísticas, com payloads de 1 KiB e 1 MiB. Cada execução valida todos os bytes copiados contra o payload esperado.

## Execução

```powershell
dotnet run --project benchmarks\NetX.Benchmarks\NetX.Benchmarks.csproj --configuration Release -- --filter *ReceiveCopyBenchmarks*
```

## Resultado ShortRun

Executado em 2026-08-23 com BenchmarkDotNet 0.13.5, .NET 10.0.11 e `ShortRun` (1 launch, 3 warmups, 3 iterações):

| Payload | DoubleCopy Mean | DoubleCopy Allocated | SingleCopy Mean | SingleCopy Allocated | SingleCopy / DoubleCopy |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 1 KiB | 638,1 ns | - | 641,9 ns | - | 1,01x |
| 1 MiB | 624.461,8 ns | 1024 B | 599.933,0 ns | 1025 B | 0,96x |

Conclusão: não houve regressão material no caminho `SingleCopy`. Em 1 KiB houve uma diferença nominal de +3,8 ns (+0,6%), mas os intervalos do ShortRun se sobrepõem amplamente. Em 1 MiB, o caminho single-copy foi aproximadamente 3,9% mais rápido. O BenchmarkDotNet reportou uma diferença de 1 B na alocação gerenciada do caso de 1 MiB (`1025 B` contra `1024 B`), sem alteração no pool ou no contrato de ownership.
