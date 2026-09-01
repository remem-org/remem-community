//! Shared configuration for the reproducible memory-create benchmark.

pub const WRITE_BENCHMARK_CONCURRENCY: usize = 50;
pub const DEFAULT_WRITE_BENCHMARK_TOTAL_WRITES: usize = 1_000;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WriteBenchmarkConfig {
    total_writes: usize,
}

impl WriteBenchmarkConfig {
    pub fn from_environment() -> Result<Self, String> {
        let value = std::env::var("REMEM_BENCHMARK_TOTAL_WRITES").ok();
        Self::from_total_writes(value.as_deref())
    }

    pub fn from_total_writes(value: Option<&str>) -> Result<Self, String> {
        let total_writes = match value {
            Some(value) => value.parse::<usize>().map_err(|_| {
                format!(
                    "REMEM_BENCHMARK_TOTAL_WRITES must be a positive multiple of {WRITE_BENCHMARK_CONCURRENCY}"
                )
            })?,
            None => DEFAULT_WRITE_BENCHMARK_TOTAL_WRITES,
        };

        if total_writes == 0 || total_writes % WRITE_BENCHMARK_CONCURRENCY != 0 {
            return Err(format!(
                "REMEM_BENCHMARK_TOTAL_WRITES must be a positive multiple of {WRITE_BENCHMARK_CONCURRENCY}"
            ));
        }

        Ok(Self { total_writes })
    }

    pub fn total_writes(self) -> usize {
        self.total_writes
    }

    pub fn concurrency(self) -> usize {
        WRITE_BENCHMARK_CONCURRENCY
    }

    pub fn waves(self) -> usize {
        self.total_writes / WRITE_BENCHMARK_CONCURRENCY
    }
}
