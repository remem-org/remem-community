package embedding

// ThreadSource says where the model's intra-op thread count came from.
type ThreadSource int

const (
	// ThreadsDerived means embedding.intra_op_threads was zero and the count is
	// the number of CPUs this process may use.
	ThreadsDerived ThreadSource = iota
	// ThreadsConfigured means an operator set the count.
	ThreadsConfigured
)

func (s ThreadSource) String() string {
	if s == ThreadsConfigured {
		return "configured"
	}
	return "derived"
}

// IntraOpThreads resolves embedding.intra_op_threads against the CPUs this
// process may use.
//
// The model runs one inference at a time, so its own thread pool is the only
// parallelism inference has, and the right size for it is the CPUs the process
// is allowed to run on. The caller passes that as available. The server passes
// runtime.GOMAXPROCS(0), which since Go 1.25 is the smaller of the affinity
// mask and the cgroup CPU quota, and which an explicit GOMAXPROCS overrides.
// Zero is not left to ONNX Runtime, because its own default counts the host's
// cores and ignores a quota: a 2-CPU container on a 32-core host would run 32
// threads and be throttled.
//
// A positive configured value is used as given, even above available, because
// an operator who set it has measured something this function cannot.
func IntraOpThreads(configured, available int) (int, ThreadSource) {
	if configured > 0 {
		return configured, ThreadsConfigured
	}
	return max(available, 1), ThreadsDerived
}
