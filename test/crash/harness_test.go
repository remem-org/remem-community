//go:build crash

package crash

import (
	"bufio"
	"bytes"
	"io"
	"os"
	"os/exec"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

// roleEnv names the scenario a child runs; dirEnv the data directory it runs
// it in. Both are read only by TestCrashChild.
const (
	roleEnv = "REMEM_CRASH_ROLE"
	dirEnv  = "REMEM_CRASH_DIR"
)

// scenarios are the child sides, by role. Each file that adds a crash test
// registers its child here in an init function.
var scenarios = map[string]func(t *testing.T, dir string){}

// TestCrashChild is the child process's entry point. Run directly, it skips:
// it only means something when a parent has named a role.
func TestCrashChild(t *testing.T) {
	role := os.Getenv(roleEnv)
	if role == "" {
		t.Skip("the child side of the crash tests; a parent runs it with " + roleEnv + " set")
	}
	run, ok := scenarios[role]
	if !ok {
		t.Fatalf("no crash scenario is registered as %q", role)
	}
	run(t, os.Getenv(dirEnv))
}

// child is one running child process.
type child struct {
	t     *testing.T
	cmd   *exec.Cmd
	lines chan string

	mu     sync.Mutex
	output bytes.Buffer // everything the child wrote, for a failure message
	done   bool
}

// spawn starts a child running role against dir. The child is killed when the
// test ends, whatever state it is in.
func spawn(t *testing.T, role, dir string, env ...string) *child {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^TestCrashChild$", "-test.count=1")
	// The parent's REMEM_* settings are not the child's business: the child
	// builds its configuration in code, and a legacy variable left in a shell
	// (CLAUDE.md warns about REMEM_API_KEY) has no place in it.
	for _, kv := range os.Environ() {
		if !strings.HasPrefix(kv, "REMEM_") {
			cmd.Env = append(cmd.Env, kv)
		}
	}
	cmd.Env = append(cmd.Env, roleEnv+"="+role, dirEnv+"="+dir)
	cmd.Env = append(cmd.Env, env...)

	c := &child{t: t, cmd: cmd, lines: make(chan string, 1024)}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	cmd.Stderr = &lockedWriter{c: c}
	if err := cmd.Start(); err != nil {
		t.Fatalf("starting the %s child: %v", role, err)
	}
	go c.read(stdout)
	t.Cleanup(c.kill)
	return c
}

func (c *child) read(r io.Reader) {
	sc := bufio.NewScanner(r)
	for sc.Scan() {
		line := sc.Text()
		c.mu.Lock()
		c.output.WriteString(line + "\n")
		c.mu.Unlock()
		c.lines <- line
	}
	close(c.lines)
}

// waitFor blocks until the child prints a line starting with prefix, and
// returns the rest of it.
func (c *child) waitFor(prefix string, timeout time.Duration) string {
	c.t.Helper()
	deadline := time.After(timeout)
	for {
		select {
		case line, ok := <-c.lines:
			if !ok {
				c.t.Fatalf("the child exited before printing %q:\n%s", prefix, c.transcript())
			}
			if rest, found := strings.CutPrefix(line, prefix); found {
				return rest
			}
		case <-deadline:
			c.t.Fatalf("the child did not print %q within %s:\n%s", prefix, timeout, c.transcript())
		}
	}
}

// kill sends SIGKILL — not SIGTERM: nothing may run in the child after this —
// and waits for the process to be reaped, which is what releases Pebble's lock.
func (c *child) kill() {
	c.mu.Lock()
	if c.done {
		c.mu.Unlock()
		return
	}
	c.done = true
	c.mu.Unlock()
	_ = c.cmd.Process.Signal(syscall.SIGKILL)
	_ = c.cmd.Wait()
}

// stop sends SIGTERM and waits, for the tests that need a clean restart rather
// than a crash.
func (c *child) stop(timeout time.Duration) {
	c.t.Helper()
	c.mu.Lock()
	c.done = true
	c.mu.Unlock()
	_ = c.cmd.Process.Signal(syscall.SIGTERM)
	exited := make(chan error, 1)
	go func() { exited <- c.cmd.Wait() }()
	select {
	case <-exited:
	case <-time.After(timeout):
		_ = c.cmd.Process.Signal(syscall.SIGKILL)
		<-exited
		c.t.Fatalf("the child did not stop within %s of SIGTERM:\n%s", timeout, c.transcript())
	}
}

// wait blocks until the child exits on its own, for a scenario that is meant
// to run to completion rather than be killed. A non-zero exit is a failure of
// the child's own test and is reported with its output.
func (c *child) wait(timeout time.Duration) {
	c.t.Helper()
	c.mu.Lock()
	c.done = true
	c.mu.Unlock()
	exited := make(chan error, 1)
	go func() { exited <- c.cmd.Wait() }()
	select {
	case err := <-exited:
		if err != nil {
			c.t.Fatalf("the child exited with %v:\n%s", err, c.transcript())
		}
	case <-time.After(timeout):
		_ = c.cmd.Process.Signal(syscall.SIGKILL)
		<-exited
		c.t.Fatalf("the child did not finish within %s:\n%s", timeout, c.transcript())
	}
}

func (c *child) transcript() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := c.output.String()
	if len(out) > 4000 {
		out = "…" + out[len(out)-4000:]
	}
	return out
}

type lockedWriter struct{ c *child }

func (w *lockedWriter) Write(p []byte) (int, error) {
	w.c.mu.Lock()
	defer w.c.mu.Unlock()
	return w.c.output.Write(p)
}
