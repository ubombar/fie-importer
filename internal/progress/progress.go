// Package progress draws the progress of an upload on a terminal: a smooth
// bar for the whole upload, one for the current file, the row rate and the
// time left. When the output is not a terminal it prints a plain line every
// few seconds instead.
package progress

import (
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/term"
)

// Display shows the progress of one upload. Its methods may be called from
// several goroutines.
type Display struct {
	out   io.Writer
	tty   bool
	color bool
	width int

	total    atomic.Int64
	sent     atomic.Int64
	bytes    atomic.Int64
	files    atomic.Int64
	fileIdx  atomic.Int64
	fileRows atomic.Int64
	fileRead atomic.Int64
	fileName atomic.Value
	phase    atomic.Value

	// byteTotal and byteRead drive the bar of a stream (StartStream).
	byteTotal int64
	byteRead  func() int64

	// query is set by StartQuery: the progress of a query on the server.
	query bool

	start   time.Time
	mu      sync.Mutex
	drawn   int // lines drawn by the last frame
	samples []sample
	tick    int
	stop    chan struct{}
	done    chan struct{}
}

type sample struct {
	at   time.Time
	sent int64
}

// New returns a display writing to f. Colors are used on a terminal unless
// NO_COLOR is set.
func New(f *os.File) *Display {
	fd := int(f.Fd()) //nolint:gosec // file descriptors fit in int
	tty := term.IsTerminal(fd)
	width := 100
	if w, _, err := term.GetSize(fd); err == nil && w > 0 {
		width = w
	}
	d := &Display{out: f, tty: tty, color: tty && os.Getenv("NO_COLOR") == "", width: width}
	d.fileName.Store("")
	d.phase.Store("")
	return d
}

// Header prints the title lines once, above the live area.
func (d *Display) Header(title, detail string) {
	_, _ = fmt.Fprintf(d.out, "%s %s\n", d.style(bold+cyan, "▶ "+title), d.style(dim, detail))
}

// Phase shows a spinner with text until Start is called.
func (d *Display) Phase(text string) { d.phase.Store(text) }

// Start begins showing progress towards total rows over files files.
func (d *Display) Start(total int64, files int) {
	d.total.Store(total)
	d.files.Store(int64(files))
	d.mu.Lock()
	d.start = time.Now()
	d.samples = []sample{{at: d.start}}
	d.mu.Unlock()
	d.phase.Store("")
}

// StartStream begins showing the progress of reading a stream of unknown
// row count: the bar follows read() against totalBytes (0 if unknown), and
// the counters show the rows sent.
func (d *Display) StartStream(totalBytes int64, read func() int64) {
	d.mu.Lock()
	d.byteTotal, d.byteRead = totalBytes, read
	d.mu.Unlock()
	d.Start(0, 0)
}

// StartQuery begins showing the progress of a query running on the server:
// rows read against the server's estimate, both set with Rows.
func (d *Display) StartQuery() {
	d.mu.Lock()
	d.query = true
	d.mu.Unlock()
	d.Start(0, 0)
}

// Rows sets the rows a query has read and its estimated total.
func (d *Display) Rows(read, total int64) {
	d.sent.Store(read)
	d.total.Store(total)
}

// Run redraws until Stop is called. Start it in its own goroutine.
func (d *Display) Run() {
	d.stop, d.done = make(chan struct{}), make(chan struct{})
	go func() {
		defer close(d.done)
		every := 100 * time.Millisecond
		if !d.tty {
			every = 5 * time.Second
		}
		ticker := time.NewTicker(every)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				d.draw(false)
			case <-d.stop:
				d.draw(true)
				return
			}
		}
	}()
}

// Stop draws the final frame and stops redrawing.
func (d *Display) Stop() {
	if d.stop == nil {
		return
	}
	close(d.stop)
	<-d.done
	d.stop = nil
}

// File announces the file being read: its position (1-based) and row count.
func (d *Display) File(index int, name string, rows int64) {
	d.fileIdx.Store(int64(index))
	d.fileName.Store(name)
	d.fileRows.Store(rows)
	d.fileRead.Store(0)
}

// Read counts rows read from the current file.
func (d *Display) Read(n int64) { d.fileRead.Add(n) }

// Sent counts rows confirmed by ClickHouse and their approximate size.
func (d *Display) Sent(rows, bytes int64) {
	d.sent.Add(rows)
	d.bytes.Add(bytes)
}

func (d *Display) draw(final bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.tick++
	var lines []string
	if phase := d.phase.Load().(string); phase != "" {
		lines = []string{d.style(cyan, spinner[d.tick%len(spinner)]) + " " + phase}
	} else if !d.start.IsZero() {
		lines = d.frame(final)
	}
	if !d.tty {
		if len(lines) > 0 {
			_, _ = fmt.Fprintln(d.out, strings.TrimSpace(strings.Join(lines, " | ")))
		}
		return
	}
	var b strings.Builder
	if d.drawn > 0 {
		fmt.Fprintf(&b, "\x1b[%dA", d.drawn)
	}
	for _, l := range lines {
		b.WriteString("\x1b[2K" + l + "\n")
	}
	for i := len(lines); i < d.drawn; i++ {
		b.WriteString("\x1b[2K\n")
	}
	if extra := d.drawn - len(lines); extra > 0 {
		fmt.Fprintf(&b, "\x1b[%dA", extra)
	}
	d.drawn = len(lines)
	_, _ = io.WriteString(d.out, b.String())
}

func (d *Display) frame(final bool) []string {
	if d.byteRead != nil {
		return d.streamFrame(final)
	}
	if d.query {
		return d.queryFrame(final)
	}
	now := time.Now()
	total, sent := d.total.Load(), d.sent.Load()
	elapsed := now.Sub(d.start)
	rate := d.rate(now, sent)
	frac := 1.0
	if total > 0 {
		frac = float64(sent) / float64(total)
	}
	barWidth := max(10, min(48, d.width-42))

	eta := "—"
	switch {
	case final:
		eta = "done"
	case rate > 0:
		eta = clock(time.Duration(float64(total-sent) / rate * float64(time.Second)))
	}
	byteRate := float64(d.bytes.Load()) / max(elapsed.Seconds(), 1e-3)

	overall := fmt.Sprintf(" %s %s  %s %s",
		d.style(cyan, bar(frac, barWidth)),
		d.style(bold, fmt.Sprintf("%5.1f%%", frac*100)),
		d.style(bold, human(sent)), d.style(dim, "/ "+human(total)+" rows"))
	stats := d.style(dim, fmt.Sprintf("   %s rows/s · ≈%s/s to ClickHouse · elapsed %s · ETA %s",
		human(int64(rate)), bytesHuman(byteRate), clock(elapsed), eta))
	lines := []string{overall, stats}

	if !final {
		rows, read := d.fileRows.Load(), d.fileRead.Load()
		ff := 1.0
		if rows > 0 {
			ff = float64(read) / float64(rows)
		}
		lines = append(lines, fmt.Sprintf("   %s %s  %s %s",
			d.style(dim, fmt.Sprintf("file %d/%d", d.fileIdx.Load(), d.files.Load())),
			d.fileName.Load().(string),
			d.style(blue, bar(ff, 12)), d.style(dim, fmt.Sprintf("%3.0f%%", ff*100))))
	}
	return lines
}

func (d *Display) streamFrame(final bool) []string {
	now := time.Now()
	sent, read := d.sent.Load(), d.byteRead()
	elapsed := now.Sub(d.start)
	rate := d.rate(now, sent)
	barWidth := max(10, min(48, d.width-48))
	frac, eta := 0.0, "—"
	if d.byteTotal > 0 {
		frac = min(float64(read)/float64(d.byteTotal), 1)
		if frac > 0 && !final {
			eta = clock(time.Duration(float64(elapsed) * (1 - frac) / frac))
		}
	}
	if final {
		frac, eta = 1, "done"
	}
	size := bytesHuman(float64(read))
	if d.byteTotal > 0 {
		size += " / " + bytesHuman(float64(d.byteTotal))
	}
	overall := fmt.Sprintf(" %s %s  %s %s",
		d.style(cyan, bar(frac, barWidth)), d.style(bold, fmt.Sprintf("%5.1f%%", frac*100)),
		d.style(bold, human(sent)+" rows"), d.style(dim, "· "+size+" read"))
	stats := d.style(dim, fmt.Sprintf("   %s rows/s · ≈%s/s to ClickHouse · elapsed %s · ETA %s",
		human(int64(rate)), bytesHuman(float64(d.bytes.Load())/max(elapsed.Seconds(), 1e-3)), clock(elapsed), eta))
	return []string{overall, stats}
}

func (d *Display) queryFrame(final bool) []string {
	now := time.Now()
	total, read := d.total.Load(), d.sent.Load()
	elapsed := now.Sub(d.start)
	rate := d.rate(now, read)
	barWidth := max(10, min(48, d.width-42))
	frac, eta := 0.0, "—"
	if total > 0 {
		frac = min(float64(read)/float64(total), 1)
		if rate > 0 && !final {
			eta = clock(time.Duration(float64(max(total-read, 0)) / rate * float64(time.Second)))
		}
	}
	if final {
		frac, eta = 1, "done"
	}
	overall := fmt.Sprintf(" %s %s  %s %s",
		d.style(cyan, bar(frac, barWidth)), d.style(bold, fmt.Sprintf("%5.1f%%", frac*100)),
		d.style(bold, human(read)), d.style(dim, "/ ≈"+human(total)+" rows read"))
	stats := d.style(dim, fmt.Sprintf("   %s rows/s on the server · elapsed %s · ETA %s", human(int64(rate)), clock(elapsed), eta))
	return []string{overall, stats}
}

// rate is the row rate over the last few seconds.
func (d *Display) rate(now time.Time, sent int64) float64 {
	d.samples = append(d.samples, sample{at: now, sent: sent})
	for len(d.samples) > 2 && now.Sub(d.samples[1].at) > 3*time.Second {
		d.samples = d.samples[1:]
	}
	first := d.samples[0]
	if dt := now.Sub(first.at).Seconds(); dt > 0.2 {
		return float64(sent-first.sent) / dt
	}
	return 0
}

const (
	bold = "\x1b[1m"
	dim  = "\x1b[2m"
	cyan = "\x1b[36m"
	blue = "\x1b[34m"
	red  = "\x1b[31m"
	grn  = "\x1b[32m"
	off  = "\x1b[0m"
)

var spinner = []string{"⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"}

func (d *Display) style(code, s string) string {
	if !d.color {
		return s
	}
	return code + s + off
}

// Success and Failure style a closing line.
func (d *Display) Success(s string) string { return d.style(bold+grn, "✔ ") + s }
func (d *Display) Failure(s string) string { return d.style(bold+red, "✘ ") + s }

// Dim styles secondary text.
func (d *Display) Dim(s string) string { return d.style(dim, s) }

// bar draws frac of width cells with eighth-block resolution.
func bar(frac float64, width int) string {
	frac = min(max(frac, 0), 1)
	eighths := int(frac * float64(width*8))
	full, part := eighths/8, eighths%8
	partial := []string{"", "▏", "▎", "▍", "▌", "▋", "▊", "▉"}
	s := strings.Repeat("█", full)
	if full < width {
		pad := width - full
		if part > 0 {
			s += partial[part]
			pad--
		}
		s += strings.Repeat(" ", pad)
	}
	return "▕" + s + "▏"
}

func human(n int64) string {
	switch {
	case n >= 1e9:
		return fmt.Sprintf("%.2fG", float64(n)/1e9)
	case n >= 1e6:
		return fmt.Sprintf("%.1fM", float64(n)/1e6)
	case n >= 1e4:
		return fmt.Sprintf("%.0fk", float64(n)/1e3)
	}
	return fmt.Sprint(n)
}

func bytesHuman(b float64) string {
	switch {
	case b >= 1e9:
		return fmt.Sprintf("%.1f GB", b/1e9)
	case b >= 1e6:
		return fmt.Sprintf("%.1f MB", b/1e6)
	}
	return fmt.Sprintf("%.0f kB", b/1e3)
}

func clock(d time.Duration) string {
	s := int(d.Round(time.Second).Seconds())
	if s >= 3600 {
		return fmt.Sprintf("%d:%02d:%02d", s/3600, s/60%60, s%60)
	}
	return fmt.Sprintf("%d:%02d", s/60, s%60)
}
