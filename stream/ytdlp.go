package stream

import (
	"bufio"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	meowcaller "github.com/purpshell/meowcaller"
)

var (
	YtDlpPath  = "yt-dlp"
	FFmpegPath = "ffmpeg"
)

type Track struct {
	Title       string
	URL         string
	Duration    time.Duration
	RequestedBy string
}

func Resolve(ctx context.Context, query string) (Track, error) {
	target := query
	if !strings.HasPrefix(query, "http://") && !strings.HasPrefix(query, "https://") {
		target = "ytsearch1:" + query
	}
	out, err := exec.CommandContext(ctx, YtDlpPath,
		"--no-playlist", "--no-warnings", "-j", "--", target).Output()
	if err != nil {
		var ee *exec.ExitError
		if errors.As(err, &ee) && len(ee.Stderr) > 0 {
			return Track{}, fmt.Errorf("yt-dlp: %s", strings.TrimSpace(string(ee.Stderr)))
		}
		return Track{}, fmt.Errorf("yt-dlp: %w", err)
	}

	line, _, _ := strings.Cut(strings.TrimSpace(string(out)), "\n")
	var info struct {
		Title       string  `json:"title"`
		WebpageURL  string  `json:"webpage_url"`
		OriginalURL string  `json:"original_url"`
		Duration    float64 `json:"duration"`
	}
	if err := json.Unmarshal([]byte(line), &info); err != nil {
		return Track{}, fmt.Errorf("parse yt-dlp output: %w", err)
	}
	url := info.WebpageURL
	if url == "" {
		url = info.OriginalURL
	}
	if url == "" {
		return Track{}, errors.New("no result")
	}
	return Track{
		Title:    info.Title,
		URL:      url,
		Duration: time.Duration(info.Duration * float64(time.Second)),
	}, nil
}

const frameBytes = meowcaller.FrameSamples * 4

const audioFilter = "highpass=f=70,lowpass=f=7200,loudnorm=I=-20:TP=-4:LRA=11,alimiter=limit=0.6"

func ffmpegArgs(input string) []string {
	return []string{
		"-hide_banner", "-loglevel", "error", "-i", input, "-vn",
		"-af", audioFilter,
		"-f", "f32le", "-ac", "1", "-ar", fmt.Sprint(meowcaller.SampleRate), "pipe:1",
	}
}

const (
	bufferFrames  = 100
	prebufferFrms = 17
)

type Source struct {
	cancel   context.CancelFunc
	ytdlp    *exec.Cmd
	ffmpeg   *exec.Cmd
	frames   chan []float32
	primed   bool
	gain     *atomic.Uint32
	played   atomic.Int64
	finished atomic.Bool
	closeMu  sync.Once
}

func newSource(cancel context.CancelFunc, ytdlp, ffmpeg *exec.Cmd, out io.Reader, gain *atomic.Uint32) *Source {
	s := &Source{cancel: cancel, ytdlp: ytdlp, ffmpeg: ffmpeg, frames: make(chan []float32, bufferFrames), gain: gain}
	go s.fill(bufio.NewReaderSize(out, frameBytes*16))
	return s
}

func (s *Source) fill(r io.Reader) {
	defer func() {
		s.finished.Store(true)
		close(s.frames)
	}()
	buf := make([]byte, frameBytes)
	for {
		n, err := io.ReadFull(r, buf)
		if n == 0 {
			return
		}
		clear(buf[n:])
		frame := make([]float32, meowcaller.FrameSamples)
		for i := range frame {
			frame[i] = math.Float32frombits(binary.LittleEndian.Uint32(buf[i*4:]))
		}
		s.frames <- frame
		if err != nil {
			return
		}
	}
}

func Open(track Track, gain *atomic.Uint32) (*Source, error) {
	ctx, cancel := context.WithCancel(context.Background())
	ytdlp := exec.CommandContext(ctx, YtDlpPath,
		"--no-playlist", "--no-warnings", "-q", "-f", "bestaudio/best", "-o", "-", "--", track.URL)
	ffmpeg := exec.CommandContext(ctx, FFmpegPath, ffmpegArgs("pipe:0")...)

	pipe, err := ytdlp.StdoutPipe()
	if err != nil {
		cancel()
		return nil, err
	}
	ffmpeg.Stdin = pipe
	out, err := ffmpeg.StdoutPipe()
	if err != nil {
		cancel()
		return nil, err
	}
	if err := ytdlp.Start(); err != nil {
		cancel()
		return nil, fmt.Errorf("start yt-dlp: %w", err)
	}
	if err := ffmpeg.Start(); err != nil {
		cancel()
		_ = ytdlp.Wait()
		return nil, fmt.Errorf("start ffmpeg: %w", err)
	}
	return newSource(cancel, ytdlp, ffmpeg, out, gain), nil
}

func OpenFile(path string, gain *atomic.Uint32) (*Source, error) {
	ctx, cancel := context.WithCancel(context.Background())
	ffmpeg := exec.CommandContext(ctx, FFmpegPath, ffmpegArgs(path)...)
	out, err := ffmpeg.StdoutPipe()
	if err != nil {
		cancel()
		return nil, err
	}
	if err := ffmpeg.Start(); err != nil {
		cancel()
		return nil, fmt.Errorf("start ffmpeg: %w", err)
	}
	return newSource(cancel, nil, ffmpeg, out, gain), nil
}

func (s *Source) ReadFrame() ([]float32, error) {
	if !s.primed {

		if len(s.frames) < prebufferFrms && !s.finished.Load() {
			return make([]float32, meowcaller.FrameSamples), nil
		}
		s.primed = true
	}
	var frame []float32
	select {
	case f, ok := <-s.frames:
		if !ok {
			return nil, io.EOF
		}
		frame = f
	default:
		s.primed = false
		return make([]float32, meowcaller.FrameSamples), nil
	}
	g := float32(1)
	if s.gain != nil {
		g = math.Float32frombits(s.gain.Load())
	}
	for i, v := range frame {
		frame[i] = max(-1, min(1, v*g))
	}
	s.played.Add(1)
	return frame, nil
}

func (s *Source) Elapsed() time.Duration {
	return time.Duration(s.played.Load()) * meowcaller.FrameSamples * time.Second / meowcaller.SampleRate
}

func (s *Source) Close() error {
	s.closeMu.Do(func() {
		s.cancel()
		go func() {
			for range s.frames {
			}
		}()
		_ = s.ffmpeg.Wait()
		if s.ytdlp != nil {
			_ = s.ytdlp.Wait()
		}
	})
	return nil
}
