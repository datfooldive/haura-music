package bot

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	"github.com/polymorfa/hypermeow/types"
	meowcaller "github.com/purpshell/meowcaller"
	"github.com/rs/zerolog"

	"haura-music/stream"
)

var IdleTimeout = 3 * time.Minute

type Session struct {
	m     *Manager
	group types.JID
	log   zerolog.Logger

	advMu    sync.Mutex
	mu       sync.Mutex
	call     *meowcaller.Call
	player   *meowcaller.Player
	queue    []stream.Track
	current  *stream.Track
	src      *stream.Source
	loop     bool
	gain     atomic.Uint32
	idle     *time.Timer
	starting bool
	closed   bool
}

func newSession(m *Manager, group types.JID) *Session {
	s := &Session{m: m, group: group, log: m.log.With().Str("group", group.String()).Logger()}
	s.gain.Store(math.Float32bits(1))
	return s
}

func (s *Session) say(text string) { s.m.send(s.group, text) }

func (s *Session) Enqueue(ctx context.Context, t stream.Track) (position int, err error) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return 0, errors.New("session closed")
	}
	s.queue = append(s.queue, t)
	position = len(s.queue)
	needCall := s.call == nil && !s.starting
	if needCall {
		s.starting = true
	}
	idle := s.current == nil && s.call != nil
	from := s.src
	s.mu.Unlock()

	if needCall {
		if err := s.startCall(ctx); err != nil {
			s.mu.Lock()
			s.starting = false
			s.queue = s.queue[:0]
			s.mu.Unlock()
			s.m.drop(s)
			return 0, err
		}
		return 0, nil
	}
	if idle {
		s.advance(from)
		return 0, nil
	}
	return position, nil
}

func (s *Session) startCall(ctx context.Context) error {
	call, err := s.m.placeCall(ctx, s.group)
	if err != nil {
		return err
	}
	player := meowcaller.NewPlayer()
	call.Subscribe(player)

	player.OnFinish(func() {
		s.mu.Lock()
		src := s.src
		s.mu.Unlock()
		go s.onTrackEnd(src)
	})
	call.OnEnd(func(reason string) {
		s.log.Info().Str("reason", reason).Msg("call ended")
		go s.shutdown(false)
	})

	s.mu.Lock()
	s.call = call
	s.player = player
	s.starting = false
	s.mu.Unlock()
	s.say("📞 Memulai call — angkat/join call untuk dengar musiknya.")
	s.advance(nil)
	return nil
}

func (s *Session) onTrackEnd(finished *stream.Source) {
	s.mu.Lock()

	if s.closed || s.player == nil || s.src != finished {
		s.mu.Unlock()
		return
	}
	if s.loop && s.current != nil {
		s.queue = append([]stream.Track{*s.current}, s.queue...)
	}
	s.mu.Unlock()
	s.advance(finished)
}

func (s *Session) advance(from *stream.Source) {
	s.advMu.Lock()
	defer s.advMu.Unlock()
	for {
		s.mu.Lock()
		if s.closed || s.player == nil || s.src != from {
			s.mu.Unlock()
			return
		}
		if len(s.queue) == 0 {
			s.current, s.src = nil, nil
			s.player.Stop()
			s.armIdleLocked()
			s.mu.Unlock()
			return
		}
		t := s.queue[0]
		s.queue = s.queue[1:]
		s.stopIdleLocked()
		s.mu.Unlock()

		src, err := stream.Open(t, &s.gain)
		if err != nil {
			s.log.Error().Err(err).Str("track", t.Title).Msg("open stream")
			s.say(fmt.Sprintf("⚠️ Gagal memutar *%s*: %v", t.Title, err))
			continue
		}

		s.mu.Lock()
		if s.closed {
			s.mu.Unlock()
			_ = src.Close()
			return
		}
		s.current, s.src = &t, src
		s.player.Play(src)
		s.mu.Unlock()
		s.say(fmt.Sprintf("🎶 Now playing: *%s* [%s]\nreq: %s", t.Title, fmtDur(t.Duration), t.RequestedBy))
		return
	}
}

func (s *Session) armIdleLocked() {
	s.stopIdleLocked()
	s.idle = time.AfterFunc(IdleTimeout, func() {
		s.say("💤 Antrian kosong, keluar dari call.")
		s.shutdown(true)
	})
}

func (s *Session) stopIdleLocked() {
	if s.idle != nil {
		s.idle.Stop()
		s.idle = nil
	}
}

func (s *Session) Skip() bool {
	s.mu.Lock()
	playing := s.current != nil
	from := s.src
	s.mu.Unlock()
	if playing {
		s.advance(from)
	}
	return playing
}

func (s *Session) Pause() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.player == nil || s.player.State() != meowcaller.PlayerPlaying {
		return false
	}
	s.player.Pause()
	return true
}

func (s *Session) Resume() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.player == nil || s.player.State() != meowcaller.PlayerPaused {
		return false
	}
	s.player.Resume()
	return true
}

func (s *Session) shutdown(hangup bool) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.closed = true
	s.stopIdleLocked()
	call, player := s.call, s.player
	s.queue, s.current, s.src = nil, nil, nil
	s.mu.Unlock()

	if player != nil {
		player.Stop()
	}
	if hangup && call != nil {
		if err := call.Hangup(); err != nil {
			s.log.Warn().Err(err).Msg("hangup")
		}
	}
	s.m.drop(s)
}

func (s *Session) Snapshot() (cur *stream.Track, elapsed time.Duration, paused bool, queue []stream.Track, loop bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.current != nil {
		c := *s.current
		cur = &c
	}
	if s.src != nil {
		elapsed = s.src.Elapsed()
	}
	paused = s.player != nil && s.player.State() == meowcaller.PlayerPaused
	return cur, elapsed, paused, append([]stream.Track(nil), s.queue...), s.loop
}

func (s *Session) Remove(i int) (stream.Track, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if i < 1 || i > len(s.queue) {
		return stream.Track{}, false
	}
	t := s.queue[i-1]
	s.queue = append(s.queue[:i-1], s.queue[i:]...)
	return t, true
}

func (s *Session) Clear() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := len(s.queue)
	s.queue = nil
	return n
}

func (s *Session) Shuffle() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	rand.Shuffle(len(s.queue), func(i, j int) { s.queue[i], s.queue[j] = s.queue[j], s.queue[i] })
	return len(s.queue)
}

func (s *Session) ToggleLoop() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.loop = !s.loop
	return s.loop
}

func (s *Session) SetVolume(percent int) {
	s.gain.Store(math.Float32bits(float32(percent) / 100))
}

func (s *Session) Volume() int {
	return int(math.Round(float64(math.Float32frombits(s.gain.Load())) * 100))
}

func fmtDur(d time.Duration) string {
	if d <= 0 {
		return "live"
	}
	d = d.Round(time.Second)
	h, m, sec := int(d.Hours()), int(d.Minutes())%60, int(d.Seconds())%60
	if h > 0 {
		return fmt.Sprintf("%d:%02d:%02d", h, m, sec)
	}
	return fmt.Sprintf("%d:%02d", m, sec)
}
