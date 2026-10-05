package bot

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	whatsmeow "github.com/polymorfa/hypermeow"
	"github.com/polymorfa/hypermeow/proto/waE2E"
	"github.com/polymorfa/hypermeow/types"
	"github.com/polymorfa/hypermeow/types/events"
	meowcaller "github.com/purpshell/meowcaller"
	"github.com/rs/zerolog"
	"google.golang.org/protobuf/proto"

	"haura-music/stream"
)

type Manager struct {
	wa     *whatsmeow.Client
	mc     *meowcaller.Client
	log    zerolog.Logger
	prefix string

	mu       sync.Mutex
	sessions map[types.JID]*Session
}

func NewManager(wa *whatsmeow.Client, mc *meowcaller.Client, log zerolog.Logger, prefix string) *Manager {
	return &Manager{wa: wa, mc: mc, log: log, prefix: prefix, sessions: map[types.JID]*Session{}}
}

func (m *Manager) session(group types.JID, create bool) *Session {
	m.mu.Lock()
	defer m.mu.Unlock()
	s := m.sessions[group]
	if s == nil && create {
		s = newSession(m, group)
		m.sessions[group] = s
	}
	return s
}

func (m *Manager) drop(s *Session) {
	m.mu.Lock()
	if m.sessions[s.group] == s {
		delete(m.sessions, s.group)
	}
	m.mu.Unlock()
}

func (m *Manager) Shutdown() {
	m.mu.Lock()
	all := make([]*Session, 0, len(m.sessions))
	for _, s := range m.sessions {
		all = append(all, s)
	}
	m.mu.Unlock()
	for _, s := range all {
		s.shutdown(true)
	}
}

func (m *Manager) send(chat types.JID, text string) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if _, err := m.wa.SendMessage(ctx, chat, &waE2E.Message{Conversation: proto.String(text)}); err != nil {
		m.log.Warn().Err(err).Msg("send message")
	}
}

func (m *Manager) reply(evt *events.Message, text string) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	msg := &waE2E.Message{ExtendedTextMessage: &waE2E.ExtendedTextMessage{
		Text: proto.String(text),
		ContextInfo: &waE2E.ContextInfo{
			StanzaID:      proto.String(evt.Info.ID),
			Participant:   proto.String(evt.Info.Sender.ToNonAD().String()),
			QuotedMessage: evt.Message,
		},
	}}
	if _, err := m.wa.SendMessage(ctx, evt.Info.Chat, msg); err != nil {
		m.log.Warn().Err(err).Msg("send reply")
		return
	}
	m.log.Debug().Str("chat", evt.Info.Chat.String()).Str("text", text).Msg("replied")
}

func (m *Manager) HandleEvent(evt any) {

	if u, ok := evt.(*events.UndecryptableMessage); ok {
		m.log.Warn().Str("chat", u.Info.Chat.String()).Str("from", u.Info.Sender.String()).
			Bool("unavailable", u.IsUnavailable).Msg("could not decrypt incoming message")
		return
	}
	msg, ok := evt.(*events.Message)
	if !ok {
		return
	}
	text := msg.Message.GetConversation()
	if text == "" {
		text = msg.Message.GetExtendedTextMessage().GetText()
	}
	text = strings.TrimSpace(text)
	m.log.Debug().Str("chat", msg.Info.Chat.String()).Bool("group", msg.Info.IsGroup).
		Str("from", msg.Info.Sender.String()).Str("type", msg.Info.Type).Str("text", text).Msg("message")
	if !msg.Info.IsGroup {
		return
	}
	if !strings.HasPrefix(text, m.prefix) {
		return
	}
	cmd, args, _ := strings.Cut(strings.TrimPrefix(text, m.prefix), " ")
	m.log.Info().Str("group", msg.Info.Chat.String()).Str("from", msg.Info.Sender.String()).
		Str("cmd", cmd).Str("args", args).Msg("command")
	go m.dispatch(msg, strings.ToLower(cmd), strings.TrimSpace(args))
}

const helpText = `🎵 *Haura Music*
.play <judul/url> — putar / tambah ke antrian
.skip — lewati lagu
.pause / .resume
.np — lagu yang sedang diputar
.queue — lihat antrian
.remove <no> — hapus dari antrian
.clear — kosongkan antrian
.shuffle — acak antrian
.loop — ulang lagu sekarang (on/off)
.volume <0-200>
.stop — berhenti & keluar call`

func (m *Manager) dispatch(evt *events.Message, cmd, args string) {
	group := evt.Info.Chat
	s := m.session(group, false)
	notInCall := func() { m.reply(evt, "Bot belum ada di call. Pakai .play <judul> dulu.") }

	switch cmd {
	case "help", "menu":
		m.reply(evt, helpText)

	case "play", "p":
		if args == "" {
			m.reply(evt, "Contoh: .play never gonna give you up")
			return
		}
		m.reply(evt, "🔎 Mencari: "+args)
		ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
		defer cancel()
		t, err := stream.Resolve(ctx, args)
		if err != nil {
			m.log.Warn().Err(err).Str("query", args).Msg("resolve")
			m.reply(evt, "❌ Tidak ketemu: "+err.Error())
			return
		}
		t.RequestedBy = evt.Info.PushName
		if t.RequestedBy == "" {
			t.RequestedBy = evt.Info.Sender.User
		}
		pos, err := m.session(group, true).Enqueue(ctx, t)
		if err != nil {
			m.log.Error().Err(err).Msg("start group call")
			m.reply(evt, "❌ Gagal memulai call: "+err.Error())
			return
		}
		if pos > 0 {
			m.reply(evt, fmt.Sprintf("➕ Ditambahkan ke antrian #%d: *%s* [%s]", pos, t.Title, fmtDur(t.Duration)))
		}

	case "skip", "s", "next":
		if s == nil {
			notInCall()
		} else if !s.Skip() {
			m.reply(evt, "Tidak ada lagu yang diputar.")
		} else {
			m.reply(evt, "⏭️ Skipped.")
		}

	case "pause":
		if s == nil {
			notInCall()
		} else if s.Pause() {
			m.reply(evt, "⏸️ Paused.")
		} else {
			m.reply(evt, "Tidak ada yang sedang diputar.")
		}

	case "resume", "unpause":
		if s == nil {
			notInCall()
		} else if s.Resume() {
			m.reply(evt, "▶️ Resumed.")
		} else {
			m.reply(evt, "Player tidak sedang di-pause.")
		}

	case "stop", "leave", "dc":
		if s == nil {
			notInCall()
			return
		}
		s.shutdown(true)
		m.reply(evt, "⏹️ Stopped, keluar dari call.")

	case "np", "nowplaying":
		if s == nil {
			notInCall()
			return
		}
		cur, elapsed, paused, _, loop := s.Snapshot()
		if cur == nil {
			m.reply(evt, "Tidak ada lagu yang diputar.")
			return
		}
		state := "▶️"
		if paused {
			state = "⏸️"
		}
		extra := ""
		if loop {
			extra = " 🔂"
		}
		m.reply(evt, fmt.Sprintf("%s *%s*%s\n%s / %s\nreq: %s\n%s",
			state, cur.Title, extra, fmtDur(elapsed), fmtDur(cur.Duration), cur.RequestedBy, cur.URL))

	case "queue", "q":
		if s == nil {
			notInCall()
			return
		}
		cur, _, _, queue, _ := s.Snapshot()
		var b strings.Builder
		if cur != nil {
			fmt.Fprintf(&b, "🎶 Now: *%s* [%s]\n", cur.Title, fmtDur(cur.Duration))
		}
		if len(queue) == 0 {
			b.WriteString("Antrian kosong.")
		}
		const maxShown = 15
		var total time.Duration
		for i, t := range queue {
			total += t.Duration
			if i < maxShown {
				fmt.Fprintf(&b, "%d. %s [%s] — %s\n", i+1, t.Title, fmtDur(t.Duration), t.RequestedBy)
			}
		}
		if len(queue) > maxShown {
			fmt.Fprintf(&b, "…dan %d lagi\n", len(queue)-maxShown)
		}
		if len(queue) > 0 {
			fmt.Fprintf(&b, "Total: %d lagu, %s", len(queue), fmtDur(total))
		}
		m.reply(evt, strings.TrimSpace(b.String()))

	case "remove", "rm":
		if s == nil {
			notInCall()
			return
		}
		n, err := strconv.Atoi(args)
		if err != nil {
			m.reply(evt, "Contoh: .remove 2")
			return
		}
		if t, ok := s.Remove(n); ok {
			m.reply(evt, "🗑️ Dihapus: *"+t.Title+"*")
		} else {
			m.reply(evt, "Nomor antrian tidak valid.")
		}

	case "clear":
		if s == nil {
			notInCall()
			return
		}
		m.reply(evt, fmt.Sprintf("🧹 %d lagu dihapus dari antrian.", s.Clear()))

	case "shuffle":
		if s == nil {
			notInCall()
			return
		}
		m.reply(evt, fmt.Sprintf("🔀 %d lagu diacak.", s.Shuffle()))

	case "loop", "repeat":
		if s == nil {
			notInCall()
			return
		}
		if s.ToggleLoop() {
			m.reply(evt, "🔂 Loop: ON")
		} else {
			m.reply(evt, "Loop: OFF")
		}

	case "volume", "vol", "v":
		if s == nil {
			notInCall()
			return
		}
		if args == "" {
			m.reply(evt, fmt.Sprintf("🔊 Volume: %d%%", s.Volume()))
			return
		}
		v, err := strconv.Atoi(strings.TrimSuffix(args, "%"))
		if err != nil || v < 0 || v > 200 {
			m.reply(evt, "Volume harus 0-200.")
			return
		}
		s.SetVolume(v)
		m.reply(evt, fmt.Sprintf("🔊 Volume: %d%%", v))

	default:
		m.reply(evt, fmt.Sprintf("Command %s%s tidak dikenal. Ketik %shelp.", m.prefix, cmd, m.prefix))
	}
}

func (m *Manager) placeCall(ctx context.Context, group types.JID) (*meowcaller.Call, error) {
	info, err := m.wa.GetGroupInfo(ctx, group)
	if err != nil {
		return nil, fmt.Errorf("get group info: %w", err)
	}
	self := map[string]bool{m.wa.Store.GetJID().User: true, m.wa.Store.GetLID().User: true}
	var remote []types.JID
	for _, p := range info.Participants {
		if self[p.JID.User] || self[p.PhoneNumber.User] || self[p.LID.User] {
			continue
		}
		remote = append(remote, p.JID.ToNonAD())
	}
	switch {
	case len(remote) == 0:
		return nil, errors.New("grup tidak punya anggota lain untuk ditelepon")
	case len(remote) == 1:
		m.log.Info().Str("target", remote[0].String()).Msg("group has one member, placing 1:1 call")
		return m.mc.Call(ctx, remote[0].String())
	default:
		return m.mc.GroupCallByID(ctx, group.String())
	}
}
