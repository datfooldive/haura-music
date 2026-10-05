package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/mdp/qrterminal/v3"
	whatsmeow "github.com/polymorfa/hypermeow"
	"github.com/polymorfa/hypermeow/proto/waCompanionReg"
	"github.com/polymorfa/hypermeow/store"
	"github.com/polymorfa/hypermeow/store/sqlstore"
	"github.com/polymorfa/hypermeow/types"
	"github.com/polymorfa/hypermeow/types/events"
	waLog "github.com/polymorfa/hypermeow/util/log"
	meowcaller "github.com/purpshell/meowcaller"
	"github.com/rs/zerolog"
	"google.golang.org/protobuf/proto"

	_ "modernc.org/sqlite"

	"haura-music/bot"
	"haura-music/stream"
)

func main() {
	cfg := loadConfig()
	level, err := zerolog.ParseLevel(cfg.LogLevel)
	if err != nil {
		level = zerolog.InfoLevel
	}
	logger := zerolog.New(zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: "15:04:05"}).
		Level(level).With().Timestamp().Logger()
	stream.YtDlpPath, stream.FFmpegPath = cfg.YtDlp, cfg.FFmpeg
	bot.IdleTimeout = cfg.IdleTimeout

	ctx, stop := signal.NotifyContext(logger.WithContext(context.Background()), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, cfg, logger, level); err != nil && !errors.Is(err, context.Canceled) {
		logger.Fatal().Err(err).Msg("haura-music failed")
	}
}

func run(ctx context.Context, cfg config, logger zerolog.Logger, level zerolog.Level) error {
	store.DeviceProps.Os = proto.String("Haura Music")
	store.DeviceProps.PlatformType = waCompanionReg.DeviceProps_CHROME.Enum()
	container, err := sqlstore.New(ctx, "sqlite",
		"file:"+cfg.DBPath+"?_pragma=foreign_keys(1)&_pragma=busy_timeout(5000)",
		waLog.Zerolog(logger).Sub("db"))
	if err != nil {
		return fmt.Errorf("open store: %w", err)
	}
	device, err := container.GetFirstDevice(ctx)
	if err != nil {
		return fmt.Errorf("load device: %w", err)
	}
	wa := whatsmeow.NewClient(device, waLog.Zerolog(logger.Level(max(level, zerolog.InfoLevel))).Sub("wa"))
	mc := meowcaller.NewClient(wa, meowcaller.WithLogger(logger.With().Str("mod", "voip").Logger()))

	manager := bot.NewManager(wa, mc, logger, cfg.Prefix)
	wa.AddEventHandler(manager.HandleEvent)

	mc.OnIncomingCall(func(call *meowcaller.Call) { _ = call.Reject() })

	if err := connect(ctx, wa, logger); err != nil {
		return err
	}
	defer wa.Disconnect()
	defer manager.Shutdown()
	logger.Info().Str("jid", wa.Store.GetJID().String()).Str("prefix", cfg.Prefix).Msg("haura-music ready")
	<-ctx.Done()
	return ctx.Err()
}

func connect(ctx context.Context, wa *whatsmeow.Client, logger zerolog.Logger) error {
	if wa.Store.ID == nil {
		qr, err := wa.GetQRChannel(ctx)
		if err != nil {
			return fmt.Errorf("open QR channel: %w", err)
		}
		if err := wa.Connect(); err != nil {
			return fmt.Errorf("connect: %w", err)
		}
		for evt := range qr {
			if evt.Event == "code" {
				fmt.Fprintln(os.Stderr, "Scan QR ini di WhatsApp → Perangkat tertaut:")
				qrterminal.GenerateHalfBlock(evt.Code, qrterminal.L, os.Stderr)
			} else {
				logger.Info().Str("event", evt.Event).Msg("pairing")
			}
		}
	} else if err := wa.Connect(); err != nil {
		return fmt.Errorf("connect: %w", err)
	}

	ready := make(chan struct{}, 1)
	id := wa.AddEventHandler(func(evt any) {
		if _, ok := evt.(*events.Connected); ok {
			select {
			case ready <- struct{}{}:
			default:
			}
		}
	})
	defer wa.RemoveEventHandler(id)
	timeout := time.After(time.Minute)
	for !(wa.IsConnected() && wa.IsLoggedIn()) {
		select {
		case <-ready:
		case <-timeout:
			return errors.New("timed out waiting for WhatsApp connection")
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if wa.Store.PushName == "" {
		wa.Store.PushName = "Haura Music"
	}
	if err := wa.SendPresence(ctx, types.PresenceAvailable); err != nil {
		logger.Warn().Err(err).Msg("send presence")
	}
	return nil
}
