package main

import (
	"os"
	"time"
)

type config struct {
	Prefix      string
	DBPath      string
	YtDlp       string
	FFmpeg      string
	LogLevel    string
	IdleTimeout time.Duration
}

func loadConfig() config {
	idle, err := time.ParseDuration(env("HAURA_IDLE_TIMEOUT", "3m"))
	if err != nil {
		idle = 3 * time.Minute
	}
	return config{
		Prefix:      env("HAURA_PREFIX", "."),
		DBPath:      env("HAURA_DB", "haura.db"),
		YtDlp:       env("HAURA_YTDLP", "yt-dlp"),
		FFmpeg:      env("HAURA_FFMPEG", "ffmpeg"),
		LogLevel:    env("HAURA_LOG", "info"),
		IdleTimeout: idle,
	}
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
