package catalog

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Catalog is the read-through pointer DB. A nil pool means the catalog is
// DISABLED (no DATABASE_URL, or the DB was unreachable at boot): every method is a
// safe no-op / miss so the bot runs exactly as today. The catalog must never
// hard-block the bot (master §8, D4).
type Catalog struct {
	pool *pgxpool.Pool
}

// New connects to the Supabase (Singapore) pooled Postgres at dsn. An empty dsn
// returns a disabled (nil-pool) Catalog with no error, so the caller can keep one
// non-nil *Catalog and let every call degrade to a miss. A non-empty dsn that
// fails to parse/connect/ping returns an error so the caller can log it and fall
// back to a disabled Catalog.
func New(ctx context.Context, dsn string) (*Catalog, error) {
	dsn = strings.TrimSpace(dsn)
	if dsn == "" {
		return &Catalog{}, nil
	}
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("catalog: parse DATABASE_URL: %w", err)
	}
	// Karen is read-heavy but low-QPS; a small pool is plenty (master Phase 2 doc).
	cfg.MaxConns = 5
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("catalog: connect: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("catalog: ping: %w", err)
	}
	return &Catalog{pool: pool}, nil
}

// Enabled reports whether the catalog has a live pool. Every public method also
// guards on this, so callers can invoke them unconditionally.
func (c *Catalog) Enabled() bool { return c != nil && c.pool != nil }

// Close releases the pool. Safe on a disabled/nil catalog.
func (c *Catalog) Close() {
	if c.Enabled() {
		c.pool.Close()
	}
}

// migrations is the §4.3 schema applied idempotently at boot. Each statement is
// run separately (pgx's default extended protocol allows one command per Exec).
var migrations = []string{
	`create table if not exists dumps (
		dump_id        bigint primary key,
		access_hash    bigint not null,
		title          text,
		kind           text not null default 'own',
		last_msg_id    int  not null default 0,
		added_at       timestamptz not null default now()
	)`,
	`create table if not exists tracks (
		id             bigserial primary key,
		dump_id        bigint not null references dumps(dump_id),
		message_id     int    not null,
		kind           text not null default 'track',
		apple_track_id bigint,
		apple_album_id bigint,
		apple_url      text,
		isrc           text,
		title          text,
		artist         text,
		album          text,
		format         text not null,
		variant        text not null,
		lyrics_sync    text not null default 'none',
		cover_embedded boolean not null default false,
		bitrate        int  not null default 0,
		sample_rate    int  not null default 0,
		bit_depth      int  not null default 0,
		storefront     text,
		duration_sec   int  not null default 0,
		size_bytes     bigint not null default 0,
		file_unique_id text,
		caption_raw    text,
		indexed_at     timestamptz not null default now(),
		unique (dump_id, message_id)
	)`,
	`create index if not exists tracks_adam_kind_var on tracks (apple_track_id, kind, variant)`,
	`create index if not exists tracks_isrc_kind_var on tracks (isrc, kind, variant)`,
	`create index if not exists tracks_title_artist on tracks (lower(title), lower(artist))`,
	`create index if not exists tracks_funiq on tracks (file_unique_id)`,
	// Per-day heavy-rip quota (charge-at-submit / refund-on-failure). The bot keeps an
	// in-memory authoritative copy; these rows are write-through + boot-restore only, so
	// a DB blip never lifts the quota (enforcement stays in memory). quota_day is the
	// 1PM-Dhaka day key the bot computes; state walks open→committed|refunded.
	`create table if not exists quota_charges (
		charge_id      text primary key,
		user_id        bigint not null,
		kind           text   not null,
		quota_day      date   not null,
		state          text   not null default 'open',
		releases_total int    not null default 0,
		delivered      int    not null default 0,
		created_at     timestamptz not null default now(),
		finalized_at   timestamptz
	)`,
	`create index if not exists quota_charges_day_state on quota_charges (quota_day, state)`,
	`create index if not exists quota_charges_user_day on quota_charges (user_id, quota_day, kind)`,
	// Gofile re-rip dedup. One row per Gofile link we deliver for a collection rip
	// (album/playlist/artist); a multi-part heavy rip writes several rows sharing one
	// content_key. A re-request within the 7-day window (Gofile links live ~7 days)
	// is served the existing link(s) instead of re-ripping + re-uploading, which the
	// VPS's capped bandwidth can't afford. expires_at gates lookups; rows are pruned
	// at the daily reset. Best-effort like the rest of the catalog: a DB blip fails
	// OPEN (the rip just proceeds), so this never blocks a legitimate request.
	`create table if not exists gofile_deliveries (
		id          bigserial primary key,
		content_key text not null,
		kind        text not null,
		content_id  text not null,
		variant     text not null,
		label       text not null default '',
		gofile_link text not null,
		user_id     bigint not null,
		username    text,
		created_at  timestamptz not null default now(),
		expires_at  timestamptz not null
	)`,
	`create index if not exists gofile_deliveries_key_exp on gofile_deliveries (content_key, expires_at)`,
}

// Migrate applies the schema. Idempotent; safe to call on every boot. No-op when
// the catalog is disabled.
func (c *Catalog) Migrate(ctx context.Context) error {
	if !c.Enabled() {
		return nil
	}
	for _, stmt := range migrations {
		if _, err := c.pool.Exec(ctx, stmt); err != nil {
			return fmt.Errorf("catalog migrate: %w", err)
		}
	}
	return nil
}

// Hit is a catalog match: the durable handle (DumpID, MsgID) plus the stored meta
// for display. Phase 2's HIT path passes (DumpID, MsgID) to Phase 1's
// DeliverFromDump (§4.5).
type Hit struct {
	DumpID int64
	MsgID  int
	Meta   TrackMeta
}

// trackCols is the full column list selected into a Hit. nullable columns are
// COALESCEd so we never scan into pointers.
const trackCols = `dump_id, message_id, kind,
	coalesce(apple_track_id,0), coalesce(apple_album_id,0), coalesce(apple_url,''),
	coalesce(isrc,''), coalesce(title,''), coalesce(artist,''), coalesce(album,''),
	format, variant, lyrics_sync, cover_embedded,
	bitrate, sample_rate, bit_depth, coalesce(storefront,''), duration_sec, size_bytes`

func scanHit(row pgx.Row) (Hit, error) {
	var h Hit
	m := &h.Meta
	err := row.Scan(
		&h.DumpID, &h.MsgID, &m.Kind,
		&m.AppleTrackID, &m.AppleAlbumID, &m.AppleURL,
		&m.ISRC, &m.Title, &m.Artist, &m.Album,
		&m.Format, &m.Variant, &m.LyricsSync, &m.CoverEmbedded,
		&m.Bitrate, &m.SampleRate, &m.BitDepth, &m.Storefront, &m.DurationSec, &m.SizeBytes,
	)
	return h, err
}

// Lookup returns the stored artifact for an exact (kind, identity, variant), or
// ok=false (§4.4, §4.5). Identity match priority (D6): apple_track_id first, then
// isrc. variant already pins format+quality, so no quality ordering is needed; ties
// break to the most recently indexed row. Disabled catalog → miss, no error.
func (c *Catalog) Lookup(ctx context.Context, kind string, adamID int64, isrc, variant string) (Hit, bool, error) {
	if !c.Enabled() {
		return Hit{}, false, nil
	}
	if kind == "" {
		kind = KindTrack
	}

	if adamID != 0 {
		h, ok, err := c.queryOne(ctx,
			`select `+trackCols+` from tracks
			 where apple_track_id = $1 and kind = $2 and variant = $3
			 order by indexed_at desc limit 1`,
			adamID, kind, variant)
		if err != nil || ok {
			return h, ok, err
		}
	}
	if isrc != "" {
		return c.queryOne(ctx,
			`select `+trackCols+` from tracks
			 where isrc = $1 and kind = $2 and variant = $3
			 order by indexed_at desc limit 1`,
			isrc, kind, variant)
	}
	return Hit{}, false, nil
}

// LookupAlbumZip returns the stored album-zip artifact for (apple_album_id,
// variant) (§4.6, D9). Album zips are keyed by album id, not track id, so this is
// a separate path from Lookup. Disabled catalog → miss, no error.
func (c *Catalog) LookupAlbumZip(ctx context.Context, albumID int64, variant string) (Hit, bool, error) {
	if !c.Enabled() || albumID == 0 {
		return Hit{}, false, nil
	}
	return c.queryOne(ctx,
		`select `+trackCols+` from tracks
		 where apple_album_id = $1 and kind = $2 and variant = $3
		 order by indexed_at desc limit 1`,
		albumID, KindAlbumZip, variant)
}

func (c *Catalog) queryOne(ctx context.Context, sql string, args ...any) (Hit, bool, error) {
	h, err := scanHit(c.pool.QueryRow(ctx, sql, args...))
	if errors.Is(err, pgx.ErrNoRows) {
		return Hit{}, false, nil
	}
	if err != nil {
		return Hit{}, false, fmt.Errorf("catalog lookup: %w", err)
	}
	return h, true, nil
}

// IndexInline records a row for a file we just uploaded ourselves (D8). We already
// hold the full meta, so the caption is reconstructed from it for caption_raw. No
// file_unique_id at upload time (the crawler fills it on a later pass if needed).
func (c *Catalog) IndexInline(ctx context.Context, dumpID int64, msgID int, m TrackMeta) error {
	return c.UpsertTrack(ctx, dumpID, msgID, m, "", FormatCaption(m))
}

// UpsertTrack inserts or updates one pointer row, keyed by (dump_id, message_id)
// so re-indexing the same message updates in place (§4.3). Used by both the inline
// indexer and the crawler. Disabled catalog → no-op.
func (c *Catalog) UpsertTrack(ctx context.Context, dumpID int64, msgID int, m TrackMeta, fileUniqueID, captionRaw string) error {
	if !c.Enabled() {
		return nil
	}
	if m.Kind == "" {
		m.Kind = KindTrack
	}
	if m.Variant == "" {
		m.Variant = VariantKey(m)
	}
	// Empty variant = a non-cacheable tier (e.g. binaural): never store a row, so
	// these always rip fresh and never produce a stale HIT.
	if m.Variant == "" {
		return nil
	}
	lyrics := m.LyricsSync
	if lyrics == "" {
		lyrics = "none"
	}
	_, err := c.pool.Exec(ctx, `
		insert into tracks (
			dump_id, message_id, kind, apple_track_id, apple_album_id, apple_url,
			isrc, title, artist, album, format, variant, lyrics_sync, cover_embedded,
			bitrate, sample_rate, bit_depth, storefront, duration_sec, size_bytes,
			file_unique_id, caption_raw
		) values (
			$1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22
		)
		on conflict (dump_id, message_id) do update set
			kind = excluded.kind,
			apple_track_id = excluded.apple_track_id,
			apple_album_id = excluded.apple_album_id,
			apple_url = excluded.apple_url,
			isrc = excluded.isrc,
			title = excluded.title,
			artist = excluded.artist,
			album = excluded.album,
			format = excluded.format,
			variant = excluded.variant,
			lyrics_sync = excluded.lyrics_sync,
			cover_embedded = excluded.cover_embedded,
			bitrate = excluded.bitrate,
			sample_rate = excluded.sample_rate,
			bit_depth = excluded.bit_depth,
			storefront = excluded.storefront,
			duration_sec = excluded.duration_sec,
			size_bytes = excluded.size_bytes,
			file_unique_id = excluded.file_unique_id,
			caption_raw = excluded.caption_raw,
			indexed_at = now()`,
		dumpID, msgID, m.Kind, nullI64(m.AppleTrackID), nullI64(m.AppleAlbumID), nullStr(m.AppleURL),
		nullStr(m.ISRC), m.Title, m.Artist, m.Album, m.Format, m.Variant, lyrics, m.CoverEmbedded,
		m.Bitrate, m.SampleRate, m.BitDepth, nullStr(m.Storefront), m.DurationSec, m.SizeBytes,
		nullStr(fileUniqueID), nullStr(captionRaw),
	)
	if err != nil {
		return fmt.Errorf("catalog upsert track (dump=%d msg=%d): %w", dumpID, msgID, err)
	}
	return nil
}

// UpsertDump records (or refreshes) a dump's row. The crawl checkpoint
// (last_msg_id) is intentionally preserved on conflict so re-registering a dump
// doesn't restart its crawl. Disabled catalog → no-op.
func (c *Catalog) UpsertDump(ctx context.Context, dumpID, accessHash int64, title, kind string) error {
	if !c.Enabled() {
		return nil
	}
	if kind == "" {
		kind = "own"
	}
	_, err := c.pool.Exec(ctx, `
		insert into dumps (dump_id, access_hash, title, kind)
		values ($1, $2, $3, $4)
		on conflict (dump_id) do update set
			access_hash = excluded.access_hash,
			title = excluded.title,
			kind = excluded.kind`,
		dumpID, accessHash, title, kind)
	if err != nil {
		return fmt.Errorf("catalog upsert dump %d: %w", dumpID, err)
	}
	return nil
}

// dumpCheckpoint returns the highest message_id crawled for a dump (0 if the dump
// is unknown or the catalog is disabled).
func (c *Catalog) dumpCheckpoint(ctx context.Context, dumpID int64) (int, error) {
	if !c.Enabled() {
		return 0, nil
	}
	var last int
	err := c.pool.QueryRow(ctx, `select last_msg_id from dumps where dump_id = $1`, dumpID).Scan(&last)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("catalog read checkpoint %d: %w", dumpID, err)
	}
	return last, nil
}

// setDumpCheckpoint advances a dump's crawl checkpoint. No-op when disabled.
func (c *Catalog) setDumpCheckpoint(ctx context.Context, dumpID int64, lastMsgID int) error {
	if !c.Enabled() {
		return nil
	}
	_, err := c.pool.Exec(ctx, `update dumps set last_msg_id = $2 where dump_id = $1`, dumpID, lastMsgID)
	if err != nil {
		return fmt.Errorf("catalog set checkpoint %d: %w", dumpID, err)
	}
	return nil
}

// nullI64 / nullStr map zero/empty values to SQL NULL so the lookup indexes aren't
// polluted with 0/"" sentinels (a foreign row with no adamID stays NULL, not 0).
func nullI64(v int64) any {
	if v == 0 {
		return nil
	}
	return v
}

func nullStr(s string) any {
	if s == "" {
		return nil
	}
	return s
}
