package ampapi

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
)

// ArtistAlbum is the slimmed-down view of one release in an artist's `albums`
// relationship — just the fields /check needs to categorize releases and tally
// tracks. Apple returns trackCount/isSingle/isCompilation right here, so the
// whole discography can be counted from the (paginated) list with no per-album
// tracklist fetch.
type ArtistAlbum struct {
	ID            string
	Name          string
	ArtistName    string
	ReleaseDate   string
	TrackCount    int
	IsSingle      bool
	IsCompilation bool
	GenreNames    []string
	AudioTraits   []string
	ContentRating string
}

// artistAlbumsPage mirrors the shape of /artists/{id}/albums (only the fields we
// consume). Kept local so the public ArtistAlbum stays decoupled from the wire
// format.
type artistAlbumsPage struct {
	Next string `json:"next"`
	Data []struct {
		ID         string `json:"id"`
		Attributes struct {
			Name          string   `json:"name"`
			ArtistName    string   `json:"artistName"`
			ReleaseDate   string   `json:"releaseDate"`
			TrackCount    int      `json:"trackCount"`
			IsSingle      bool     `json:"isSingle"`
			IsCompilation bool     `json:"isCompilation"`
			GenreNames    []string `json:"genreNames"`
			AudioTraits   []string `json:"audioTraits"`
			ContentRating string   `json:"contentRating"`
		} `json:"attributes"`
	} `json:"data"`
}

func artistRequest(rawURL, token string) (*http.Request, error) {
	req, err := http.NewRequest("GET", rawURL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", token))
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")
	req.Header.Set("Origin", "https://music.apple.com")
	return req, nil
}

// GetArtistAlbums paginates the full `albums` relationship for an artist,
// following Apple's `next` cursor until exhausted. Returns every release with
// the attributes /check needs to categorize and tally it.
func GetArtistAlbums(storefront, artistID, language, token string) ([]ArtistAlbum, error) {
	var err error
	if token == "" {
		if token, err = GetToken(); err != nil {
			return nil, err
		}
	}

	var albums []ArtistAlbum
	offset := 0
	for {
		req, err := artistRequest(fmt.Sprintf("https://amp-api.music.apple.com/v1/catalog/%s/artists/%s/albums", storefront, artistID), token)
		if err != nil {
			return nil, err
		}
		q := url.Values{}
		q.Set("limit", "100")
		q.Set("offset", fmt.Sprintf("%d", offset))
		if language != "" {
			q.Set("l", language)
		}
		req.URL.RawQuery = q.Encode()

		do, err := http.DefaultClient.Do(req)
		if err != nil {
			return nil, err
		}
		if do.StatusCode != http.StatusOK {
			do.Body.Close()
			return nil, errors.New(do.Status)
		}
		page := new(artistAlbumsPage)
		if err := json.NewDecoder(do.Body).Decode(page); err != nil {
			do.Body.Close()
			return nil, err
		}
		do.Body.Close()

		for _, d := range page.Data {
			albums = append(albums, ArtistAlbum{
				ID:            d.ID,
				Name:          d.Attributes.Name,
				ArtistName:    d.Attributes.ArtistName,
				ReleaseDate:   d.Attributes.ReleaseDate,
				TrackCount:    d.Attributes.TrackCount,
				IsSingle:      d.Attributes.IsSingle,
				IsCompilation: d.Attributes.IsCompilation,
				GenreNames:    d.Attributes.GenreNames,
				AudioTraits:   d.Attributes.AudioTraits,
				ContentRating: d.Attributes.ContentRating,
			})
		}
		if page.Next == "" {
			break
		}
		offset += 100
	}
	return albums, nil
}

// ArtistSectionItem is one entry (album or music video) in an artist section
// listing — just the id and display name needed to enqueue and report it.
type ArtistSectionItem struct {
	ID   string
	Name string
}

// ListArtistSection paginates any artist relationship or view (e.g. "albums",
// "view/full-albums", "view/singles", "music-videos") and returns its items in
// the order Apple serves them. apiPath is appended to /artists/{id}/ verbatim,
// so callers pass either a relationship name or a "view/<name>" path. The
// {next, data:[{id, attributes:{name}}]} shape is common to album-views and the
// music-videos relationship, so one decoder serves all sections.
func ListArtistSection(storefront, artistID, apiPath, language, token string) ([]ArtistSectionItem, error) {
	var err error
	if token == "" {
		if token, err = GetToken(); err != nil {
			return nil, err
		}
	}

	var items []ArtistSectionItem
	offset := 0
	for {
		req, err := artistRequest(fmt.Sprintf("https://amp-api.music.apple.com/v1/catalog/%s/artists/%s/%s", storefront, artistID, apiPath), token)
		if err != nil {
			return nil, err
		}
		q := url.Values{}
		q.Set("limit", "100")
		q.Set("offset", fmt.Sprintf("%d", offset))
		if language != "" {
			q.Set("l", language)
		}
		req.URL.RawQuery = q.Encode()

		do, err := http.DefaultClient.Do(req)
		if err != nil {
			return nil, err
		}
		if do.StatusCode != http.StatusOK {
			do.Body.Close()
			return nil, errors.New(do.Status)
		}
		page := new(struct {
			Next string `json:"next"`
			Data []struct {
				ID         string `json:"id"`
				Attributes struct {
					Name string `json:"name"`
				} `json:"attributes"`
			} `json:"data"`
		})
		if err := json.NewDecoder(do.Body).Decode(page); err != nil {
			do.Body.Close()
			return nil, err
		}
		do.Body.Close()

		for _, d := range page.Data {
			items = append(items, ArtistSectionItem{ID: d.ID, Name: d.Attributes.Name})
		}
		if page.Next == "" {
			break
		}
		offset += 100
	}
	return items, nil
}

// CountArtistRelationship paginates a count-only relationship/view on an artist
// (e.g. "music-videos", "playlists", "view/appears-on-albums") and returns the
// total number of items. A non-200 response or transport error yields (0, err)
// — callers that treat a missing bucket as "0 of those" can simply ignore the
// error, since not every artist/storefront exposes every relationship.
func CountArtistRelationship(storefront, artistID, relationship, language, token string) (int, error) {
	var err error
	if token == "" {
		if token, err = GetToken(); err != nil {
			return 0, err
		}
	}

	total := 0
	offset := 0
	for {
		req, err := artistRequest(fmt.Sprintf("https://amp-api.music.apple.com/v1/catalog/%s/artists/%s/%s", storefront, artistID, relationship), token)
		if err != nil {
			return total, err
		}
		q := url.Values{}
		q.Set("limit", "100")
		q.Set("offset", fmt.Sprintf("%d", offset))
		if language != "" {
			q.Set("l", language)
		}
		req.URL.RawQuery = q.Encode()

		do, err := http.DefaultClient.Do(req)
		if err != nil {
			return total, err
		}
		if do.StatusCode != http.StatusOK {
			do.Body.Close()
			return total, errors.New(do.Status)
		}
		// Only the cursor + item count matter here.
		page := new(struct {
			Next string            `json:"next"`
			Data []json.RawMessage `json:"data"`
		})
		if err := json.NewDecoder(do.Body).Decode(page); err != nil {
			do.Body.Close()
			return total, err
		}
		do.Body.Close()

		total += len(page.Data)
		if page.Next == "" {
			break
		}
		offset += 100
	}
	return total, nil
}
