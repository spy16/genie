package portal

import (
	"bufio"
	"crypto/sha1"
	_ "embed"
	"encoding/hex"
	"errors"
	"fmt"
	"html/template"
	"log"
	"net/http"
	"net/url"
	"strings"

	"github.com/gorilla/mux"

	"github.com/spy16/genie"
)

var (
	//go:embed index.html
	indexHTML string

	indexTpl = template.Must(template.New("index").Parse(indexHTML))
)

// New returns a new web portal handler.
func New(q genie.Queue) http.Handler {
	r := mux.NewRouter()
	r.Handle("/", handleIndexGet(q)).Methods(http.MethodGet)
	r.Handle("/", handleUpload(q)).Methods(http.MethodPost)
	return r
}

func handleIndexGet(q genie.Queue) http.HandlerFunc {
	return func(wr http.ResponseWriter, req *http.Request) {
		d := map[string]interface{}{}
		if status := strings.TrimSpace(req.URL.Query().Get("status")); status != "" {
			d["status"] = status
		} else if errStr := strings.TrimSpace(req.URL.Query().Get("error")); errStr != "" {
			d["error"] = errStr
		} else {
			stats, err := q.Stats()
			if err != nil {
				d["error"] = fmt.Sprintf("stats unavailable: %v", err)
			} else {
				d["stats"] = doPercent(stats)
			}
		}

		if err := indexTpl.Execute(wr, d); err != nil {
			log.Printf("failed to serve index page: %v", err)
		}
	}
}

func handleUpload(q genie.Queue) http.HandlerFunc {
	return func(wr http.ResponseWriter, req *http.Request) {
		if err := req.ParseMultipartForm(10 << 20); err != nil {
			redirectErr(wr, req, err.Error())
			return
		}

		file, header, err := req.FormFile("jobFile")
		if err != nil {
			if errors.Is(err, http.ErrMissingFile) {
				redirectErr(wr, req, "please select a file")
			} else {
				redirectErr(wr, req, err.Error())
			}
			return
		}
		defer file.Close()

		var items []genie.Item
		sc := bufio.NewScanner(file)
		for line := 0; sc.Scan(); line++ {
			items = append(items, genie.Item{
				ID:      generateID(fmt.Sprintf("%s_%d", header.Filename, line)),
				Type:    req.FormValue("jobType"),
				Payload: sc.Text(),
				GroupID: header.Filename,
			})
		}

		if err := q.Push(req.Context(), items...); err != nil {
			redirectErr(wr, req, fmt.Sprintf("failed to stream-read upload (error: %v)", err))
			return
		}

		redirectMsg(wr, req, fmt.Sprintf("%d items queued successfully", len(items)))
	}
}

func redirectErr(wr http.ResponseWriter, req *http.Request, msg string) {
	http.Redirect(wr, req, "/?error="+url.QueryEscape(msg), http.StatusFound)
}

func redirectMsg(wr http.ResponseWriter, req *http.Request, msg string) {
	http.Redirect(wr, req, "/?status="+url.QueryEscape(msg), http.StatusFound)
}

func generateID(s string) string {
	h := sha1.New()
	h.Write([]byte(s))
	sha := h.Sum(nil) // "sha" is uint8 type, encoded in base16
	return hex.EncodeToString(sha[:10])
}

func doPercent(stats []genie.Stats) []percentStat {
	result := make([]percentStat, len(stats), len(stats))
	for i, stat := range stats {
		result[i] = percentStat{
			GroupID: stat.GroupID,
			Type:    stat.Type,
			Total:   stat.Total,
			Done:    float64(100 * stat.Done / stat.Total),
			Failed:  float64(100 * stat.Failed / stat.Total),
			Skipped: float64(100 * stat.Skipped / stat.Total),
		}
	}
	return result
}

type percentStat struct {
	GroupID string  `json:"group_id"`
	Type    string  `json:"type"`
	Total   int     `json:"total"`
	Done    float64 `json:"done"`
	Failed  float64 `json:"failed"`
	Skipped float64 `json:"skipped"`
}
