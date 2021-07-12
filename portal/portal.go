package portal

import (
	_ "embed"
	"html/template"
	"log"
	"net/http"

	"github.com/spy16/genie"
)

var (
	//go:embed index.html
	indexHTML string

	indexTpl = template.Must(template.New("index").Parse(indexHTML))
)

// New returns a new web portal handler.
func New(q genie.Queue) http.Handler {
	return http.HandlerFunc(func(wr http.ResponseWriter, req *http.Request) {
		d := map[string]interface{}{}
		if err := indexTpl.Execute(wr, d); err != nil {
			log.Printf("failed to serve index page: %v", err)
		}
	})
}
