package glsolr

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"path"
)

// Response realizes format of Solr response
type Response struct {
	ResponseHeader struct {
		ZkConnected bool `json:"zkConnected"`
		Status      int  `json:"status"`
		QTime       int  `json:"QTime"`
		Params      struct {
			Q          string `json:"q"`
			Fl         string `json:"fl"`
			CursorMark string `json:"cursorMark"`
			Sort       string `json:"sort"`
			Rows       string `json:"rows"`
			Start      string `json:"start"`
		} `json:"params"`
	} `json:"responseHeader"`

	Error struct {
		Msg   string `json:"msg"`
		Trace string `json:"trace"`
	} `json:"error"`

	Response struct {
		NumFound int64             `json:"numFound"`
		Start    int64             `json:"start"`
		Docs     []json.RawMessage `json:"docs"`
	} `json:"response"`

	NextCursorMark string          `json:"nextCursorMark"`
	Highlighting   json.RawMessage `json:"highlighting"`
}

// Select makes select request to a Solr collection
func Select(cLink, user, passw string, params url.Values, headers map[string]string, client *http.Client) (*Response, error) {
	reqURL, err := url.Parse(cLink)
	if err != nil {
		return nil, err
	}

	reqURL.Path = path.Join(reqURL.Path, "/select")

	if params.Get("q") == "" {
		params.Set("q", "*:*")
	}

	reqURL.RawQuery = params.Encode()

	// Create request
	req, err := http.NewRequest(http.MethodGet, reqURL.String(), nil)
	if err != nil {
		return nil, err
	}

	// Add headers
	for k, v := range headers {
		req.Header.Add(k, v)
	}

	// Add autorization if user is set
	if user != "" {
		userPass := fmt.Sprintf("%s:%s", user, passw)
		basicCred := base64.StdEncoding.EncodeToString([]byte(userPass))
		req.Header.Add("Authorization", fmt.Sprintf("Basic %s", basicCred))
	}

	// Make the request
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}

	// Return a response status of  as a golang error
	if resp.StatusCode >= 400 && resp.StatusCode < 500 {
		return nil, errors.New(resp.Status)
	}

	respBody, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var solrResp Response
	err = json.Unmarshal(respBody, &solrResp)
	if err != nil {
		return nil, err
	}

	// Return Solr error as golang error
	if resp.StatusCode >= 500 {
		return &solrResp, errors.New(solrResp.Error.Msg)
	}
	return &solrResp, nil
}

// CursorSelect returns generator as channel to receive results of a cursor select query or error
func CursorSelect(cLink, user, passw string, params url.Values, headers map[string]string, client *http.Client) (<-chan interface{}, error) {
	// Only json format can be used to receive nextCursorMark value
	params.Set("wt", "json")

	// Set cursorMark parameter if that isn't set
	if params.Get("cursorMark") == "" {
		params.Set("cursorMark", "*")
	}

	// Create a channel to return the results
	ch := make(chan interface{})

	// Start goroutine to send the responses to channell
	go func() {
		for {
			res, err := Select(cLink, user, passw, params, headers, client)
			if err != nil {
				ch <- err
				break
			}

			// Chack if the response is last and exit
			if params.Get("cursorMark") == res.NextCursorMark {
				break
			}
			params.Set("cursorMark", res.NextCursorMark)
			ch <- res
		}
		close(ch)
	}()

	return ch, nil
}
