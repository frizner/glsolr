package glsolr

// glsolr supports only JSON format to request Solr

import (
	"encoding/base64"
	"encoding/json"
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

// handleResponse processes Solr response and returns json data or(and) an error
func handleResponse(resp *http.Response) (solrResp *Response, err error) {
	respBody, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	json.Unmarshal(respBody, &solrResp)
	if err != nil {
		return nil, err
	}

	// If status is ok, return json data
	if resp.StatusCode < 400 {
		return solrResp, nil
	}

	if msg := solrResp.Error.Msg; msg != "" {
		err = fmt.Errorf("%s. %s", resp.Status, msg)
	} else {
		err = fmt.Errorf("%s", resp.Status)
	}

	return nil, err
}

// Select makes select request to a Solr collection
func Select(cLink, user, passw string, params url.Values, headers map[string]string, client *http.Client) (solrResp *Response, err error) {
	reqURL, err := url.Parse(cLink)
	if err != nil {
		return nil, err
	}

	reqURL.Path = path.Join(reqURL.Path, "/select")

	// Only JSON format is supported
	if params == nil {
		params = url.Values{}
	}
	params.Set("wt", "json")

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

	return handleResponse(resp)
}

// CursorSelect returns generator as channel to receive results of a cursor select query or error
func CursorSelect(cLink, user, passw string, params url.Values, headers map[string]string, client *http.Client) (<-chan interface{}, error) {

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
