// Copyright 2010 Gary Burd
//
// Licensed under the Apache License, Version 2.0 (the "License"): you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

// The twitterstream package implements the basic functionality for accessing
// the Twitter streaming APIs.  See http://dev.twitter.com/pages/streaming_api
// for information on the Twitter streaming APIs.
package twitterstream

import (
	"bufio"
	"bytes"
	"github.com/garyburd/twister/web"
	"http"
	"json"
	"log"
	"net"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"
	"encoding/base64"
)

// BasicAuthHeader returns user and pwd encoded as an HTTP basic auth header.
func BasicAuthHeader(user, pwd string) string {
	s := user + ":" + pwd
	p := make([]byte, base64.StdEncoding.EncodedLen(len(s)))
	base64.StdEncoding.Encode(p, []byte(s))
	return "Basic " + string(p)
}

// TwitterStream manages the connection to Twitter. The stream automatically
// reconnects to Twitter if there is an error with the connection.
type TwitterStream struct {
	conn net.Conn
	r    *bufio.Reader
	req  []byte
	addr string
}

// New returns a new TwitterStream. The caller is responsible for providing
// authentication information in header or param.
func New(urlString string, header web.StringsMap, param web.StringsMap) *TwitterStream {
	url, err := http.ParseURL(urlString)
	if err != nil {
		panic("bad url: " + urlString)
	}

	addr := url.Host
	if strings.LastIndex(addr, ":") <= strings.LastIndex(addr, "]") {
		addr = addr + ":80"
	}

	body := param.FormEncodedBytes()

	header.Set(web.HeaderHost, url.Host)
	header.Set(web.HeaderConnection, "close") // disable chunk encoding in response
	header.Set(web.HeaderContentLength, strconv.Itoa(len(body)))
	header.Set(web.HeaderContentType, "application/x-www-form-urlencoded")

	var b bytes.Buffer
	b.WriteString("POST ")
	b.WriteString(url.RawPath)
	b.WriteString(" HTTP/1.1\r\n")
	header.WriteHttpHeader(&b)
	b.Write(body)

	ts := &TwitterStream{addr: addr, req: b.Bytes()}
	ts.connect()
	return ts
}

// Close releases all resources used by the stream.
func (ts *TwitterStream) Close() {
	if ts.conn != nil {
		ts.conn.Close()
		ts.conn = nil
	}
	ts.r = nil
}

var responseLineRegexp = regexp.MustCompile("^HTTP/[0-9.]+ ([0-9]+) ")

func (ts *TwitterStream) connect() {
	var err os.Error
	ts.conn, err = net.Dial("tcp", "", ts.addr)
	if err != nil {
		log.Println("twitterstream: dial failed ", err)
		ts.Close()
		return
	}

	if _, err := ts.conn.Write(ts.req); err != nil {
		log.Println("twitterstream: error writing request: ", err)
		ts.Close()
		return
	}

	ts.r, _ = bufio.NewReaderSize(ts.conn, 8192)
	p, err := ts.r.ReadSlice('\n')
	if err != nil {
		log.Println("twittersteam: error reading response: ", err)
		ts.Close()
		return
	}

	m := responseLineRegexp.FindSubmatch(p)
	if m == nil {
		log.Println("twitterstream: bad response line")
		ts.Close()
		return
	}

	if string(m[1]) != "200" {
		log.Println("twiterstream: bad response code: ", string(m[1]))
		ts.Close()
		return
	}

	for {
		p, err = ts.r.ReadSlice('\n')
		if err != nil {
			log.Println("twittersteam: error reading header: ", err)
			ts.Close()
			return
		}
		if len(p) <= 2 {
			break
		}
	}

	log.Println("twitterstream: connected to ", ts.addr)
}

// Next returns the next entity from the stream. 
func (ts *TwitterStream) Next(v interface{}) os.Error {
	var p []byte
	for {
		timeout := int64(1e9)
		for ts.r == nil {
			time.Sleep(timeout)
			ts.connect()
			timeout = timeout * 2
			if timeout > 60e9 {
				timeout = 60e9
			}
		}
		var err os.Error
		p, err = ts.r.ReadSlice('\n')
		if err == nil {
			break
		}
		ts.Close()
	}
	return json.Unmarshal(p, v)
}
