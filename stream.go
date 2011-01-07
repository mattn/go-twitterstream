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
	"crypto/tls"
	"github.com/garyburd/twister/oauth"
	"github.com/garyburd/twister/web"
	"http"
	"io/ioutil"
	"log"
	"net"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// TwitterStream manages the connection to Twitter. The stream automatically
// reconnects to Twitter if there is an error with the connection.
type TwitterStream struct {
	waitUntil      int64
	chunkRemaining int64
	chunkState     int
	conn           net.Conn
	r              *bufio.Reader
	url            string
	param          web.StringsMap
	oauthClient    *oauth.Client
	accessToken    *oauth.Credentials
}

// New returns a new TwitterStream. 
func New(oauthClient *oauth.Client, accessToken *oauth.Credentials, url string, param web.StringsMap) *TwitterStream {
	return &TwitterStream{oauthClient: oauthClient, accessToken: accessToken, url: url, param: param}
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

const (
	stateStart = iota
	stateEnd
	stateNormal
)

func (ts *TwitterStream) error(msg string, err os.Error) {
	log.Println("twitterstream:", msg, err)
	ts.Close()
}

func (ts *TwitterStream) connect() {
	var err os.Error
	log.Println("twitterstream: connecting to", ts.url)

	url, err := http.ParseURL(ts.url)
	if err != nil {
		panic("bad url: " + ts.url)
	}

	addr := url.Host
	if strings.LastIndex(addr, ":") <= strings.LastIndex(addr, "]") {
		if url.Scheme == "http" {
			addr = addr + ":80"
		} else {
			addr = addr + ":443"
		}
	}

	param := web.StringsMap{}
	for key, values := range ts.param {
		param[key] = values
	}
	ts.oauthClient.SignParam(ts.accessToken, "POST", ts.url, param)

	body := param.FormEncodedBytes()

	header := web.NewStringsMap(
		web.HeaderHost, url.Host,
		web.HeaderContentLength, strconv.Itoa(len(body)),
		web.HeaderContentType, "application/x-www-form-urlencoded")

	var request bytes.Buffer
	request.WriteString("POST ")
	request.WriteString(url.RawPath)
	request.WriteString(" HTTP/1.1\r\n")
	header.WriteHttpHeader(&request)
	request.Write(body)

	if url.Scheme == "http" {
		ts.conn, err = net.Dial("tcp", "", addr)
		if err != nil {
			ts.error("dial failed ", err)
			return
		}
	} else {
		ts.conn, err = tls.Dial("tcp", "", addr, nil)
		if err != nil {
			ts.error("dial failed ", err)
			return
		}
		if err = ts.conn.(*tls.Conn).VerifyHostname(addr[:strings.LastIndex(addr, ":")]); err != nil {
			ts.error("could not verify host", err)
			return
		}
	}

	// Set timeout to detect dead connection. Twitter sends at least one line
	// to the response every 30 seconds.
	err = ts.conn.SetReadTimeout(60e9)
	if err != nil {
		ts.error("set read timeout failed", err)
		return
	}

	if _, err := ts.conn.Write(request.Bytes()); err != nil {
		ts.error("error writing request: ", err)
		return
	}

	ts.r, _ = bufio.NewReaderSize(ts.conn, 8192)
	p, err := ts.r.ReadSlice('\n')
	if err != nil {
		ts.error("error reading response: ", err)
		return
	}

	m := responseLineRegexp.FindSubmatch(p)
	if m == nil {
		ts.error("bad response line", nil)
		return
	}

	for {
		p, err = ts.r.ReadSlice('\n')
		if err != nil {
			ts.error("error reading header: ", err)
			return
		}
		if len(p) <= 2 {
			break
		}
	}

	if string(m[1]) != "200" {
		p, _ := ioutil.ReadAll(ts.r)
		log.Println(string(p))
		ts.error("bad response code: "+string(m[1]), nil)
		return
	}

	ts.chunkState = stateStart

	log.Println("twitterstream: connected to", ts.url)
}

// Next returns the next line from the stream. The returned slice is
// overwritten by the next call to Next.
func (ts *TwitterStream) Next() []byte {
	for {
		if ts.r == nil {
			d := ts.waitUntil - time.Nanoseconds()
			if d > 0 {
				time.Sleep(d)
			}
			ts.waitUntil = time.Nanoseconds() + 30e9
			ts.connect()
			continue
		}

		p, err := ts.r.ReadSlice('\n')
		if err != nil {
			ts.error("error reading line", err)
			continue
		}

		switch ts.chunkState {
		case stateStart:
			ts.chunkRemaining, err = strconv.Btoi64(string(p[:len(p)-2]), 16)
			switch {
			case err != nil:
				ts.error("error parsing chunk size", err)
			case ts.chunkRemaining == 0:
				ts.error("end of chunked stream", nil)
			}
			ts.chunkState = stateNormal
			continue
		case stateEnd:
			ts.chunkState = stateStart
			continue
		case stateNormal:
			ts.chunkRemaining = ts.chunkRemaining - int64(len(p))
			if ts.chunkRemaining == 0 {
				ts.chunkState = stateEnd
			}
		}

		if len(p) <= 2 {
			continue // ignore keepalive line
		}

		return p
	}
	panic("should not get here")
	return nil
}
