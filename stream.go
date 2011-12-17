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

// Package twitterstream implements the basic functionality for accessing the
// Twitter streaming APIs. See http://dev.twitter.com/pages/streaming_api for
// information on the Twitter streaming APIs.
package twitterstream

import (
	"bufio"
	"bytes"
	"crypto/tls"
	"github.com/garyburd/go-oauth"
	"github.com/garyburd/twister/web"
	"io/ioutil"
	"log"
	"net"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// TwitterStream manages the connection to Twitter. The stream automatically
// reconnects to Twitter if there is an error with the connection.
type TwitterStream struct {
	waitUntil      time.Time
	chunkRemaining int64
	chunkState     int
	conn           net.Conn
	r              *bufio.Reader
	urlStr         string
	params         url.Values
	oauthClient    *oauth.Client
	accessToken    *oauth.Credentials
}

// New returns a new TwitterStream. 
func New(oauthClient *oauth.Client, accessToken *oauth.Credentials, urlStr string, params url.Values) *TwitterStream {
	return &TwitterStream{oauthClient: oauthClient, accessToken: accessToken, urlStr: urlStr, params: params}
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

func (ts *TwitterStream) error(msg string, err error) {
	log.Println("twitterstream:", msg+":", err)
	ts.Close()
}

func (ts *TwitterStream) connect() {
	var err error
	log.Println("twitterstream: connecting to", ts.urlStr)

	u, err := url.Parse(ts.urlStr)
	if err != nil {
		panic("bad url: " + ts.urlStr)
	}

	addr := u.Host
	if strings.LastIndex(addr, ":") <= strings.LastIndex(addr, "]") {
		if u.Scheme == "http" {
			addr = addr + ":80"
		} else {
			addr = addr + ":443"
		}
	}

	params := url.Values{}
	for key, values := range ts.params {
		params[key] = values
	}
	ts.oauthClient.SignParam(ts.accessToken, "POST", ts.urlStr, params)

	body := params.Encode()

	header := web.NewHeader(
		web.HeaderHost, u.Host,
		web.HeaderContentLength, strconv.Itoa(len(body)),
		web.HeaderContentType, "application/x-www-form-urlencoded")

	var request bytes.Buffer
	request.WriteString("POST ")
	request.WriteString(u.RawPath)
	request.WriteString(" HTTP/1.1\r\n")
	header.WriteHttpHeader(&request)
	request.WriteString(body)

	if u.Scheme == "http" {
		ts.conn, err = net.Dial("tcp", addr)
		if err != nil {
			ts.error("dial failed ", err)
			return
		}
	} else {
		ts.conn, err = tls.Dial("tcp", addr, nil)
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
	err = ts.conn.SetReadTimeout(int64(60 * time.Second))
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

	log.Println("twitterstream: connected to", ts.urlStr)
}

// Next returns the next line from the stream. The returned slice is
// overwritten by the next call to Next.
func (ts *TwitterStream) Next() []byte {
	for {
		if ts.r == nil {
			d := ts.waitUntil.Sub(time.Now())
			if d > 0 {
				time.Sleep(d)
			}
			ts.waitUntil = time.Now().Add(30 * time.Second)
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
			ts.chunkRemaining, err = strconv.ParseInt(string(p[:len(p)-2]), 16, 64)
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
