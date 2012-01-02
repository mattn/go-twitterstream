go-twitterstream is a [Go](http://golang.org/) package for working with the 
[Twitter streaming API](http://dev.twitter.com/pages/streaming_api).

[Documentation](http://gopkgdoc.appspot.com/pkg/github.com/garyburd/go-twitterstream)

Installation:

    goinstall github.com/garyburd/go-twitterstream


Change on 1/1/2012:

It is now the application's responsibility to reconnect to Twitter on a dropped
connection.  The package API was changed significantly to reflect this change
in functionality.  The example in the package comment illustrates how to
implement reconnect logic in the application.

This change allows applications to implement backfill and other features on
reconnect.
