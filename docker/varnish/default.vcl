vcl 4.1;

# Tag-based purging: responses carry a tag header (X-Cache-Tags); a PURGE with a pattern header
# (X-Cache-Tags-Pattern) bans the matching objects. Backend: the test's HTTP server.
backend default {
    .host = "127.0.0.1";
    .port = "34682";
}

acl purge {
    "127.0.0.1";
}

sub vcl_recv {
    if (req.method == "PURGE") {
        if (client.ip !~ purge) {
            return (synth(405, "Method not allowed"));
        }
        if (!req.http.X-Cache-Tags-Pattern) {
            return (synth(400, "X-Cache-Tags-Pattern header required"));
        }
        ban("obj.http.X-Cache-Tags ~ " + req.http.X-Cache-Tags-Pattern);
        return (synth(200, "Purged"));
    }
    return (hash);
}

sub vcl_backend_response {
    set beresp.ttl = 1h;
}

sub vcl_deliver {
    if (obj.hits > 0) {
        set resp.http.X-Cache = "HIT";
    } else {
        set resp.http.X-Cache = "MISS";
    }
}
