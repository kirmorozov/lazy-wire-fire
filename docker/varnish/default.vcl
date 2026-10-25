vcl 4.1;

# Tag purging the way Magento's generated VCL does it: responses carry X-Magento-Tags, a PURGE
# with X-Magento-Tags-Pattern bans the matching objects. Backend: the test's HTTP server.
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
        if (!req.http.X-Magento-Tags-Pattern) {
            return (synth(400, "X-Magento-Tags-Pattern header required"));
        }
        ban("obj.http.X-Magento-Tags ~ " + req.http.X-Magento-Tags-Pattern);
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
