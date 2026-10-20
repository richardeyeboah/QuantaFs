package io.github.richardeyeboah.quantafs.cluster;

import java.util.Objects;

/** A validated DNS host and TCP port. Resolution is deliberately deferred to gRPC. */
public record NetworkEndpoint(String host, int port) {

    public NetworkEndpoint {
        Objects.requireNonNull(host, "host");
        if (host.isBlank() || !host.equals(host.trim()) || host.contains(":")) {
            throw new IllegalArgumentException("endpoint host must be a non-empty DNS name or IPv4 address");
        }
        if (port < 1 || port > 65_535) {
            throw new IllegalArgumentException("endpoint port must be 1..65535");
        }
    }

    public static NetworkEndpoint parse(String authority) {
        Objects.requireNonNull(authority, "authority");
        int separator = authority.lastIndexOf(':');
        if (separator < 1 || separator == authority.length() - 1) {
            throw new IllegalArgumentException("endpoint must use host:port");
        }
        try {
            return new NetworkEndpoint(
                    authority.substring(0, separator),
                    Integer.parseInt(authority.substring(separator + 1)));
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("endpoint port must be an integer", e);
        }
    }

    public static NetworkEndpoint loopback(int port) {
        return new NetworkEndpoint("127.0.0.1", port);
    }

    public String authority() {
        return host + ":" + port;
    }
}
