package io.kestra.plugin.fs;

/**
 * What a reachability task (ICMP ping, TCP check) expects from its target.
 * Shared by {@code icmp.Ping} and {@code tcp.Check}.
 */
public enum ReachabilityExpectation {
    REACHABLE,
    UNREACHABLE,
    ANY
}
