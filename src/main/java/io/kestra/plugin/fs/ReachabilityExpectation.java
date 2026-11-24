package io.kestra.plugin.fs;

/** Outcome a reachability task expects from its target. */
public enum ReachabilityExpectation {
    REACHABLE,
    UNREACHABLE,
    ANY
}
