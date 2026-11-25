package com.shaktidb.bench.rrb.web;

import jakarta.servlet.http.HttpServletRequest;

/** Client IP (first X-Forwarded-For hop when behind the load balancer) and User-Agent. */
final class ClientInfo {

    private ClientInfo() {}

    static String ip(HttpServletRequest req) {
        String xff = req.getHeader("X-Forwarded-For");
        String ip = xff != null && !xff.isBlank() ? xff.split(",", 2)[0].trim() : req.getRemoteAddr();
        return looksLikeIp(ip) ? ip : null;   // never send junk into CAST(? AS inet)
    }

    static String userAgent(HttpServletRequest req) {
        String ua = req.getHeader("User-Agent");
        return ua == null ? null : ua.length() > 256 ? ua.substring(0, 256) : ua;
    }

    private static boolean looksLikeIp(String s) {
        if (s == null || s.isEmpty() || s.length() > 45) {
            return false;
        }
        boolean colon = false;
        boolean dot = false;
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == ':') {
                colon = true;
            } else if (c == '.') {
                dot = true;
            } else if (Character.digit(c, 16) < 0) {
                return false;
            }
        }
        if (colon) {
            return true;                      // IPv6 (inet cast validates the rest)
        }
        if (!dot) {
            return false;
        }
        String[] parts = s.split("\\.", -1);
        if (parts.length != 4) {
            return false;
        }
        for (String p : parts) {
            if (p.isEmpty() || p.length() > 3 || !p.chars().allMatch(Character::isDigit) || Integer.parseInt(p) > 255) {
                return false;
            }
        }
        return true;
    }
}
