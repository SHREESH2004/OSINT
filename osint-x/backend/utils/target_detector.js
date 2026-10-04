function detectTarget(target) {

    target = target.trim();

    if (!target) {
        return {
            type: "unknown",
            value: target
        };
    }

    // -------------------------
    // URL
    // -------------------------

    try {
        const url = new URL(target);

        if (url.protocol === "http:" || url.protocol === "https:") {
            return {
                type: "url",
                value: target,
                hostname: url.hostname
            };
        }

    } catch (error) {
        // Not a URL
    }

    // -------------------------
    // IPv4
    // -------------------------

    const ipv4Regex =
        /^(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$/;

    if (ipv4Regex.test(target)) {
        return {
            type: "ipv4",
            value: target
        };
    }

    // -------------------------
    // IPv6
    // -------------------------

    const ipv6Regex =
        /^(?:[0-9a-fA-F]{1,4}:){2,7}[0-9a-fA-F]{1,4}$/;

    if (ipv6Regex.test(target)) {
        return {
            type: "ipv6",
            value: target
        };
    }

    // -------------------------
    // Email
    // -------------------------

    const emailRegex =
        /^[^\s@]+@[^\s@]+\.[^\s@]+$/;

    if (emailRegex.test(target)) {
        return {
            type: "email",
            value: target,
            domain: target.split("@")[1]
        };
    }

    // -------------------------
    // ASN
    // -------------------------

    const asnRegex = /^AS\d+$/i;

    if (asnRegex.test(target)) {
        return {
            type: "asn",
            value: target.toUpperCase()
        };
    }

    // -------------------------
    // Domain
    // -------------------------

    const domainRegex =
        /^(?=.{1,253}$)([a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?\.)+[a-zA-Z]{2,}$/;

    if (domainRegex.test(target)) {
        return {
            type: "domain",
            value: target.toLowerCase()
        };
    }

    // -------------------------
    // Username
    // -------------------------

    return {
        type: "username",
        value: target
    };
}

module.exports = detectTarget;