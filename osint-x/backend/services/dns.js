const dns = require("dns").promises;


/**
 * Resolve DNS information for a domain
 */
async function resolveDNS(domain) {

    console.log(
        `[DNS] Resolving domain: ${domain}`
    );

    try {

        const [ipv4, ipv6] = await Promise.all([
            dns.resolve4(domain).catch(() => []),
            dns.resolve6(domain).catch(() => [])
        ]);

        const result = {
            domain,
            ipv4,
            ipv6
        };

        console.log("[DNS] Result:");
        console.log(result);

        return result;

    } catch (error) {

        console.error(
            `[DNS] Failed for ${domain}:`,
            error.message
        );

        throw error;
    }
}


module.exports = resolveDNS;