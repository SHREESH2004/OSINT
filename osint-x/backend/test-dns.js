const extractDomains = require("./utils/domainExtractor");
const resolveDNS = require("./services/dns");

async function test() {

    const sherlockOutput = `
    [+] GitHub: https://github.com/s0umo_
    [+] Example: https://example.com/profile/s0umo_
    [+] CodeChef: https://www.codechef.com/users/s0umo_
    `;

    console.log("[1] Extracting domains...");

    const domains = extractDomains(sherlockOutput);

    console.log("[DOMAINS]");
    console.log(domains);


    console.log("\n[2] Resolving DNS...\n");

    for (const domain of domains) {

        const result = await resolveDNS(domain);

        console.log(result);
    }
}

test();