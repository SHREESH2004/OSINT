function extractDomains(text) {

    const domains = new Set();

    const urlRegex =
        /https?:\/\/([^\/\s]+)/gi;

    let match;

    while ((match = urlRegex.exec(text)) !== null) {

        const host = match[1]
            .split(":")[0]
            .toLowerCase();

        domains.add(host);
    }

    return [...domains];
}


module.exports = extractDomains;