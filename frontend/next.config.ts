// next.config.js
module.exports = {
  async headers() {
    return [
      {
        source: "/(.*)", // Toutes les routes
        headers: [
          {
            key: "Content-Security-Policy",
            value: `
              default-src 'self';
              script-src 'self' 'unsafe-inline';
              object-src 'none';
              style-src 'self' 'unsafe-inline';
            `.replace(/\s{2,}/g, " ").trim(),
          },
        ],
      },
    ];
  },
};
