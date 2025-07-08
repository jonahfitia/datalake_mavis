import { NextConfig } from 'next';

const nextConfig: NextConfig = {
  experimental: {
    allowedDevOrigins: ['http://192.168.2.139:3000'], // adapte selon ton IP + port
  } as any,

  async headers() {
    return [
      {
        source: '/(.*)', // Toutes les routes
        headers: [
          {
            key: 'Content-Security-Policy',
            value: `
              default-src 'self';
              script-src 'self' 'unsafe-inline';
              object-src 'none';
              style-src 'self' 'unsafe-inline';
            `.replace(/\s{2,}/g, ' ').trim(),
          },
        ],
      },
    ];
  },
};

export default nextConfig;
