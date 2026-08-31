import path from 'path';
import type { NextConfig } from 'next';

const repoRoot = path.join(__dirname, '../..');
const coreSrc = path.join(repoRoot, 'src');

const nextConfig: NextConfig = {
  // Parent lockfile makes Next treat the repo as the workspace; keep src reachable.
  outputFileTracingRoot: repoRoot,
  transpilePackages: ['@agent/core'],
  webpack: (config) => {
    config.resolve.alias = {
      ...config.resolve.alias,
      '@agent/core/adapters/prisma': path.join(coreSrc, 'adapters/prisma.ts'),
      '@agent/core/adapters/redis': path.join(coreSrc, 'adapters/redis.ts'),
      '@agent/core/adapters/qstash': path.join(coreSrc, 'adapters/qstash.ts'),
      '@agent/core/adapters/upstash': path.join(coreSrc, 'adapters/upstash.ts'),
      '@agent/core/adapters/memory': path.join(coreSrc, 'adapters/memory.ts'),
      '@agent/core': path.join(coreSrc, 'index.ts'),
    };
    return config;
  },
};

export default nextConfig;
