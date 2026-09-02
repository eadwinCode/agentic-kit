import { defineConfig } from 'vitepress';

const repo = 'https://github.com/eadwinCode/agentic-kit';

export default defineConfig({
  title: 'agentic-kit',
  description: 'A durable runtime for AI agent runs, and the React hook that talks to it.',
  // Project page, so the site is served from /agentic-kit/ rather than the root.
  base: '/agentic-kit/',
  lastUpdated: true,
  cleanUrls: true,
  // README.md is kept for browsing the folder on GitHub; index.md is the site's
  // home. Without this both would build, as two routes with the same content.
  srcExclude: ['README.md'],

  head: [
    ['meta', { name: 'theme-color', content: '#3c8772' }],
    ['meta', { property: 'og:title', content: 'agentic-kit' }],
    [
      'meta',
      {
        property: 'og:description',
        content: 'A durable runtime for AI agent runs, and the React hook that talks to it.',
      },
    ],
  ],

  themeConfig: {
    search: { provider: 'local' },

    nav: [
      { text: 'Getting started', link: '/getting-started' },
      { text: 'Concepts', link: '/concepts' },
      { text: 'React', link: '/react' },
      { text: 'Production', link: '/production' },
    ],

    sidebar: [
      {
        text: 'Start here',
        items: [
          { text: 'Overview', link: '/' },
          { text: 'Getting started', link: '/getting-started' },
          { text: 'Core concepts', link: '/concepts' },
          { text: 'HTTP API', link: '/http-api' },
        ],
      },
      {
        text: 'Setting it up',
        items: [
          { text: 'setupAgentCore', link: '/setup' },
          { text: 'Ports and adapters', link: '/ports-and-adapters' },
          { text: 'Configuration', link: '/configuration' },
        ],
      },
      {
        text: 'Building with it',
        items: [
          { text: 'Agents and tools', link: '/agents-and-tools' },
          { text: 'Human in the loop', link: '/human-in-the-loop' },
          { text: 'Subagents', link: '/subagents' },
          { text: 'Context and tokens', link: '/context-and-tokens' },
          { text: 'Provider options', link: '/provider-options' },
          { text: 'Custom events', link: '/custom-events' },
        ],
      },
      {
        text: 'Multi-tenancy',
        items: [
          { text: 'Run state', link: '/run-state' },
          { text: 'Multi-tenancy', link: '/multi-tenancy' },
        ],
      },
      {
        text: 'Client',
        items: [{ text: 'React: use-agentenkit', link: '/react' }],
      },
      {
        text: 'Operating it',
        items: [
          { text: 'Observability', link: '/observability' },
          { text: 'Production', link: '/production' },
          { text: 'Troubleshooting', link: '/troubleshooting' },
        ],
      },
    ],

    socialLinks: [{ icon: 'github', link: repo }],

    editLink: {
      pattern: `${repo}/edit/main/docs/:path`,
      text: 'Edit this page on GitHub',
    },

    footer: {
      message: 'Released under the MIT License.',
      copyright: `<a href="${repo}">github.com/eadwinCode/agentic-kit</a>`,
    },
  },
});
