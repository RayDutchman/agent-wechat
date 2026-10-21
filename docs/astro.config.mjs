import { defineConfig } from 'astro/config';
import starlight from '@astrojs/starlight';
import { remarkBasePath } from './remark-base-path.mjs';
import { ogImage } from './og-image-integration.mjs';

const base = '/agent-wechat';
const site = 'https://RayDutchman.github.io';

export default defineConfig({
  site,
  base,
  markdown: {
    remarkPlugins: [remarkBasePath(base)],
  },
  integrations: [
    ogImage(),
    starlight({
      title: 'agent-wechat',
      description: 'A programmable WeChat interface for AI agents and automation.',
      customCss: ['./src/styles/custom.css'],
      components: {
        Head: './src/components/Head.astro',
      },
      social: {
        github: 'https://github.com/RayDutchman/agent-wechat',
      },
      sidebar: [
        {
          label: 'Getting Started',
          items: [
            { label: 'What is agent-wechat?', slug: 'getting-started/overview' },
            { label: 'Quick Start', slug: 'getting-started/quickstart' },
          ],
        },
        {
          label: 'How It Works',
          items: [
            { label: 'Architecture', slug: 'how-it-works/architecture' },
          ],
        },
        {
          label: 'Container Setup',
          items: [
            { label: 'Using the CLI (wx up)', slug: 'getting-started/container-setup/cli' },
            { label: 'Docker Compose', slug: 'getting-started/container-setup/docker-compose' },
            { label: 'Building from Source', slug: 'getting-started/container-setup/building-locally' },
          ],
        },
        {
          label: 'CLI Reference',
          items: [
            { label: 'Installation', slug: 'getting-started/cli/installation' },
            { label: 'Commands', slug: 'getting-started/cli/commands' },
          ],
        },
        {
          label: 'Operations',
          items: [
            { label: 'User Guide', slug: 'operations/user-guide' },
            { label: 'VNC Viewer', slug: 'operations/vnc' },
            { label: 'Tokens & Authentication', slug: 'operations/tokens' },
            { label: 'Data & Storage', slug: 'operations/data' },
            { label: 'Proxy Configuration', slug: 'operations/proxy' },
            { label: 'Restarting & Recovery', slug: 'operations/restarting' },
          ],
        },
        {
          label: 'Hosting',
          items: [
            { label: 'Requirements', slug: 'hosting/requirements' },
            { label: 'Self-Hosting', slug: 'hosting/self-hosting' },
            { label: 'Managed Hosting', slug: 'hosting/managed-hosting' },
          ],
        },
        {
          label: 'Reference',
          items: [
            { label: 'REST API', slug: 'reference/api' },
            { label: 'Environment Variables', slug: 'reference/environment-vars' },
            { label: 'Troubleshooting', slug: 'reference/troubleshooting' },
          ],
        },
      ],
    }),
  ],
});
