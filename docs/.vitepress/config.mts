import { defineConfig } from 'vitepress'

export default defineConfig({
  title: 'Bria',
  description: 'Multi-pipeline job orchestrator',
  base: '/bria/',
  themeConfig: {
    nav: [
      { text: 'Guide', link: '/guide/getting-started' },
      { text: 'Reference', link: '/reference/responses' }
    ],
    sidebar: {
      '/guide/': [
        { text: 'Guide', items: [
          { text: 'Getting started', link: '/guide/getting-started' },
          { text: 'Configuration', link: '/guide/configuration' },
          { text: 'Usage', link: '/guide/usage' }
        ] }
      ],
      '/reference/': [
        { text: 'Reference', items: [
          { text: 'Responses', link: '/reference/responses' },
          { text: 'Operations', link: '/reference/operations' }
        ] }
      ]
    },
    socialLinks: [{ icon: 'github', link: 'https://github.com/melonask/bria' }]
  }
})
