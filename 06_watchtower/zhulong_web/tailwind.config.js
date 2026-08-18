/** @type {import('tailwindcss').Config} */
export default {
  content: ['./index.html', './src/**/*.{vue,js,ts,jsx,tsx}'],
  theme: {
    extend: {
      colors: {
        abyss: '#0B0E11',
        slateCard: '#151A21',
        porcelain: '#EAECEF',
        titan: '#848E9C',
        pass: '#16C784',
        veto: '#EA3943',
        gold: '#F0B90B',
      },
      boxShadow: {
        panel: '0 20px 45px rgba(7, 10, 17, 0.35)',
      },
      backdropBlur: {
        panel: '10px',
      },
    },
  },
  plugins: [],
}
