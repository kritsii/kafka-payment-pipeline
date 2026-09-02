/** @type {import('tailwindcss').Config} */
export default {
  content: ['./index.html', './src/**/*.{js,jsx}'],
  theme: { extend: { colors: { ink: '#172033', navy: '#101828', line: '#e4e8ef', blue: '#155eef' }, fontFamily: { display: ['Space Grotesk', 'sans-serif'], sans: ['DM Sans', 'sans-serif'] } } },
  plugins: [],
}
