/** @type {import('tailwindcss').Config} */
export default {
  content: ["./index.html", "./src/**/*.{ts,tsx}"],
  theme: {
    extend: {
      fontFamily: {
        display: ["Space Grotesk", "Inter", "system-ui", "sans-serif"],
        sans: ["Inter", "system-ui", "sans-serif"],
        mono: ["JetBrains Mono", "ui-monospace", "SFMono-Regular", "monospace"],
      },
      colors: {
        ledger: {
          ink: "#101820",
          panel: "#F7F9F4",
          mist: "#DDE5DF",
          grid: "#B7C2BA",
          teal: "#2D8C83",
          copper: "#B66A3C",
          violet: "#6D5FA8",
          red: "#BA4A45",
        },
      },
    },
  },
  plugins: [],
};
