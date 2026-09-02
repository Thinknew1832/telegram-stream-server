import type { Metadata } from 'next';
import './globals.css';

export const metadata: Metadata = {
  title: 'AnimeToon - Stream Anime Online',
  description: 'Fast streaming anime web application',
};

export default function RootLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <html lang="en">
      <head>
        <meta name="referrer" content="no-referrer" />
        <script src="https://cdn.jsdelivr.net/npm/artplayer/dist/artplayer.js" defer></script>
      </head>
      <body className="bg-slate-950 text-white min-h-screen antialiased selection:bg-pink-500 selection:text-white">
        {children}
      </body>
    </html>
  );
}
